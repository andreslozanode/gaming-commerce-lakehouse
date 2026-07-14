"""Two-tower retrieval model (player tower x title tower) + churn head, PyTorch on CUDA.

GPU optimizations, all switchable from conf/base.yaml -> ml.torch:
  * AMP with bf16 autocast (A100/H100) or fp16 + GradScaler on older SM — ~2x throughput
  * TF32 matmul/cudnn for fp32 fallback paths
  * torch.compile(mode="max-autotune") — kernel fusion on the embedding+MLP path
  * DistributedDataParallel with gradient bucketing (multi-GPU node or multi-node on Databricks)
  * DataLoader: persistent_workers + pin_memory + prefetch_factor, non_blocking H2D copies
  * cudnn.benchmark for fixed input shapes
  * Fused AdamW + set_to_none gradient zeroing
  * Petastorm/Arrow batch reads straight from Delta (no CSV round-trip)
"""

from __future__ import annotations

import argparse
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import mlflow
import numpy as np
import pandas as pd
import torch
import torch.distributed as dist
import torch.nn as nn
import torch.nn.functional as Fn
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader, Dataset, DistributedSampler

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.ml.registry import log_and_register

log = get_logger(__name__)


# --------------------------------------------------------------------------------------
# Data
# --------------------------------------------------------------------------------------
class InteractionDataset(Dataset):
    """In-memory tensors: the interaction matrix fits comfortably in RAM at this scale.
    Beyond ~100M rows, swap to Petastorm / Mosaic StreamingDataset without touching the model."""

    def __init__(self, players: np.ndarray, items: np.ndarray, labels: np.ndarray, churn: np.ndarray):
        self.players = torch.from_numpy(players).long()
        self.items = torch.from_numpy(items).long()
        self.labels = torch.from_numpy(labels).float()
        self.churn = torch.from_numpy(churn).float()

    def __len__(self) -> int:
        return int(self.players.shape[0])

    def __getitem__(self, idx: int):
        return self.players[idx], self.items[idx], self.labels[idx], self.churn[idx]


def index_dir() -> Path:
    """Process-local scratch dir for the factorize() index arrays (logged to MLflow afterwards)."""
    path = Path(os.getenv("GC_ARTIFACT_DIR") or tempfile.mkdtemp(prefix="gc-recsys-"))
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_training_arrays() -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, int, int]:
    """Delta -> Arrow -> NumPy. Spark does the join; the GPU box never touches the lakehouse twice."""
    from gaming_lakehouse.spark import build_spark

    s = load_settings()
    spark = build_spark("recsys-featureload")
    pdf = (
        spark.table(s.table("gold", "feat_interactions"))
        .join(spark.table(s.table("gold", "feat_player")).select("player_id", "is_churned"), "player_id")
        .select("player_id", "product_id", "confidence", "is_churned")
        .toPandas()  # arrow-enabled in spark.py
    )
    pdf = cast(pd.DataFrame, pdf)  # pyspark stubs erase the pandas frame type
    player_codes, player_index = pdf["player_id"].factorize()
    item_codes, item_index = pdf["product_id"].factorize()
    # The code<->id mappings are part of the model contract: batch_inference needs them to
    # translate embedding rows back into player_id / product_id. Written to a process-local
    # temp dir and logged as MLflow artifacts alongside the weights, never to a shared /tmp path.
    idx_dir = index_dir()
    np.save(idx_dir / "player_index.npy", player_index.to_numpy())
    np.save(idx_dir / "item_index.npy", item_index.to_numpy())
    return (
        player_codes.astype(np.int64),
        item_codes.astype(np.int64),
        pdf["confidence"].to_numpy(np.float32),
        pdf["is_churned"].to_numpy(np.float32),
        len(player_index),
        len(item_index),
    )


# --------------------------------------------------------------------------------------
# Model
# --------------------------------------------------------------------------------------
@dataclass
class ModelConfig:
    n_players: int
    n_items: int
    dim: int = 128
    hidden: int = 256
    dropout: float = 0.15


class TwoTower(nn.Module):
    def __init__(self, cfg: ModelConfig):
        super().__init__()
        self.player_emb = nn.Embedding(cfg.n_players, cfg.dim, sparse=False)
        self.item_emb = nn.Embedding(cfg.n_items, cfg.dim, sparse=False)
        nn.init.normal_(self.player_emb.weight, std=0.05)
        nn.init.normal_(self.item_emb.weight, std=0.05)

        def tower() -> nn.Sequential:
            return nn.Sequential(
                nn.Linear(cfg.dim, cfg.hidden),
                nn.GELU(),
                nn.Dropout(cfg.dropout),
                nn.Linear(cfg.hidden, cfg.dim),
                nn.LayerNorm(cfg.dim),
            )

        self.player_tower, self.item_tower = tower(), tower()
        # Multi-task: the churn head shares the player representation -> better-regularized embeddings.
        self.churn_head = nn.Sequential(nn.Linear(cfg.dim, 64), nn.GELU(), nn.Linear(64, 1))

    def forward(self, players: torch.Tensor, items: torch.Tensor):
        p = Fn.normalize(self.player_tower(self.player_emb(players)), dim=-1)
        i = Fn.normalize(self.item_tower(self.item_emb(items)), dim=-1)
        score = (p * i).sum(-1)  # cosine similarity
        churn_logit = self.churn_head(p).squeeze(-1)
        return score, churn_logit


# --------------------------------------------------------------------------------------
# Training
# --------------------------------------------------------------------------------------
def setup_cuda(precision: str) -> tuple[torch.device, torch.dtype | None]:
    if not torch.cuda.is_available():
        log.warning("CUDA unavailable — falling back to CPU (dev only)")
        return torch.device("cpu"), None
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.backends.cudnn.allow_tf32 = True
    torch.backends.cudnn.benchmark = True  # fixed shapes -> autotuned kernels
    local_rank = int(os.getenv("LOCAL_RANK", "0"))
    torch.cuda.set_device(local_rank)
    device = torch.device(f"cuda:{local_rank}")
    supports_bf16 = torch.cuda.get_device_capability(local_rank)[0] >= 8
    dtype = {"bf16": torch.bfloat16 if supports_bf16 else torch.float16, "fp16": torch.float16, "fp32": None}[
        precision
    ]
    log.info(
        "cuda ready",
        extra={
            "extra_fields": {
                "device": torch.cuda.get_device_name(local_rank),
                "amp_dtype": str(dtype),
                "capability": torch.cuda.get_device_capability(local_rank),
            }
        },
    )
    return device, dtype


def train(args: argparse.Namespace) -> None:
    s = load_settings()
    tcfg = s.get("ml.torch", {})
    ddp_enabled = bool(tcfg.get("ddp")) and int(os.getenv("WORLD_SIZE", "1")) > 1
    if ddp_enabled:
        dist.init_process_group(backend="nccl")
    rank = int(os.getenv("RANK", "0"))

    device, amp_dtype = setup_cuda(tcfg.get("precision", "bf16"))
    players, items, labels, churn, n_players, n_items = load_training_arrays()

    dataset = InteractionDataset(players, items, labels, churn)
    sampler = DistributedSampler(dataset) if ddp_enabled else None
    loader = DataLoader(
        dataset,
        batch_size=tcfg.get("batch_size", 4096),
        shuffle=sampler is None,
        sampler=sampler,
        num_workers=tcfg.get("num_workers", 8),
        pin_memory=True,  # page-locked host memory -> async H2D
        persistent_workers=True,
        prefetch_factor=tcfg.get("prefetch_factor", 4),
        drop_last=True,  # stable shapes for cudnn.benchmark and torch.compile
    )

    model = TwoTower(ModelConfig(n_players, n_items)).to(device, memory_format=torch.contiguous_format)
    if tcfg.get("compile", True) and hasattr(torch, "compile"):
        model = torch.compile(model, mode="max-autotune", dynamic=False)
    if ddp_enabled:
        model = DDP(model, device_ids=[device.index], gradient_as_bucket_view=True, broadcast_buffers=False)

    optimizer = torch.optim.AdamW(
        model.parameters(), lr=args.lr, weight_decay=1e-2, fused=torch.cuda.is_available()
    )
    scheduler = torch.optim.lr_scheduler.OneCycleLR(
        optimizer, max_lr=args.lr, total_steps=args.epochs * len(loader), pct_start=0.1
    )
    scaler = torch.amp.GradScaler("cuda", enabled=amp_dtype == torch.float16)

    if rank == 0:
        mlflow.set_experiment(s.get("ml.experiment"))
        mlflow.start_run(run_name=f"two-tower-{s.cloud}-{s.environment}")
        mlflow.log_params(
            {
                "dim": 128,
                "batch_size": tcfg.get("batch_size"),
                "lr": args.lr,
                "epochs": args.epochs,
                "precision": tcfg.get("precision"),
                "compile": tcfg.get("compile"),
                "ddp": ddp_enabled,
                "n_players": n_players,
                "n_items": n_items,
            }
        )

    for epoch in range(args.epochs):
        if sampler:
            sampler.set_epoch(epoch)
        model.train()
        running = 0.0
        for step, (p, i, y, c) in enumerate(loader):
            p = p.to(device, non_blocking=True)
            i = i.to(device, non_blocking=True)
            y = y.to(device, non_blocking=True)
            c = c.to(device, non_blocking=True)

            with torch.autocast("cuda", dtype=amp_dtype, enabled=amp_dtype is not None):
                score, churn_logit = model(p, i)
                # In-batch sampled softmax: every other item in the batch is an implicit negative.
                logits = score.unsqueeze(0) - score.unsqueeze(1)
                retrieval_loss = Fn.softplus(-logits.diagonal()).mean() + 0.1 * Fn.mse_loss(
                    score, y / (y.max() + 1e-6)
                )
                churn_loss = Fn.binary_cross_entropy_with_logits(churn_logit, c)
                loss = retrieval_loss + args.churn_weight * churn_loss

            optimizer.zero_grad(set_to_none=True)  # cheaper than zeroing buffers
            scaler.scale(loss).backward()
            scaler.unscale_(optimizer)
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            scaler.step(optimizer)
            scaler.update()
            scheduler.step()
            running += loss.item()

            if rank == 0 and step % 100 == 0:
                mem = torch.cuda.max_memory_allocated() / 1e9 if torch.cuda.is_available() else 0
                log.info(
                    "step",
                    extra={
                        "extra_fields": {
                            "epoch": epoch,
                            "step": step,
                            "loss": round(loss.item(), 4),
                            "gpu_mem_gb": round(mem, 2),
                        }
                    },
                )
        if rank == 0:
            mlflow.log_metric("train_loss", running / max(len(loader), 1), step=epoch)

    if rank == 0:
        base = model.module if hasattr(model, "module") else model
        log_and_register(
            base,
            name="gc_two_tower_recsys",
            flavor="pytorch",
            signature_input=(torch.zeros(1, dtype=torch.long), torch.zeros(1, dtype=torch.long)),
        )
        mlflow.log_artifacts(str(index_dir()), artifact_path="indexes")
        mlflow.end_run()
    if ddp_enabled:
        dist.destroy_process_group()


def main() -> None:
    parser = argparse.ArgumentParser()
    settings = load_settings()
    parser.add_argument("--epochs", type=int, default=settings.get("ml.torch.max_epochs", 10))
    parser.add_argument("--lr", type=float, default=3e-3)
    parser.add_argument("--churn_weight", type=float, default=0.3)
    train(parser.parse_args())


if __name__ == "__main__":
    main()
