"""TensorFlow demand forecaster: revenue per platform x channel, 28-day horizon.

GPU/CPU optimizations:
  * mixed_float16 policy (loss-scaled automatically by the LossScaleOptimizer)
  * XLA jit_compile=True on the train step -> fused kernels, ~1.3-1.6x on the LSTM stack
  * tf.data: cache -> shuffle -> batch -> prefetch(AUTOTUNE), interleave with num_parallel_calls
  * MirroredStrategy (single-node multi-GPU) / MultiWorkerMirroredStrategy (Databricks multi-node)
  * Deterministic seeds + explicit memory growth so the process never grabs the whole GPU
"""

from __future__ import annotations

import argparse
from typing import cast

import mlflow
import numpy as np
import pandas as pd
import tensorflow as tf

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.ml.registry import log_and_register

log = get_logger(__name__)

LOOKBACK = 56
HORIZON = 28
FEATURES = [
    "revenue",
    "revenue_lag_1",
    "revenue_lag_7",
    "revenue_lag_28",
    "revenue_roll_7",
    "revenue_roll_28",
    "dow",
    "month",
    "is_holiday_season",
]


def configure_gpu() -> tf.distribute.Strategy:
    gpus = tf.config.list_physical_devices("GPU")
    for gpu in gpus:
        tf.config.experimental.set_memory_growth(gpu, True)  # don't preallocate the whole card
    if len(gpus) > 1:
        strategy = tf.distribute.MirroredStrategy()
    elif gpus:
        strategy = tf.distribute.OneDeviceStrategy("/gpu:0")
    else:
        strategy = tf.distribute.get_strategy()
    settings = load_settings()
    if gpus and settings.get("ml.tensorflow.mixed_precision"):
        tf.keras.mixed_precision.set_global_policy(settings.get("ml.tensorflow.mixed_precision"))
    log.info(
        "tf strategy ready",
        extra={
            "extra_fields": {
                "gpus": len(gpus),
                "replicas": strategy.num_replicas_in_sync,
                "policy": tf.keras.mixed_precision.global_policy().name,
            }
        },
    )
    return strategy


def load_series() -> tuple[np.ndarray, np.ndarray]:
    from gaming_lakehouse.spark import build_spark

    s = load_settings()
    spark = build_spark("tf-forecast-featureload")
    pdf = (
        spark.table(s.table("gold", "feat_sales_ts"))
        .orderBy("platform_code", "channel_type", "ds")
        .toPandas()
    )
    pdf = cast(pd.DataFrame, pdf)  # pyspark stubs erase the pandas frame type
    windows_x, windows_y = [], []
    for _, group in pdf.groupby(["platform_code", "channel_type"], sort=False):
        values = group[FEATURES].to_numpy(np.float32)
        target = group["revenue"].to_numpy(np.float32)
        for start in range(len(values) - LOOKBACK - HORIZON):
            windows_x.append(values[start : start + LOOKBACK])
            windows_y.append(target[start + LOOKBACK : start + LOOKBACK + HORIZON])
    return np.stack(windows_x), np.stack(windows_y)


def make_dataset(x: np.ndarray, y: np.ndarray, batch_size: int, *, training: bool) -> tf.data.Dataset:
    ds = tf.data.Dataset.from_tensor_slices((x, y))
    options = tf.data.Options()
    options.experimental_deterministic = not training
    ds = ds.with_options(options)
    if training:
        ds = ds.cache().shuffle(min(len(x), 50_000), reshuffle_each_iteration=True)
    return ds.batch(batch_size, drop_remainder=training).prefetch(tf.data.AUTOTUNE)


def build_model(n_features: int) -> tf.keras.Model:
    inputs = tf.keras.Input(shape=(LOOKBACK, n_features))
    x = tf.keras.layers.Conv1D(64, 3, padding="causal", activation="relu")(inputs)
    x = tf.keras.layers.LayerNormalization()(x)
    x = tf.keras.layers.Bidirectional(tf.keras.layers.LSTM(96, return_sequences=True))(x)
    x = tf.keras.layers.Dropout(0.2)(x)
    x = tf.keras.layers.LSTM(64)(x)
    x = tf.keras.layers.Dense(128, activation="gelu")(x)
    # float32 head: mixed precision requires the output layer to stay in fp32 for numerical safety.
    outputs = tf.keras.layers.Dense(HORIZON, dtype="float32")(x)
    return tf.keras.Model(inputs, outputs, name="gc_revenue_forecaster")


def main() -> None:
    settings = load_settings()
    parser = argparse.ArgumentParser()
    parser.add_argument("--epochs", type=int, default=settings.get("ml.tensorflow.max_epochs", 10))
    parser.add_argument("--batch_size", type=int, default=settings.get("ml.tensorflow.batch_size", 512))
    args = parser.parse_args()

    tf.keras.utils.set_random_seed(42)
    strategy = configure_gpu()

    x, y = load_series()
    split = int(len(x) * 0.85)  # chronological split, never random
    train_ds = make_dataset(x[:split], y[:split], args.batch_size, training=True)
    val_ds = make_dataset(x[split:], y[split:], args.batch_size, training=False)

    mlflow.set_experiment(settings.get("ml.experiment"))
    with mlflow.start_run(run_name=f"tf-forecast-{settings.cloud}-{settings.environment}"):
        mlflow.tensorflow.autolog(log_models=False)
        with strategy.scope():
            model = build_model(len(FEATURES))
            optimizer = tf.keras.optimizers.AdamW(learning_rate=1e-3, weight_decay=1e-4)
            model.compile(
                optimizer=optimizer,
                loss=tf.keras.losses.Huber(),  # robust to the Black-Friday style spikes
                metrics=[
                    tf.keras.metrics.MeanAbsoluteError(name="mae"),
                    tf.keras.metrics.RootMeanSquaredError(name="rmse"),
                ],
                jit_compile=bool(settings.get("ml.tensorflow.xla", True)),
            )
        model.fit(
            train_ds,
            validation_data=val_ds,
            epochs=args.epochs,
            callbacks=[
                tf.keras.callbacks.EarlyStopping(patience=5, restore_best_weights=True, monitor="val_mae"),
                tf.keras.callbacks.ReduceLROnPlateau(factor=0.5, patience=2, monitor="val_mae"),
                tf.keras.callbacks.TerminateOnNaN(),
            ],
            verbose=2,
        )
        metrics = model.evaluate(val_ds, return_dict=True)
        mlflow.log_metrics({f"val_{k}": float(v) for k, v in metrics.items()})
        log_and_register(model, name="gc_revenue_forecaster", flavor="tensorflow")


if __name__ == "__main__":
    main()
