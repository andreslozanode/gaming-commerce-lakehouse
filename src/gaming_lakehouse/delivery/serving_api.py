"""Low-latency serving API (FastAPI) over the Gold layer + the registered models.

Deployed to Cloud Run (GCP) or Container Apps (Azure) by the CD pipeline. Reads are served
from the warehouse, not from Delta, so the lakehouse never sits in a user-facing latency path.
"""

from __future__ import annotations

from functools import lru_cache

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger

log = get_logger(__name__)
app = FastAPI(title="Gaming Commerce Lakehouse API", version="1.0.0")


class EraSummary(BaseModel):
    era: str
    console_family: str
    channel_type: str
    net_revenue: float | None = None
    physical_units_m: float | None = None


class ChurnScore(BaseModel):
    player_id: str
    churn_probability: float
    risk_band: str


@lru_cache(maxsize=1)
def _warehouse():
    s = load_settings()
    if s.cloud == "gcp":
        from google.cloud import bigquery  # type: ignore

        return bigquery.Client()
    import pyodbc  # type: ignore

    from gaming_lakehouse.secrets import get_secret

    return pyodbc.connect(get_secret("synapse-odbc-dsn"))


@app.get("/health")
def health() -> dict[str, str]:
    s = load_settings()
    return {"status": "ok", "cloud": s.cloud, "environment": s.environment}


@app.get("/v1/sales/by-era", response_model=list[EraSummary])
def sales_by_era(
    console_family: str = Query("PlayStation", pattern="^(PlayStation|Xbox)$"),
) -> list[EraSummary]:
    s = load_settings()
    table = f"{s.get('warehouse.dataset')}_{s.environment}.gold_sales_by_era_platform"
    sql = f"SELECT era, console_family, channel_type, net_revenue, physical_units_m FROM {table} WHERE console_family = ?"  # noqa: S608
    try:
        client = _warehouse()
        rows = (
            client.query(sql.replace("?", f"'{console_family}'")).result()
            if s.cloud == "gcp"
            else client.execute(sql, console_family).fetchall()
        )
        return [EraSummary(**dict(r)) if s.cloud == "gcp" else EraSummary(*r) for r in rows]
    except Exception as exc:
        log.warning("query failed", extra={"extra_fields": {"error": str(exc)}})
        raise HTTPException(status_code=503, detail="warehouse unavailable") from exc


@app.get("/v1/players/{player_id}/churn", response_model=ChurnScore)
def player_churn(player_id: str) -> ChurnScore:
    s = load_settings()
    table = f"{s.get('warehouse.dataset')}_{s.environment}.gold_player_churn_scores"
    client = _warehouse()
    sql = f"SELECT player_id, churn_probability, risk_band FROM {table} WHERE player_id = '{player_id}'"  # noqa: S608
    rows = list(client.query(sql).result()) if s.cloud == "gcp" else client.execute(sql).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="player not scored yet")
    row = rows[0]
    return ChurnScore(player_id=row[0], churn_probability=float(row[1]), risk_band=row[2])
