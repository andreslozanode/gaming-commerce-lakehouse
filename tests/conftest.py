from __future__ import annotations

import os

import pytest

os.environ.setdefault("CLOUD", "gcp")
os.environ.setdefault("ENVIRONMENT", "dev")

_DELTA_AVAILABLE = True


@pytest.fixture(scope="session")
def spark():
    """Local Spark + Delta. Small shuffle partitions keep the suite under a minute.

    Offline resilience: resolving the Delta jars needs Maven Central. On an
    air-gapped machine (or a sandboxed runner) that resolution fails, so we fall
    back to a vanilla Spark session and let Delta-dependent suites skip cleanly
    via the `delta_spark` fixture instead of dying with an Ivy stack trace.
    """
    global _DELTA_AVAILABLE
    from pyspark.sql import SparkSession

    def _base_builder():
        return (
            SparkSession.builder.appName("tests")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
        )

    try:
        from delta import configure_spark_with_delta_pip

        builder = (
            _base_builder()
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        session = configure_spark_with_delta_pip(builder).getOrCreate()
        # Force jar resolution problems to surface here, not mid-test.
        session.sql("SELECT 1").collect()
    except Exception:  # pragma: no cover - only on machines without Maven access
        _DELTA_AVAILABLE = False
        session = _base_builder().getOrCreate()

    yield session
    session.stop()


@pytest.fixture(scope="session")
def delta_spark(spark):
    """Spark session guaranteed to have Delta Lake; skips the test otherwise."""
    if not _DELTA_AVAILABLE:
        pytest.skip("Delta jars unavailable offline - run `make test` with Maven access or in CI")
    return spark
