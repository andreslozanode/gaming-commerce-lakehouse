"""Small reference dimensions built with pandas — the right tool below ~1M rows.

Optimizations applied (see docs/OPTIMIZATIONS.md#pandas):
  * PyArrow-backed dtypes  -> ~3-5x less memory than object dtype, zero-copy to Spark/Delta
  * category dtype on low-cardinality keys (platform, era, publisher)
  * downcast numerics      -> int64 -> int16/int32 where the range allows
  * vectorized era binning -> pd.cut instead of .apply(lambda)
"""

from __future__ import annotations

import pandas as pd

CONSOLE_FAMILY = {
    "PS": "PlayStation",
    "PS2": "PlayStation",
    "PS3": "PlayStation",
    "PS4": "PlayStation",
    "PS5": "PlayStation",
    "PSP": "PlayStation",
    "PSV": "PlayStation",
    "XB": "Xbox",
    "X360": "Xbox",
    "XOne": "Xbox",
    "XS": "Xbox",
    "XBL": "Xbox",
}

GENERATION = {
    "PS": 5,
    "PS2": 6,
    "XB": 6,
    "PS3": 7,
    "X360": 7,
    "PS4": 8,
    "XOne": 8,
    "PS5": 9,
    "XS": 9,
}

LAUNCH_YEAR = {
    "PS": 1994,
    "PS2": 2000,
    "XB": 2001,
    "PS3": 2006,
    "X360": 2005,
    "PS4": 2013,
    "XOne": 2013,
    "PS5": 2020,
    "XS": 2020,
    "PSP": 2004,
    "PSV": 2011,
}


def build_dim_console() -> pd.DataFrame:
    df = pd.DataFrame(
        {"platform_code": list(CONSOLE_FAMILY)},
    ).assign(
        console_family=lambda d: d["platform_code"].map(CONSOLE_FAMILY),
        generation=lambda d: d["platform_code"].map(GENERATION).astype("Int8"),
        launch_year=lambda d: d["platform_code"].map(LAUNCH_YEAR).astype("Int16"),
    )
    df["era"] = pd.cut(
        df["launch_year"],
        bins=[1989, 1999, 2009, 2019, 2029],
        labels=["90s", "00s", "10s", "20s"],
    )
    for col in ("platform_code", "console_family"):
        df[col] = df[col].astype("category")
    return df.convert_dtypes(dtype_backend="pyarrow")


def build_dim_purchase_channel() -> pd.DataFrame:
    rows = [
        ("physical_retail", "PHYSICAL", "Boxed disc/cartridge bought in store", True),
        ("physical_online", "PHYSICAL", "Boxed copy shipped from an online retailer", True),
        ("digital_store", "DIGITAL", "PS Store / Microsoft Store download", False),
        ("membership_included", "MEMBERSHIP", "Title claimed via PS Plus / Game Pass", False),
        ("membership_fee", "MEMBERSHIP", "Recurring subscription charge", False),
        ("console_hardware", "HARDWARE", "Console purchase (PS/Xbox SKU)", True),
    ]
    df = pd.DataFrame(rows, columns=["channel_code", "channel_type", "description", "is_shippable"])
    df["channel_code"] = df["channel_code"].astype("category")
    df["channel_type"] = df["channel_type"].astype("category")
    return df.convert_dtypes(dtype_backend="pyarrow")
