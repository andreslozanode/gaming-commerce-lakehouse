"""Declarative expectations with warn / drop / fail semantics (severity comes from the env config).

Every Silver/Gold job runs its rules through `apply_expectations`; violations are always written
to the quarantine table, whatever the action, so nothing disappears silently.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger

log = get_logger(__name__)

Action = Literal["warn", "drop", "fail"]


@dataclass(frozen=True)
class Expectation:
    name: str
    condition: str  # SQL boolean expression that must be TRUE for a valid row
    action: Action = "warn"


SILVER_SALES_RULES = [
    Expectation("game_title_not_null", "title IS NOT NULL AND length(trim(title)) > 0", "drop"),
    Expectation("platform_known", "platform_code IS NOT NULL", "drop"),
    Expectation("year_plausible", "release_year BETWEEN 1990 AND year(current_date()) + 1", "warn"),
    Expectation("sales_non_negative", "global_sales_musd >= 0", "fail"),
    Expectation(
        "regions_sum_matches",
        "abs(coalesce(na_sales,0)+coalesce(eu_sales,0)+coalesce(jp_sales,0)+coalesce(other_sales,0) - global_sales_musd) < 0.05",
        "warn",
    ),
]

SILVER_EVENTS_RULES = [
    Expectation("event_id_not_null", "event_id IS NOT NULL", "fail"),
    Expectation("price_positive", "unit_price > 0 OR channel_code = 'membership_included'", "drop"),
    Expectation(
        "discount_bounded", "discount_pct IS NULL OR (discount_pct >= 0 AND discount_pct <= 1)", "drop"
    ),
    Expectation(
        "membership_tier_consistency",
        "(channel_code LIKE 'membership%' AND membership_tier IS NOT NULL) OR channel_code NOT LIKE 'membership%'",
        "warn",
    ),
    Expectation("freshness", "occurred_at <= current_timestamp() + interval 5 minutes", "warn"),
]


def apply_expectations(df: DataFrame, rules: list[Expectation], *, quarantine_table: str) -> DataFrame:
    settings = load_settings()
    ov = settings.on_violation
    override = cast("Action | None", ov if ov in ("warn", "drop", "fail") else None)

    flagged = df
    for rule in rules:
        flagged = flagged.withColumn(f"_dq_{rule.name}", F.expr(rule.condition))

    flag_cols = [f"_dq_{r.name}" for r in rules]
    flagged = flagged.withColumn(
        "_dq_failed_rules",
        F.array_compact(
            F.array(*[F.when(~F.col(c), F.lit(r.name)) for c, r in zip(flag_cols, rules, strict=True)])
        ),
    ).withColumn("_dq_is_valid", F.size("_dq_failed_rules") == 0)

    invalid = flagged.filter(~F.col("_dq_is_valid"))
    invalid_count = invalid.count()

    if invalid_count:
        (
            invalid.withColumn("_quarantined_at", F.current_timestamp())
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(quarantine_table)
        )

        # `fail` rules escalate regardless of env; env only tightens warn -> drop/fail.
        hard_rules = {r.name for r in rules if r.action == "fail" or override == "fail"}
        breached = (
            invalid.select(F.explode("_dq_failed_rules").alias("rule"))
            .filter(F.col("rule").isin(list(hard_rules)))
            .limit(1)
            .count()
        )
        if breached:
            raise ValueError(
                f"Hard data-quality expectation breached ({invalid_count} rows quarantined in {quarantine_table})"
            )
        log.warning(
            "expectations violated",
            extra={"extra_fields": {"invalid_rows": invalid_count, "quarantine": quarantine_table}},
        )

    drop_mode = override == "drop" or any(r.action == "drop" for r in rules)
    result = flagged.filter(F.col("_dq_is_valid")) if drop_mode else flagged
    return result.drop(*flag_cols, "_dq_is_valid", "_dq_failed_rules")
