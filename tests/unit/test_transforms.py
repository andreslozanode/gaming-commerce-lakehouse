from pyspark.sql import Row


def test_era_binning_matches_release_year(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        [
            Row(release_year=1997),
            Row(release_year=2004),
            Row(release_year=2015),
            Row(release_year=2022),
        ]
    ).withColumn(
        "era",
        F.expr("""
        CASE WHEN release_year < 2000 THEN '90s'
             WHEN release_year < 2010 THEN '00s'
             WHEN release_year < 2020 THEN '10s' ELSE '20s' END"""),
    )
    assert [r.era for r in df.collect()] == ["90s", "00s", "10s", "20s"]


def test_net_revenue_applies_the_discount(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([Row(unit_price=60.0, quantity=2, discount_pct=0.25)])
    result = df.withColumn(
        "net_revenue",
        F.round(
            F.col("unit_price") * F.col("quantity") * (1 - F.coalesce(F.col("discount_pct"), F.lit(0.0))), 4
        ),
    ).first()
    assert result.net_revenue == 90.0
