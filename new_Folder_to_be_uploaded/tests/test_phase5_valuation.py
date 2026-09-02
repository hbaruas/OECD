"""
Phase 5 — Economic Valuation tests.

Phase 5 joins sector-level data-intensity shares with the Supply and Use
Table (SUT) to compute investment estimates under three alpha scenarios.

Key behaviours tested:
  - Investment formula: alpha × COMP_EMP × (data_share / 100)
  - GVA share: (investment / GVA) × 100
  - All three alpha scenarios (sector, economy average, conservative)
  - Zero and edge-case inputs
  - SUT year filtering
  - Linearity and proportionality
  - Cross-sector consistency
  - Spark-based end-to-end valuation on a small dataset
"""

import sys
import os
import math
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder.appName("test_phase5")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


# ---------------------------------------------------------------------------
# Investment formula — pure Python
# ---------------------------------------------------------------------------

def investment(alpha, comp_emp, data_share_pct):
    return alpha * comp_emp * (data_share_pct / 100)


def gva_share(inv, gva):
    return (inv / gva) * 100 if gva > 0 else 0.0


def test_basic_investment_calculation():
    result = investment(3.62, 100_000_000_000, 5.0)
    assert abs(result - 18_100_000_000) < 1


def test_investment_with_alpha_one_equals_wage_component():
    comp_emp = 200_000_000_000
    share = 8.0
    result = investment(1.0, comp_emp, share)
    assert abs(result - comp_emp * share / 100) < 1


def test_zero_data_share_gives_zero_investment():
    assert investment(3.62, 100_000_000_000, 0.0) == 0.0


def test_hundred_percent_data_share():
    result = investment(3.62, 100_000_000_000, 100.0)
    assert abs(result - 3.62 * 100_000_000_000) < 1


def test_investment_scales_linearly_with_data_share():
    base = investment(3.62, 100_000_000_000, 5.0)
    double = investment(3.62, 100_000_000_000, 10.0)
    assert abs(double / base - 2.0) < 1e-9


def test_investment_scales_linearly_with_comp_emp():
    base = investment(3.62, 100_000_000_000, 5.0)
    double = investment(3.62, 200_000_000_000, 5.0)
    assert abs(double / base - 2.0) < 1e-9


def test_investment_scales_linearly_with_alpha():
    base = investment(1.0, 100_000_000_000, 5.0)
    scaled = investment(3.62, 100_000_000_000, 5.0)
    assert abs(scaled / base - 3.62) < 1e-9


# ---------------------------------------------------------------------------
# Three alpha scenarios
# ---------------------------------------------------------------------------

def test_conservative_alpha_less_than_economy_average():
    comp_emp = 100_000_000_000
    share = 5.0
    assert investment(config.ALPHA_LOW, comp_emp, share) < investment(config.ALPHA_ECONOMY_AVG, comp_emp, share)


def test_sector_specific_alpha_for_construction_is_highest():
    # Construction (F) has alpha=6.64 — one of the highest
    f_alpha = config.ALPHA_MAP["F"]
    assert f_alpha > config.ALPHA_ECONOMY_AVG


def test_all_three_scenarios_produce_distinct_values():
    comp_emp = 100_000_000_000
    share = 5.0
    raw = investment(1.0, comp_emp, share)
    conservative = investment(config.ALPHA_LOW, comp_emp, share)
    avg = investment(config.ALPHA_ECONOMY_AVG, comp_emp, share)
    assert raw < conservative < avg


def test_sector_specific_investment_for_every_sector():
    comp_emp = 50_000_000_000
    share = 4.0
    for sic, alpha in config.ALPHA_MAP.items():
        result = investment(alpha, comp_emp, share)
        assert result > 0, f"Sector {sic} produced non-positive investment"


# ---------------------------------------------------------------------------
# GVA share
# ---------------------------------------------------------------------------

def test_gva_share_calculation():
    inv = investment(3.62, 100_000_000_000, 5.0)  # £18.1bn
    share = gva_share(inv, 150_000_000_000)
    assert abs(share - 12.0667) < 0.001


def test_zero_gva_returns_zero_share():
    assert gva_share(1_000_000, 0) == 0.0


def test_gva_share_at_100_pct_data_share():
    alpha = config.ALPHA_ECONOMY_AVG
    comp_emp = 100_000_000_000
    gva = 200_000_000_000
    inv = investment(alpha, comp_emp, 100.0)
    share = gva_share(inv, gva)
    expected = alpha * (comp_emp / gva) * 100
    assert abs(share - expected) < 0.001


def test_gva_share_is_positive_for_nonzero_inputs():
    inv = investment(3.62, 50_000_000_000, 6.0)
    assert gva_share(inv, 100_000_000_000) > 0


def test_higher_alpha_gives_higher_gva_share():
    comp_emp = 80_000_000_000
    gva = 120_000_000_000
    share_low = gva_share(investment(config.ALPHA_LOW, comp_emp, 5.0), gva)
    share_avg = gva_share(investment(config.ALPHA_ECONOMY_AVG, comp_emp, 5.0), gva)
    assert share_low < share_avg


# ---------------------------------------------------------------------------
# SUT year filtering (Spark)
# ---------------------------------------------------------------------------

def test_sut_year_filter_keeps_only_target_year(spark):
    import pyspark.sql.functions as F
    sut_data = spark.createDataFrame([
        ("J", 2022, 100_000.0, 60_000.0),
        ("J", 2023, 110_000.0, 65_000.0),
        ("J", 2024, 115_000.0, 68_000.0),
    ], ["SIC_Code", "year", "GVA_basic_prices", "COMP_EMP"])

    filtered = sut_data.filter(F.col("year") == config.SUT_YEAR)
    assert filtered.count() == 1
    assert filtered.first()["year"] == config.SUT_YEAR


def test_sut_filter_returns_empty_if_year_not_present(spark):
    import pyspark.sql.functions as F
    sut_data = spark.createDataFrame([
        ("J", 2021, 100_000.0, 60_000.0),
    ], ["SIC_Code", "year", "GVA_basic_prices", "COMP_EMP"])
    filtered = sut_data.filter(F.col("year") == 2099)
    assert filtered.count() == 0


# ---------------------------------------------------------------------------
# Spark end-to-end valuation
# ---------------------------------------------------------------------------

def test_spark_valuation_produces_correct_investment(spark):
    import pyspark.sql.functions as F
    sector_df = spark.createDataFrame(
        [("J", 10.0, 2023)],
        ["SIC_Code", "total_data_share", "year"]
    )
    sut_df = spark.createDataFrame(
        [("J", 150_000_000_000.0, 80_000_000_000.0)],
        ["SIC_Code", "GVA_basic_prices", "COMP_EMP"]
    )
    valued = sector_df.join(sut_df, "SIC_Code") \
        .withColumn("alpha", F.lit(config.ALPHA_ECONOMY_AVG)) \
        .withColumn(
            "total_investment",
            F.col("alpha") * F.col("COMP_EMP") * (F.col("total_data_share") / 100)
        )
    row = valued.first()
    expected = config.ALPHA_ECONOMY_AVG * 80_000_000_000 * 0.10
    assert abs(row["total_investment"] - expected) < 1


def test_spark_gva_share_when_zero_data_share(spark):
    import pyspark.sql.functions as F
    sector_df = spark.createDataFrame(
        [("A", 0.0, 2023)],
        ["SIC_Code", "total_data_share", "year"]
    )
    sut_df = spark.createDataFrame(
        [("A", 20_000_000_000.0, 5_000_000_000.0)],
        ["SIC_Code", "GVA_basic_prices", "COMP_EMP"]
    )
    valued = sector_df.join(sut_df, "SIC_Code") \
        .withColumn("alpha", F.lit(config.ALPHA_ECONOMY_AVG)) \
        .withColumn("investment", F.col("alpha") * F.col("COMP_EMP") * (F.col("total_data_share") / 100)) \
        .withColumn(
            "inv_share_gva",
            F.when(F.col("GVA_basic_prices") > 0,
                   (F.col("investment") / F.col("GVA_basic_prices")) * 100
            ).otherwise(0.0)
        )
    assert valued.first()["inv_share_gva"] == 0.0


def test_spark_valuation_across_multiple_sectors(spark):
    import pyspark.sql.functions as F
    sector_df = spark.createDataFrame([
        ("J", 10.0, 2023),
        ("K", 8.0, 2023),
        ("M-N", 6.0, 2023),
    ], ["SIC_Code", "total_data_share", "year"])
    sut_df = spark.createDataFrame([
        ("J", 150_000_000_000.0, 80_000_000_000.0),
        ("K", 200_000_000_000.0, 100_000_000_000.0),
        ("M-N", 300_000_000_000.0, 180_000_000_000.0),
    ], ["SIC_Code", "GVA_basic_prices", "COMP_EMP"])

    valued = sector_df.join(sut_df, "SIC_Code") \
        .withColumn("alpha", F.lit(config.ALPHA_ECONOMY_AVG)) \
        .withColumn("investment", F.col("alpha") * F.col("COMP_EMP") * (F.col("total_data_share") / 100)) \
        .withColumn("inv_share_gva", (F.col("investment") / F.col("GVA_basic_prices")) * 100)

    assert valued.count() == 3
    for row in valued.collect():
        assert row["investment"] > 0
        assert row["inv_share_gva"] > 0


def test_total_economy_investment_sums_across_sectors(spark):
    import pyspark.sql.functions as F
    sector_df = spark.createDataFrame([
        ("J", 10.0, 2023),
        ("K", 5.0, 2023),
    ], ["SIC_Code", "total_data_share", "year"])
    sut_df = spark.createDataFrame([
        ("J", 100_000_000_000.0, 50_000_000_000.0),
        ("K", 100_000_000_000.0, 50_000_000_000.0),
    ], ["SIC_Code", "GVA_basic_prices", "COMP_EMP"])

    alpha = config.ALPHA_ECONOMY_AVG
    valued = sector_df.join(sut_df, "SIC_Code") \
        .withColumn("investment", F.lit(alpha) * F.col("COMP_EMP") * (F.col("total_data_share") / 100))

    total = valued.agg(F.sum("investment")).first()[0]
    expected = (alpha * 50e9 * 0.10) + (alpha * 50e9 * 0.05)
    assert abs(total - expected) < 1
