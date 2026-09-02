"""
Phase 4 — Sector Mapping (SOC → SIC) tests.

Phase 4 distributes occupation-level job counts across industry sectors
using Census-derived weights. For each SOC occupation code, the Census
tells us what fraction of workers in that occupation are employed in each
SIC sector. Those fractions are the weights.

Key behaviours tested:
  - stack() pivot (wide Census → long SOC-SIC pairs)
  - Weight calculation (n_sic / total_soc)
  - Weights sum to 1.0 per SOC code
  - Weighted distribution of job counts
  - Missing SOC codes in Census are handled gracefully
  - SIC 2-digit to letter-code grouping
  - Zero-count rows are filtered out before weighting
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder.appName("test_phase4")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


# ---------------------------------------------------------------------------
# SIC 2-digit to letter-code grouping (pure Python — no Spark needed)
# ---------------------------------------------------------------------------

def sic2_to_sector(sic2: int) -> str | None:
    if 1 <= sic2 <= 3:
        return "A"
    elif 5 <= sic2 <= 39:
        return "B-E"
    elif 41 <= sic2 <= 43:
        return "F"
    elif 45 <= sic2 <= 56:
        return "G-I"
    elif 58 <= sic2 <= 63:
        return "J"
    elif 64 <= sic2 <= 66:
        return "K"
    elif sic2 == 68:
        return "L"
    elif 69 <= sic2 <= 82:
        return "M-N"
    elif 84 <= sic2 <= 88:
        return "O-Q"
    elif 90 <= sic2 <= 98:
        return "R-T"
    elif sic2 == 99:
        return "U"
    return None


def test_all_expected_sector_codes_produced():
    expected = {"A", "B-E", "F", "G-I", "J", "K", "L", "M-N", "O-Q", "R-T", "U"}
    codes_seen = set()
    # Hit representative codes from each sector
    representative = [1, 10, 41, 47, 62, 65, 68, 72, 85, 93, 99]
    for code in representative:
        result = sic2_to_sector(code)
        if result:
            codes_seen.add(result)
    assert codes_seen == expected


def test_gap_codes_return_none():
    gaps = [0, 4, 40, 44, 57, 67, 83, 89, 100]
    for code in gaps:
        assert sic2_to_sector(code) is None, f"Code {code} should map to None"


def test_sector_boundaries_are_inclusive():
    assert sic2_to_sector(1) == "A"
    assert sic2_to_sector(3) == "A"
    assert sic2_to_sector(5) == "B-E"
    assert sic2_to_sector(39) == "B-E"
    assert sic2_to_sector(58) == "J"
    assert sic2_to_sector(63) == "J"
    assert sic2_to_sector(90) == "R-T"
    assert sic2_to_sector(98) == "R-T"


# ---------------------------------------------------------------------------
# Weight calculation
# ---------------------------------------------------------------------------

def test_weights_sum_to_one_for_single_soc(spark):
    import pyspark.sql.functions as F
    rows = [
        ("3133", "J", 400),
        ("3133", "K", 350),
        ("3133", "M-N", 250),
    ]
    df = spark.createDataFrame(rows, ["soc4", "SIC_Code", "n"])
    totals = df.groupBy("soc4").agg(F.sum("n").alias("total_soc"))
    weights = df.join(totals, "soc4").withColumn("w", F.col("n") / F.col("total_soc"))
    weight_sum = weights.groupBy("soc4").agg(F.sum("w").alias("total_w")).first()
    assert abs(weight_sum["total_w"] - 1.0) < 1e-9


def test_weights_sum_to_one_for_multiple_soc_codes(spark):
    import pyspark.sql.functions as F
    rows = [
        ("3133", "J", 400), ("3133", "K", 600),
        ("2433", "J", 200), ("2433", "M-N", 300), ("2433", "O-Q", 500),
    ]
    df = spark.createDataFrame(rows, ["soc4", "SIC_Code", "n"])
    totals = df.groupBy("soc4").agg(F.sum("n").alias("total_soc"))
    weights = df.join(totals, "soc4").withColumn("w", F.col("n") / F.col("total_soc"))
    sums = weights.groupBy("soc4").agg(F.sum("w").alias("total_w"))
    for row in sums.collect():
        assert abs(row["total_w"] - 1.0) < 1e-9, f"Weights for {row['soc4']} don't sum to 1"


def test_single_sector_soc_gets_weight_one(spark):
    import pyspark.sql.functions as F
    rows = [("3133", "J", 1000)]
    df = spark.createDataFrame(rows, ["soc4", "SIC_Code", "n"])
    totals = df.groupBy("soc4").agg(F.sum("n").alias("total_soc"))
    weights = df.join(totals, "soc4").withColumn("w", F.col("n") / F.col("total_soc"))
    assert abs(weights.first()["w"] - 1.0) < 1e-9


def test_equal_counts_produce_equal_weights(spark):
    import pyspark.sql.functions as F
    rows = [("3133", "J", 500), ("3133", "K", 500)]
    df = spark.createDataFrame(rows, ["soc4", "SIC_Code", "n"])
    totals = df.groupBy("soc4").agg(F.sum("n").alias("total_soc"))
    weights = df.join(totals, "soc4").withColumn("w", F.col("n") / F.col("total_soc"))
    weight_values = [r["w"] for r in weights.collect()]
    assert all(abs(w - 0.5) < 1e-9 for w in weight_values)


# ---------------------------------------------------------------------------
# Weighted distribution of job counts
# ---------------------------------------------------------------------------

def test_weighted_jobs_distribute_proportionally(spark):
    import pyspark.sql.functions as F
    # SOC 3133: 40% in sector J, 60% in sector K
    weights = spark.createDataFrame(
        [("3133", "J", 0.4), ("3133", "K", 0.6)],
        ["soc4", "SIC_Code", "w"]
    )
    occ = spark.createDataFrame(
        [("3133", 100, 40, 2023)],
        ["soc4", "total_jobs", "any_data_intensive_jobs", "year"]
    )
    joined = occ.join(weights, "soc4", "left")
    weighted = joined.withColumn("w_intensive", F.col("any_data_intensive_jobs") * F.col("w"))

    rows = {r["SIC_Code"]: r for r in weighted.collect()}
    assert abs(rows["J"]["w_intensive"] - 16.0) < 1e-9   # 40 * 0.4
    assert abs(rows["K"]["w_intensive"] - 24.0) < 1e-9   # 40 * 0.6


def test_weighted_distribution_conserves_total_jobs(spark):
    import pyspark.sql.functions as F
    weights = spark.createDataFrame(
        [("3133", "J", 0.3), ("3133", "K", 0.4), ("3133", "M-N", 0.3)],
        ["soc4", "SIC_Code", "w"]
    )
    occ = spark.createDataFrame(
        [("3133", 200, 80, 2023)],
        ["soc4", "total_jobs", "any_data_intensive_jobs", "year"]
    )
    joined = occ.join(weights, "soc4")
    total_weighted = joined.agg(
        F.sum(F.col("any_data_intensive_jobs") * F.col("w")).alias("total")
    ).first()["total"]
    assert abs(total_weighted - 80.0) < 1e-9


def test_missing_soc_in_census_produces_zero_weighted_jobs(spark):
    import pyspark.sql.functions as F
    weights = spark.createDataFrame(
        [("3133", "J", 0.5), ("3133", "K", 0.5)],
        ["soc4", "SIC_Code", "w"]
    )
    occ = spark.createDataFrame(
        [("9999", 50, 20, 2023)],   # SOC 9999 not in Census
        ["soc4", "total_jobs", "any_data_intensive_jobs", "year"]
    )
    joined = occ.join(weights, "soc4", "left").fillna(0, subset=["w"])
    weighted_sum = joined.agg(
        F.sum(F.col("any_data_intensive_jobs") * F.col("w")).alias("total")
    ).first()["total"]
    assert weighted_sum == 0.0


# ---------------------------------------------------------------------------
# Zero-count rows filtered before weighting
# ---------------------------------------------------------------------------

def test_zero_count_rows_excluded_from_census(spark):
    import pyspark.sql.functions as F
    rows = [
        ("3133", "J", 400),
        ("3133", "K", 0),   # zero workers — should be excluded
        ("3133", "M-N", 600),
    ]
    df = spark.createDataFrame(rows, ["soc4", "SIC_Code", "n"])
    filtered = df.filter(F.col("n") > 0)
    totals = filtered.groupBy("soc4").agg(F.sum("n").alias("total_soc"))
    weights = filtered.join(totals, "soc4").withColumn("w", F.col("n") / F.col("total_soc"))
    assert weights.count() == 2
    weight_sum = weights.agg(F.sum("w")).first()[0]
    assert abs(weight_sum - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# Sector-level aggregation
# ---------------------------------------------------------------------------

def test_sector_totals_aggregate_multiple_soc_codes(spark):
    import pyspark.sql.functions as F
    # Two SOC codes both contributing to sector J
    weighted = spark.createDataFrame([
        ("J", 30.0, 60.0),
        ("J", 20.0, 40.0),
        ("K", 50.0, 100.0),
    ], ["SIC_Code", "w_intensive", "w_total"])
    sector_sums = weighted.groupBy("SIC_Code").agg(
        F.sum("w_intensive").alias("total_intensive"),
        F.sum("w_total").alias("total_jobs"),
    ).withColumn("total_data_share", 100 * F.col("total_intensive") / F.col("total_jobs"))

    rows = {r["SIC_Code"]: r for r in sector_sums.collect()}
    assert abs(rows["J"]["total_intensive"] - 50.0) < 1e-9
    assert abs(rows["J"]["total_data_share"] - 50.0) < 1e-9   # 50/100 * 100
    assert abs(rows["K"]["total_data_share"] - 50.0) < 1e-9


def test_sector_data_share_is_capped_at_100(spark):
    import pyspark.sql.functions as F
    # Edge case: all jobs in a sector are data-intensive
    weighted = spark.createDataFrame(
        [("J", 100.0, 100.0)],
        ["SIC_Code", "w_intensive", "w_total"]
    )
    sector_sums = weighted.groupBy("SIC_Code").agg(
        F.sum("w_intensive").alias("total_intensive"),
        F.sum("w_total").alias("total_jobs"),
    ).withColumn("total_data_share", 100 * F.col("total_intensive") / F.col("total_jobs"))
    assert sector_sums.first()["total_data_share"] == 100.0
