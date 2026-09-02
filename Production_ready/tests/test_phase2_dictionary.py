"""
Phase 2 — Dictionary Building tests.

Phase 2 computes a relative share score for every noun chunk across the
economy. Terms significantly over-represented in the target (anchor)
occupations form the OECD vocabulary.

The three filters that must ALL pass:
  1. relative_share >= REL_SHARE_THRESHOLD  (default 10.0)
  2. global_count >= 500                    (minimum absolute prevalence)
  3. avg_sim >= SIM_GROUNDING               (minimum similarity to "data")

Tests cover:
  - The explode(arrays_zip) unpacking step
  - Relative share calculation
  - Each of the three filters in isolation
  - Interaction between filters
  - Edge cases (empty data, all-anchor jobs, single job)
  - The countDistinct deduplication behaviour
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder.appName("test_phase2")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


# ---------------------------------------------------------------------------
# explode(arrays_zip) — unpacking packed arrays into rows
# ---------------------------------------------------------------------------

def test_arrays_zip_explode_produces_one_row_per_chunk(spark):
    import pyspark.sql.functions as F
    df = spark.createDataFrame(
        [("job_001", "3133", ["sql server", "database"], [0.8, 0.7])],
        ["doc_JobID", "doc_BGTOcc", "noun_chunks", "sim_scores"]
    )
    exploded = df.select(
        "doc_JobID", "doc_BGTOcc",
        F.explode(F.arrays_zip("noun_chunks", "sim_scores")).alias("zipped")
    ).select(
        "doc_JobID", "doc_BGTOcc",
        F.col("zipped.noun_chunks").alias("noun_chunk"),
        F.col("zipped.sim_scores").alias("sim_data"),
    )
    assert exploded.count() == 2


def test_arrays_zip_explode_preserves_chunk_score_pairing(spark):
    import pyspark.sql.functions as F
    df = spark.createDataFrame(
        [("job_001", "3133", ["sql", "nosql"], [0.9, 0.6])],
        ["doc_JobID", "doc_BGTOcc", "noun_chunks", "sim_scores"]
    )
    exploded = df.select(
        "doc_JobID",
        F.explode(F.arrays_zip("noun_chunks", "sim_scores")).alias("z")
    ).select(
        F.col("z.noun_chunks").alias("noun_chunk"),
        F.col("z.sim_scores").alias("sim_data"),
    )
    rows = {r["noun_chunk"]: r["sim_data"] for r in exploded.collect()}
    assert abs(rows["sql"] - 0.9) < 1e-9
    assert abs(rows["nosql"] - 0.6) < 1e-9


def test_empty_arrays_produce_no_rows(spark):
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType
    schema = StructType([
        StructField("doc_JobID", StringType()),
        StructField("noun_chunks", ArrayType(StringType())),
        StructField("sim_scores", ArrayType(DoubleType())),
    ])
    df = spark.createDataFrame([("job_001", [], [])], schema)
    exploded = df.select(
        "doc_JobID",
        F.explode(F.arrays_zip("noun_chunks", "sim_scores")).alias("z")
    )
    assert exploded.count() == 0


def test_multiple_jobs_explode_independently(spark):
    import pyspark.sql.functions as F
    df = spark.createDataFrame([
        ("job_001", ["sql", "oracle"], [0.8, 0.75]),
        ("job_002", ["python", "spark", "hadoop"], [0.7, 0.65, 0.6]),
    ], ["doc_JobID", "noun_chunks", "sim_scores"])
    exploded = df.select(
        "doc_JobID",
        F.explode(F.arrays_zip("noun_chunks", "sim_scores")).alias("z")
    )
    assert exploded.count() == 5


# ---------------------------------------------------------------------------
# Relative share calculation
# ---------------------------------------------------------------------------

def _build_valid_chunks(spark, rows):
    """Helper: create a valid_chunks DataFrame from (noun_chunk, doc_JobID, soc4) rows."""
    import pyspark.sql.functions as F
    return spark.createDataFrame(rows, ["noun_chunk", "doc_JobID", "soc4"])


def _compute_relative_share(spark, rows, anchor_socs):
    """Helper: run the full Phase 2 frequency computation on a small dataset."""
    import pyspark.sql.functions as F
    df = _build_valid_chunks(spark, rows)

    total_jobs = df.select("doc_JobID").distinct().count()
    anchor_jobs = max(
        df.filter(F.col("soc4").isin(anchor_socs)).select("doc_JobID").distinct().count(), 1
    )

    global_freq = df.groupBy("noun_chunk").agg(
        F.countDistinct("doc_JobID").alias("global_count"),
        F.lit(0.5).alias("avg_sim"),
    ).withColumn("share_economy", F.col("global_count") / total_jobs)

    anchor_freq = (
        df.filter(F.col("soc4").isin(anchor_socs))
        .groupBy("noun_chunk")
        .agg(F.countDistinct("doc_JobID").alias("count_anchor"))
        .withColumn("share_anchor", F.col("count_anchor") / anchor_jobs)
    )

    return (
        global_freq.join(anchor_freq, "noun_chunk", "left")
        .fillna(0)
        .withColumn("relative_share", F.col("share_anchor") / F.col("share_economy"))
    )


def test_term_only_in_anchor_gets_high_relative_share(spark):
    import pyspark.sql.functions as F
    rows = [
        # 5 anchor jobs contain "sql", 1 non-anchor job contains "sql"
        ("sql", "job_001", "3133"), ("sql", "job_002", "3133"), ("sql", "job_003", "3133"),
        ("sql", "job_004", "3133"), ("sql", "job_005", "3133"),
        ("sql", "job_006", "2425"),
        # Non-anchor term present in many jobs
        ("excel", "job_007", "2425"), ("excel", "job_008", "2425"),
    ]
    result = _compute_relative_share(spark, rows, ["3133"])
    row = result.filter(F.col("noun_chunk") == "sql").first()
    # sql: share_anchor = 5/5 = 1.0, share_economy = 6/8 = 0.75 → ratio ≈ 1.33
    assert row["relative_share"] > 1.0


def test_term_absent_from_anchor_has_zero_relative_share(spark):
    import pyspark.sql.functions as F
    rows = [
        ("excel", "job_001", "2425"),
        ("excel", "job_002", "2425"),
        ("sql", "job_003", "3133"),
    ]
    result = _compute_relative_share(spark, rows, ["3133"])
    row = result.filter(F.col("noun_chunk") == "excel").first()
    assert row["relative_share"] == 0.0


def test_term_in_all_jobs_has_relative_share_near_one(spark):
    import pyspark.sql.functions as F
    rows = [
        ("teamwork", "job_001", "3133"),
        ("teamwork", "job_002", "2425"),
    ]
    result = _compute_relative_share(spark, rows, ["3133"])
    row = result.filter(F.col("noun_chunk") == "teamwork").first()
    # share_anchor = 1/1 = 1.0, share_economy = 2/2 = 1.0 → ratio = 1.0
    assert abs(row["relative_share"] - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# countDistinct deduplication
# ---------------------------------------------------------------------------

def test_countdistinct_deduplicates_repeated_job_ids(spark):
    import pyspark.sql.functions as F
    # "sql" appears in 3 rows but only 2 distinct jobs
    rows = [
        ("sql", "job_001", "3133"),
        ("sql", "job_001", "3133"),  # duplicate — same job
        ("sql", "job_002", "3133"),
    ]
    df = spark.createDataFrame(rows, ["noun_chunk", "doc_JobID", "soc4"])
    result = df.groupBy("noun_chunk").agg(
        F.countDistinct("doc_JobID").alias("global_count")
    )
    assert result.first()["global_count"] == 2


def test_count_vs_countdistinct_differ_with_duplicates(spark):
    import pyspark.sql.functions as F
    rows = [("sql", "job_001", "3133")] * 5
    df = spark.createDataFrame(rows, ["noun_chunk", "doc_JobID", "soc4"])
    result = df.groupBy("noun_chunk").agg(
        F.count("doc_JobID").alias("raw_count"),
        F.countDistinct("doc_JobID").alias("distinct_count"),
    ).first()
    assert result["raw_count"] == 5
    assert result["distinct_count"] == 1


# ---------------------------------------------------------------------------
# The triple filter
# ---------------------------------------------------------------------------

def test_term_passing_all_three_filters_is_retained(spark):
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    schema = StructType([
        StructField("noun_chunk", StringType()),
        StructField("relative_share", DoubleType()),
        StructField("global_count", LongType()),
        StructField("avg_sim", DoubleType()),
    ])
    data = [("sql server", 15.0, 600, 0.72)]
    df = spark.createDataFrame(data, schema)
    filtered = df.filter(
        (F.col("relative_share") >= config.REL_SHARE_THRESHOLD) &
        (F.col("global_count") >= 500) &
        (F.col("avg_sim") >= config.SIM_GROUNDING)
    )
    assert filtered.count() == 1


def test_term_failing_only_relative_share_is_excluded(spark):
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    schema = StructType([
        StructField("noun_chunk", StringType()),
        StructField("relative_share", DoubleType()),
        StructField("global_count", LongType()),
        StructField("avg_sim", DoubleType()),
    ])
    data = [("spreadsheet", 5.0, 600, 0.72)]  # relative_share below 10
    df = spark.createDataFrame(data, schema)
    filtered = df.filter(
        (F.col("relative_share") >= config.REL_SHARE_THRESHOLD) &
        (F.col("global_count") >= 500) &
        (F.col("avg_sim") >= config.SIM_GROUNDING)
    )
    assert filtered.count() == 0


def test_term_failing_only_global_count_is_excluded(spark):
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    schema = StructType([
        StructField("noun_chunk", StringType()),
        StructField("relative_share", DoubleType()),
        StructField("global_count", LongType()),
        StructField("avg_sim", DoubleType()),
    ])
    data = [("rare term", 20.0, 3, 0.72)]  # global_count below 500
    df = spark.createDataFrame(data, schema)
    filtered = df.filter(
        (F.col("relative_share") >= config.REL_SHARE_THRESHOLD) &
        (F.col("global_count") >= 500) &
        (F.col("avg_sim") >= config.SIM_GROUNDING)
    )
    assert filtered.count() == 0


def test_term_failing_only_avg_sim_is_excluded(spark):
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    schema = StructType([
        StructField("noun_chunk", StringType()),
        StructField("relative_share", DoubleType()),
        StructField("global_count", LongType()),
        StructField("avg_sim", DoubleType()),
    ])
    data = [("general skills", 12.0, 700, 0.10)]  # avg_sim below 0.35
    df = spark.createDataFrame(data, schema)
    filtered = df.filter(
        (F.col("relative_share") >= config.REL_SHARE_THRESHOLD) &
        (F.col("global_count") >= 500) &
        (F.col("avg_sim") >= config.SIM_GROUNDING)
    )
    assert filtered.count() == 0


def test_terms_at_exact_threshold_boundaries_are_retained(spark):
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    schema = StructType([
        StructField("noun_chunk", StringType()),
        StructField("relative_share", DoubleType()),
        StructField("global_count", LongType()),
        StructField("avg_sim", DoubleType()),
    ])
    # Exact boundary values — must be retained (filters use >= not >)
    data = [("boundary term", float(config.REL_SHARE_THRESHOLD), 500, config.SIM_GROUNDING)]
    df = spark.createDataFrame(data, schema)
    filtered = df.filter(
        (F.col("relative_share") >= config.REL_SHARE_THRESHOLD) &
        (F.col("global_count") >= 500) &
        (F.col("avg_sim") >= config.SIM_GROUNDING)
    )
    assert filtered.count() == 1


def test_mixed_terms_only_passing_ones_retained(spark):
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    schema = StructType([
        StructField("noun_chunk", StringType()),
        StructField("relative_share", DoubleType()),
        StructField("global_count", LongType()),
        StructField("avg_sim", DoubleType()),
    ])
    data = [
        ("sql server", 15.0, 600, 0.72),     # passes all
        ("spreadsheet", 5.0, 800, 0.65),      # fails relative_share
        ("rare db term", 20.0, 2, 0.70),      # fails global_count
        ("generic skill", 11.0, 550, 0.20),   # fails avg_sim
    ]
    df = spark.createDataFrame(data, schema)
    filtered = df.filter(
        (F.col("relative_share") >= config.REL_SHARE_THRESHOLD) &
        (F.col("global_count") >= 500) &
        (F.col("avg_sim") >= config.SIM_GROUNDING)
    )
    assert filtered.count() == 1
    assert filtered.first()["noun_chunk"] == "sql server"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_single_job_in_economy(spark):
    import pyspark.sql.functions as F
    rows = [("sql", "job_001", "3133")]
    result = _compute_relative_share(spark, rows, ["3133"])
    row = result.filter(F.col("noun_chunk") == "sql").first()
    # share_economy = 1/1 = 1.0, share_anchor = 1/1 = 1.0 → ratio = 1.0
    assert row is not None
    assert row["relative_share"] >= 0


def test_no_anchor_jobs_does_not_crash(spark):
    import pyspark.sql.functions as F
    rows = [("sql", "job_001", "2425"), ("excel", "job_002", "2425")]
    # Anchor SOC "3133" is not in the data
    result = _compute_relative_share(spark, rows, ["3133"])
    # Should return rows with relative_share = 0
    for row in result.collect():
        assert row["relative_share"] == 0.0
