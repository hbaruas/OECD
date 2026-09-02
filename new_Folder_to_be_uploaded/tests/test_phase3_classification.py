"""
Phase 3 — Job Classification tests.

Phase 3 joins every job's noun chunks against the polished vocabulary.
Each job is scored by how many *distinct* vocabulary terms it contains.
If that count >= DATA_THRESHOLD, the job is classified as data-intensive.

Key behaviours tested:
  - DATA_THRESHOLD boundary (at, above, below)
  - Broadcast join correctness
  - is_anchor vs any_data_intensive distinction
  - Left join preserves zero-match jobs
  - SOC code extraction from raw soc_2020 strings
  - Year-by-year accumulation
  - Distinct term counting (not raw match count)
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
        SparkSession.builder.appName("test_phase3")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def make_vocab(spark, terms):
    """Minimal vocabulary DataFrame with just the noun_chunk column."""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    schema = StructType([
        StructField("noun_chunk", StringType()),
        StructField("relative_share", DoubleType()),
        StructField("global_count", LongType()),
    ])
    data = [(t, 15.0, 600) for t in terms]
    return spark.createDataFrame(data, schema)


# ---------------------------------------------------------------------------
# DATA_THRESHOLD boundary conditions
# ---------------------------------------------------------------------------

def test_job_exactly_at_threshold_is_intensive(spark):
    import pyspark.sql.functions as F
    threshold = config.DATA_THRESHOLD
    vocab = make_vocab(spark, [f"term_{i}" for i in range(threshold)])
    chunks = spark.createDataFrame(
        [("job_001", f"term_{i}") for i in range(threshold)],
        ["doc_JobID", "noun_chunk"]
    )
    joined = chunks.join(F.broadcast(vocab), "noun_chunk", "inner")
    scored = joined.groupBy("doc_JobID").agg(
        F.countDistinct("noun_chunk").alias("unique_data_terms")
    ).withColumn("is_intensive", (F.col("unique_data_terms") >= threshold).cast("int"))
    assert scored.first()["is_intensive"] == 1


def test_job_one_below_threshold_is_not_intensive(spark):
    import pyspark.sql.functions as F
    threshold = config.DATA_THRESHOLD
    vocab = make_vocab(spark, [f"term_{i}" for i in range(threshold)])
    chunks = spark.createDataFrame(
        [("job_002", f"term_{i}") for i in range(threshold - 1)],
        ["doc_JobID", "noun_chunk"]
    )
    joined = chunks.join(F.broadcast(vocab), "noun_chunk", "inner")
    scored = joined.groupBy("doc_JobID").agg(
        F.countDistinct("noun_chunk").alias("unique_data_terms")
    ).withColumn("is_intensive", (F.col("unique_data_terms") >= threshold).cast("int"))
    assert scored.first()["is_intensive"] == 0


def test_job_one_above_threshold_is_intensive(spark):
    import pyspark.sql.functions as F
    threshold = config.DATA_THRESHOLD
    n = threshold + 1
    vocab = make_vocab(spark, [f"term_{i}" for i in range(n)])
    chunks = spark.createDataFrame(
        [("job_003", f"term_{i}") for i in range(n)],
        ["doc_JobID", "noun_chunk"]
    )
    joined = chunks.join(F.broadcast(vocab), "noun_chunk", "inner")
    scored = joined.groupBy("doc_JobID").agg(
        F.countDistinct("noun_chunk").alias("unique_data_terms")
    ).withColumn("is_intensive", (F.col("unique_data_terms") >= threshold).cast("int"))
    assert scored.first()["is_intensive"] == 1


def test_job_with_zero_matching_terms_is_not_intensive(spark):
    import pyspark.sql.functions as F
    vocab = make_vocab(spark, ["sql", "oracle", "nosql"])
    chunks = spark.createDataFrame(
        [("job_004", "teamwork"), ("job_004", "communication")],
        ["doc_JobID", "noun_chunk"]
    )
    joined = chunks.join(F.broadcast(vocab), "noun_chunk", "inner")
    assert joined.count() == 0


# ---------------------------------------------------------------------------
# Distinct term counting (not raw match count)
# ---------------------------------------------------------------------------

def test_repeated_terms_count_once(spark):
    import pyspark.sql.functions as F
    threshold = config.DATA_THRESHOLD
    vocab = make_vocab(spark, ["sql"])
    # "sql" appears 10 times in one job — should still count as 1 unique term
    chunks = spark.createDataFrame(
        [("job_005", "sql")] * 10,
        ["doc_JobID", "noun_chunk"]
    )
    joined = chunks.join(F.broadcast(vocab), "noun_chunk", "inner")
    scored = joined.groupBy("doc_JobID").agg(
        F.countDistinct("noun_chunk").alias("unique_data_terms")
    )
    assert scored.first()["unique_data_terms"] == 1


def test_distinct_terms_across_multiple_matches(spark):
    import pyspark.sql.functions as F
    vocab = make_vocab(spark, ["sql", "oracle", "nosql"])
    # Each term appears twice but there are 3 distinct terms
    chunks = spark.createDataFrame([
        ("job_006", "sql"), ("job_006", "sql"),
        ("job_006", "oracle"), ("job_006", "oracle"),
        ("job_006", "nosql"),
    ], ["doc_JobID", "noun_chunk"])
    joined = chunks.join(F.broadcast(vocab), "noun_chunk", "inner")
    scored = joined.groupBy("doc_JobID").agg(
        F.countDistinct("noun_chunk").alias("unique_data_terms")
    )
    assert scored.first()["unique_data_terms"] == 3


# ---------------------------------------------------------------------------
# Left join preserves zero-match jobs
# ---------------------------------------------------------------------------

def test_zero_match_jobs_appear_in_output_with_zero_flag(spark):
    import pyspark.sql.functions as F
    all_jobs = spark.createDataFrame(
        [("job_001", "3133"), ("job_002", "2425"), ("job_003", "3133")],
        ["doc_JobID", "soc4"]
    )
    classified = spark.createDataFrame(
        [("job_001", "3133", 1)],
        ["doc_JobID", "soc4", "is_intensive"]
    )
    merged = all_jobs.join(classified, ["doc_JobID", "soc4"], "left").fillna(0)
    assert merged.count() == 3
    unmatched = merged.filter(F.col("doc_JobID") == "job_002").first()
    assert unmatched["is_intensive"] == 0


def test_inner_join_would_drop_zero_match_jobs(spark):
    """Demonstrates why left join is essential — inner join loses zero-match jobs."""
    import pyspark.sql.functions as F
    all_jobs = spark.createDataFrame(
        [("job_001", "3133"), ("job_002", "2425")],
        ["doc_JobID", "soc4"]
    )
    classified = spark.createDataFrame(
        [("job_001", "3133", 1)],
        ["doc_JobID", "soc4", "is_intensive"]
    )
    inner = all_jobs.join(classified, ["doc_JobID", "soc4"], "inner")
    left = all_jobs.join(classified, ["doc_JobID", "soc4"], "left")
    assert inner.count() == 1  # job_002 dropped
    assert left.count() == 2   # job_002 preserved


def test_fillna_zero_fills_unmatched_rows(spark):
    import pyspark.sql.functions as F
    all_jobs = spark.createDataFrame(
        [("job_001",), ("job_002",)],
        ["doc_JobID"]
    )
    classified = spark.createDataFrame(
        [("job_001", 1)],
        ["doc_JobID", "is_intensive"]
    )
    merged = all_jobs.join(classified, "doc_JobID", "left").fillna(0)
    job_002 = merged.filter(F.col("doc_JobID") == "job_002").first()
    assert job_002["is_intensive"] == 0


# ---------------------------------------------------------------------------
# is_anchor vs any_data_intensive distinction
# ---------------------------------------------------------------------------

def test_anchor_job_requires_correct_soc_and_intensity(spark):
    import pyspark.sql.functions as F
    anchor_socs = ["3133"]
    threshold = config.DATA_THRESHOLD
    rows = [
        ("job_001", "3133", threshold),      # correct SOC, data-intensive → is_anchor=1
        ("job_002", "2425", threshold),      # wrong SOC, data-intensive → is_anchor=0
        ("job_003", "3133", threshold - 1),  # correct SOC, not intensive → is_anchor=0
    ]
    df = spark.createDataFrame(rows, ["doc_JobID", "soc4", "unique_data_terms"])
    is_intensive = F.col("unique_data_terms") >= threshold
    result = df.withColumn(
        "is_anchor", (F.col("soc4").isin(anchor_socs) & is_intensive).cast("int")
    ).withColumn("any_data_intensive", is_intensive.cast("int"))

    rows_out = {r["doc_JobID"]: r for r in result.collect()}
    assert rows_out["job_001"]["is_anchor"] == 1
    assert rows_out["job_001"]["any_data_intensive"] == 1
    assert rows_out["job_002"]["is_anchor"] == 0
    assert rows_out["job_002"]["any_data_intensive"] == 1
    assert rows_out["job_003"]["is_anchor"] == 0
    assert rows_out["job_003"]["any_data_intensive"] == 0


def test_any_data_intensive_ignores_soc_code(spark):
    import pyspark.sql.functions as F
    threshold = config.DATA_THRESHOLD
    rows = [("job_001", "9999", threshold)]  # SOC not in any anchor list
    df = spark.createDataFrame(rows, ["doc_JobID", "soc4", "unique_data_terms"])
    is_intensive = F.col("unique_data_terms") >= threshold
    result = df.withColumn("any_data_intensive", is_intensive.cast("int"))
    assert result.first()["any_data_intensive"] == 1


# ---------------------------------------------------------------------------
# SOC code extraction from raw soc_2020 strings
# ---------------------------------------------------------------------------

def test_soc4_extracted_from_formatted_label(spark):
    import pyspark.sql.functions as F
    df = spark.createDataFrame(
        [("job_001", "2433 - Data analysts")],
        ["doc_JobID", "soc_2020"]
    )
    result = df.withColumn("soc4", F.regexp_extract("soc_2020", r"(\d{4})", 1))
    assert result.first()["soc4"] == "2433"


def test_empty_soc_string_produces_empty_soc4(spark):
    import pyspark.sql.functions as F
    df = spark.createDataFrame([("job_001", "")], ["doc_JobID", "soc_2020"])
    result = df.withColumn("soc4", F.regexp_extract("soc_2020", r"(\d{4})", 1))
    assert result.first()["soc4"] == ""


def test_jobs_with_empty_soc4_are_filtered_out(spark):
    import pyspark.sql.functions as F
    df = spark.createDataFrame([
        ("job_001", "3133"),
        ("job_002", ""),
    ], ["doc_JobID", "soc4"])
    filtered = df.filter(F.col("soc4") != "")
    assert filtered.count() == 1


# ---------------------------------------------------------------------------
# Year-by-year accumulation
# ---------------------------------------------------------------------------

def test_results_from_multiple_years_union_correctly(spark):
    import pyspark.sql.functions as F
    year_2023 = spark.createDataFrame(
        [("job_001", "3133", 1, 2023)],
        ["doc_JobID", "soc4", "is_intensive", "year"]
    )
    year_2024 = spark.createDataFrame(
        [("job_002", "3133", 1, 2024), ("job_003", "2425", 0, 2024)],
        ["doc_JobID", "soc4", "is_intensive", "year"]
    )
    combined = year_2023.unionByName(year_2024)
    assert combined.count() == 3
    assert combined.filter(F.col("year") == 2023).count() == 1
    assert combined.filter(F.col("year") == 2024).count() == 2


def test_union_by_name_handles_different_column_order(spark):
    import pyspark.sql.functions as F
    df1 = spark.createDataFrame([("job_001", 2023)], ["doc_JobID", "year"])
    df2 = spark.createDataFrame([(2024, "job_002")], ["year", "doc_JobID"])
    combined = df1.unionByName(df2)
    assert combined.count() == 2


# ---------------------------------------------------------------------------
# Occupation-level aggregation
# ---------------------------------------------------------------------------

def test_total_jobs_counted_correctly_per_soc(spark):
    import pyspark.sql.functions as F
    rows = [
        ("job_001", "3133", 1),
        ("job_002", "3133", 0),
        ("job_003", "3133", 1),
        ("job_004", "2425", 1),
    ]
    df = spark.createDataFrame(rows, ["doc_JobID", "soc4", "any_data_intensive"])
    result = df.groupBy("soc4").agg(
        F.count("*").alias("total_jobs"),
        F.sum("any_data_intensive").alias("intensive_jobs"),
    ).withColumn("total_data_share", 100 * F.col("intensive_jobs") / F.col("total_jobs"))

    rows_out = {r["soc4"]: r for r in result.collect()}
    assert rows_out["3133"]["total_jobs"] == 3
    assert rows_out["3133"]["intensive_jobs"] == 2
    assert abs(rows_out["3133"]["total_data_share"] - 66.6667) < 0.001
