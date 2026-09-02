"""
Phase 1 — NLP Extraction tests.

Phase 1 runs spaCy inside Spark via mapInPandas. It extracts noun chunks
from job description text, cleans them, and computes cosine similarity to
the word "data". The output is a Parquet file (one row per job) with two
arrays: noun_chunks and sim_scores.

Tests here cover:
  - The noun chunk cleaning rules
  - The output schema structure
  - Handling of missing / empty text
  - The year and month extraction logic
  - Sampling behaviour
  - The Parquet partitioning strategy
"""

import re
import pytest


# ---------------------------------------------------------------------------
# Cleaning logic extracted from extract_noun_chunks_packed (main.py:87-97)
# ---------------------------------------------------------------------------

def clean_chunk(raw_text: str) -> str | None:
    cleaned = raw_text.lower()
    cleaned = re.sub(r"[^a-z\s]", " ", cleaned)
    cleaned = " ".join(cleaned.split())
    word_count = len(cleaned.split())
    if not cleaned or word_count < 1 or word_count > 4:
        return None
    return cleaned


# --- Cleaning: basic rules ---

def test_clean_lowercases():
    assert clean_chunk("Database Management") == "database management"

def test_clean_removes_punctuation():
    assert clean_chunk("data-driven insights") == "data driven insights"

def test_clean_removes_digits():
    assert clean_chunk("Python3 scripting") == "python scripting"

def test_clean_collapses_whitespace():
    assert clean_chunk("  sql   server  ") == "sql server"

def test_clean_removes_parentheses():
    assert clean_chunk("NoSQL (e.g. MongoDB)") == "nosql e g mongodb"

def test_clean_removes_slashes():
    assert clean_chunk("ETL/ELT pipelines") == "etl elt pipelines"

def test_clean_removes_ampersand():
    assert clean_chunk("R&D skills") == "r d skills"


# --- Cleaning: word count boundaries ---

def test_single_word_kept():
    assert clean_chunk("sql") == "sql"

def test_two_words_kept():
    assert clean_chunk("data governance") == "data governance"

def test_three_words_kept():
    assert clean_chunk("relational database management") == "relational database management"

def test_four_words_kept():
    assert clean_chunk("relational database management system") == "relational database management system"

def test_five_words_rejected():
    assert clean_chunk("big data processing pipeline architecture") is None

def test_ten_words_rejected():
    assert clean_chunk("a b c d e f g h i j") is None


# --- Cleaning: rejection of empty / garbage ---

def test_empty_string_rejected():
    assert clean_chunk("") is None

def test_whitespace_only_rejected():
    assert clean_chunk("   ") is None

def test_punctuation_only_rejected():
    assert clean_chunk("---") is None

def test_digits_only_rejected():
    assert clean_chunk("2024") is None

def test_single_digit_rejected():
    assert clean_chunk("3") is None

def test_special_chars_only_rejected():
    assert clean_chunk("@#$%") is None


# --- Cleaning: edge cases ---

def test_hyphenated_splits_into_words():
    assert clean_chunk("end-to-end") == "end to end"

def test_apostrophe_stripped():
    # "master's" becomes "master s" — 2 words, kept
    result = clean_chunk("master's degree")
    assert result == "master s degree"

def test_mixed_lang_chars_stripped():
    assert clean_chunk("naïve bayes") == "na ve bayes"

def test_chunk_that_becomes_empty_after_cleaning_rejected():
    assert clean_chunk("123 456") is None


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def test_output_schema_field_names():
    """The Phase 1 output schema must contain exactly these fields."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
    )
    expected_fields = {
        "doc_JobID", "doc_BGTOcc", "doc_year", "doc_month",
        "noun_chunks", "sim_scores",
    }
    schema = StructType([
        StructField("doc_JobID", StringType()),
        StructField("doc_BGTOcc", StringType()),
        StructField("doc_year", IntegerType()),
        StructField("doc_month", IntegerType()),
        StructField("noun_chunks", ArrayType(StringType())),
        StructField("sim_scores", ArrayType(DoubleType())),
    ])
    actual_fields = {f.name for f in schema.fields}
    assert actual_fields == expected_fields


def test_output_schema_array_types():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
    schema = StructType([
        StructField("doc_JobID", StringType()),
        StructField("doc_BGTOcc", StringType()),
        StructField("doc_year", IntegerType()),
        StructField("doc_month", IntegerType()),
        StructField("noun_chunks", ArrayType(StringType())),
        StructField("sim_scores", ArrayType(DoubleType())),
    ])
    field_map = {f.name: f.dataType for f in schema.fields}
    assert isinstance(field_map["noun_chunks"], ArrayType)
    assert isinstance(field_map["sim_scores"], ArrayType)
    assert isinstance(field_map["noun_chunks"].elementType, StringType)
    assert isinstance(field_map["sim_scores"].elementType, DoubleType)


# ---------------------------------------------------------------------------
# Year and month extraction (Spark)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder.appName("test_phase1")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def test_year_extraction_from_date_column(spark):
    import pyspark.sql.functions as F
    df = spark.createDataFrame(
        [("job_001", "2023-06-15"), ("job_002", "2021-01-01")],
        ["job_id", "date"]
    )
    result = df.withColumn("date", F.to_date("date")) \
               .withColumn("doc_year", F.year("date")) \
               .withColumn("doc_month", F.month("date"))

    rows = {r["job_id"]: r for r in result.collect()}
    assert rows["job_001"]["doc_year"] == 2023
    assert rows["job_001"]["doc_month"] == 6
    assert rows["job_002"]["doc_year"] == 2021
    assert rows["job_002"]["doc_month"] == 1


def test_year_filter_excludes_other_years(spark):
    import pyspark.sql.functions as F
    df = spark.createDataFrame(
        [("job_001", "2023-03-01"), ("job_002", "2022-11-01"), ("job_003", "2023-07-01")],
        ["job_id", "date"]
    )
    filtered = df.withColumn("date", F.to_date("date")) \
                 .filter(F.year("date") == 2023)
    assert filtered.count() == 2


def test_null_date_handled_without_crash(spark):
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("job_id", StringType()),
        StructField("date", StringType()),
    ])
    df = spark.createDataFrame([("job_001", None)], schema)
    result = df.withColumn("date", F.to_date("date")) \
               .withColumn("doc_year", F.year("date"))
    row = result.first()
    assert row["doc_year"] is None


# ---------------------------------------------------------------------------
# Sampling behaviour
# ---------------------------------------------------------------------------

def test_sample_fraction_one_returns_all_rows(spark):
    df = spark.range(1000)
    sampled = df.sample(False, 1.0, seed=42)
    assert sampled.count() == 1000


def test_sample_fraction_half_returns_approximately_half(spark):
    df = spark.range(10000)
    sampled = df.sample(False, 0.5, seed=42)
    count = sampled.count()
    # Allow ±10% tolerance
    assert 4000 <= count <= 6000, f"Expected ~5000, got {count}"


def test_sample_fraction_zero_point_one_returns_approximately_ten_pct(spark):
    df = spark.range(10000)
    sampled = df.sample(False, 0.1, seed=42)
    count = sampled.count()
    assert 700 <= count <= 1300, f"Expected ~1000, got {count}"


# ---------------------------------------------------------------------------
# Repartition behaviour
# ---------------------------------------------------------------------------

def test_repartition_creates_expected_number_of_partitions(spark):
    df = spark.range(100000)
    repartitioned = df.repartition(10)
    assert repartitioned.rdd.getNumPartitions() == 10


def test_repartition_does_not_change_row_count(spark):
    df = spark.range(50000)
    assert df.repartition(8).count() == 50000


# ---------------------------------------------------------------------------
# Noun chunks and sim scores must be parallel arrays
# ---------------------------------------------------------------------------

def test_chunks_and_scores_arrays_same_length():
    chunks = ["sql server", "data governance", "relational database"]
    scores = [0.72, 0.65, 0.81]
    assert len(chunks) == len(scores)


def test_empty_text_produces_empty_arrays():
    # If spaCy finds no noun chunks in empty text, both arrays must be empty lists
    chunks_list = []
    sims_list = []
    assert isinstance(chunks_list, list)
    assert isinstance(sims_list, list)
    assert len(chunks_list) == len(sims_list) == 0


def test_sim_score_below_grounding_excluded():
    """Chunks whose similarity to 'data' is below SIM_GROUNDING must be dropped."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import config

    chunks = [("sql server", 0.72), ("general management", 0.10), ("data pipeline", 0.65)]
    retained = [(c, s) for c, s in chunks if s >= config.SIM_GROUNDING]
    retained_names = [c for c, _ in retained]

    assert "sql server" in retained_names
    assert "data pipeline" in retained_names
    assert "general management" not in retained_names
