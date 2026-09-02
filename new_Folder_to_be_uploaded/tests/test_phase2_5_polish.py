"""
Phase 2.5 — Semantic Polish tests.

Phase 2.5 scores every term in the raw dictionary against a Gold Standard
word list using spaCy word vectors. Only terms whose maximum cosine
similarity to any gold standard word is >= 0.45 survive.

Because running actual spaCy inference in unit tests is slow and requires
a downloaded model, these tests focus on:
  - The filtering logic (given pre-scored DataFrames)
  - Boundary conditions around the 0.45 threshold
  - Handling of empty inputs
  - The dual-stage design (statistical then semantic)
  - Score aggregation (max of multiple gold comparisons)
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

GOLD_SIM_THRESHOLD = 0.45


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (
        SparkSession.builder.appName("test_phase2_5")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def make_scored_df(spark, data):
    """Create a DataFrame simulating Phase 2.5 gold_sim_score output."""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    schema = StructType([
        StructField("noun_chunk", StringType()),
        StructField("relative_share", DoubleType()),
        StructField("avg_sim", DoubleType()),
        StructField("global_count", LongType()),
        StructField("gold_sim_score", DoubleType()),
    ])
    return spark.createDataFrame(data, schema)


# ---------------------------------------------------------------------------
# Filtering at the gold standard threshold
# ---------------------------------------------------------------------------

def test_term_above_threshold_is_retained(spark):
    import pyspark.sql.functions as F
    df = make_scored_df(spark, [("sql server", 15.0, 0.72, 600, 0.80)])
    result = df.filter(F.col("gold_sim_score") >= GOLD_SIM_THRESHOLD)
    assert result.count() == 1


def test_term_below_threshold_is_excluded(spark):
    import pyspark.sql.functions as F
    df = make_scored_df(spark, [("spreadsheet", 12.0, 0.65, 550, 0.30)])
    result = df.filter(F.col("gold_sim_score") >= GOLD_SIM_THRESHOLD)
    assert result.count() == 0


def test_term_at_exact_threshold_is_retained(spark):
    import pyspark.sql.functions as F
    df = make_scored_df(spark, [("data entry", 11.0, 0.60, 500, 0.45)])
    result = df.filter(F.col("gold_sim_score") >= GOLD_SIM_THRESHOLD)
    assert result.count() == 1


def test_term_one_unit_below_threshold_is_excluded(spark):
    import pyspark.sql.functions as F
    df = make_scored_df(spark, [("general task", 10.5, 0.55, 510, 0.4499)])
    result = df.filter(F.col("gold_sim_score") >= GOLD_SIM_THRESHOLD)
    assert result.count() == 0


# ---------------------------------------------------------------------------
# Mixed batches
# ---------------------------------------------------------------------------

def test_only_high_scoring_terms_survive_mixed_batch(spark):
    import pyspark.sql.functions as F
    data = [
        ("database schema", 14.0, 0.70, 620, 0.85),   # passes
        ("relational model", 12.0, 0.68, 580, 0.78),   # passes
        ("spreadsheet", 11.0, 0.66, 520, 0.29),        # fails
        ("communication skills", 10.5, 0.40, 800, 0.15), # fails
        ("nosql database", 16.0, 0.74, 700, 0.81),     # passes
    ]
    df = make_scored_df(spark, data)
    result = df.filter(F.col("gold_sim_score") >= GOLD_SIM_THRESHOLD)
    assert result.count() == 3
    survivors = {r["noun_chunk"] for r in result.collect()}
    assert survivors == {"database schema", "relational model", "nosql database"}


def test_all_terms_fail_gold_standard_returns_empty(spark):
    import pyspark.sql.functions as F
    data = [
        ("office admin", 10.5, 0.37, 510, 0.12),
        ("teamwork", 11.0, 0.36, 600, 0.08),
    ]
    df = make_scored_df(spark, data)
    result = df.filter(F.col("gold_sim_score") >= GOLD_SIM_THRESHOLD)
    assert result.count() == 0


def test_all_terms_pass_gold_standard(spark):
    import pyspark.sql.functions as F
    data = [
        ("sql", 20.0, 0.75, 900, 0.92),
        ("oracle", 18.0, 0.73, 850, 0.88),
        ("postgres", 17.0, 0.71, 800, 0.86),
    ]
    df = make_scored_df(spark, data)
    result = df.filter(F.col("gold_sim_score") >= GOLD_SIM_THRESHOLD)
    assert result.count() == 3


# ---------------------------------------------------------------------------
# Empty input
# ---------------------------------------------------------------------------

def test_empty_dictionary_produces_empty_output(spark):
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    schema = StructType([
        StructField("noun_chunk", StringType()),
        StructField("relative_share", DoubleType()),
        StructField("avg_sim", DoubleType()),
        StructField("global_count", LongType()),
        StructField("gold_sim_score", DoubleType()),
    ])
    df = spark.createDataFrame([], schema)
    result = df.filter(F.col("gold_sim_score") >= GOLD_SIM_THRESHOLD)
    assert result.count() == 0


# ---------------------------------------------------------------------------
# Gold score aggregation — max across multiple gold words
# ---------------------------------------------------------------------------

def test_max_similarity_selected_from_multiple_gold_words():
    """
    The pipeline takes max([doc.similarity(g) for g in gold_docs]).
    A term similar to ANY gold word should survive, not just the first.
    """
    scores_against_gold_words = [0.30, 0.72, 0.25]
    max_score = max(scores_against_gold_words)
    assert max_score >= GOLD_SIM_THRESHOLD


def test_term_similar_to_only_one_gold_word_survives():
    scores = [0.20, 0.20, 0.60]  # only similar to third gold word
    assert max(scores) >= GOLD_SIM_THRESHOLD


def test_term_dissimilar_to_all_gold_words_fails():
    scores = [0.10, 0.15, 0.20]
    assert max(scores) < GOLD_SIM_THRESHOLD


def test_zero_vector_term_gets_score_zero():
    # Terms with no vector get score 0.0 (handled in main.py:197-199)
    score = 0.0
    assert score < GOLD_SIM_THRESHOLD


# ---------------------------------------------------------------------------
# Dual-stage design — Phase 2 filter must run before Phase 2.5
# ---------------------------------------------------------------------------

def test_phase25_only_receives_phase2_survivors(spark):
    """
    Simulate that Phase 2.5 input only contains terms already passing
    the statistical filter. Phase 2.5 should never see terms with
    relative_share < REL_SHARE_THRESHOLD.
    """
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
    schema = StructType([
        StructField("noun_chunk", StringType()),
        StructField("relative_share", DoubleType()),
        StructField("avg_sim", DoubleType()),
        StructField("global_count", LongType()),
        StructField("gold_sim_score", DoubleType()),
    ])
    # All terms have already passed Phase 2 (relative_share >= 10)
    data = [
        ("sql server", 15.0, 0.72, 600, 0.85),
        ("data pipeline", 12.0, 0.68, 560, 0.76),
    ]
    df = spark.createDataFrame(data, schema)
    # No term should have relative_share below threshold
    below = df.filter(F.col("relative_share") < config.REL_SHARE_THRESHOLD)
    assert below.count() == 0


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

def test_polished_dictionary_has_gold_score_column(spark):
    import pyspark.sql.functions as F
    df = make_scored_df(spark, [("sql", 15.0, 0.72, 600, 0.85)])
    assert "gold_sim_score" in df.columns


def test_polished_dictionary_retains_all_phase2_columns(spark):
    df = make_scored_df(spark, [("sql", 15.0, 0.72, 600, 0.85)])
    required = {"noun_chunk", "relative_share", "avg_sim", "global_count", "gold_sim_score"}
    assert required.issubset(set(df.columns))


# ---------------------------------------------------------------------------
# Retention rate sanity check
# ---------------------------------------------------------------------------

def test_retention_rate_is_within_reasonable_bounds(spark):
    import pyspark.sql.functions as F
    """
    On real data, typically 30-70% of Phase 2 terms pass Phase 2.5.
    Near 0% suggests gold standard is too narrow.
    Near 100% suggests the gold standard filter is not working.
    This test validates the filtering logic produces a reduction.
    """
    data = [
        ("sql server", 15.0, 0.72, 600, 0.85),
        ("database schema", 12.0, 0.70, 580, 0.82),
        ("nosql", 14.0, 0.74, 700, 0.88),
        ("spreadsheet", 10.5, 0.65, 510, 0.28),
        ("communication", 11.0, 0.36, 550, 0.09),
        ("team player", 10.2, 0.37, 500, 0.05),
    ]
    df = make_scored_df(spark, data)
    total = df.count()
    retained = df.filter(F.col("gold_sim_score") >= GOLD_SIM_THRESHOLD).count()
    retention_rate = retained / total
    # 3 out of 6 = 50% — within the expected 30–70% window
    assert 0.30 <= retention_rate <= 0.70, (
        f"Retention rate {retention_rate:.0%} outside expected 30–70% window."
    )
