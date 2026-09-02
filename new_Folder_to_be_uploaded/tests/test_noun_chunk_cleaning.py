"""
Tests for the noun chunk cleaning logic from Phase 1.

The cleaning rules are embedded inside extract_noun_chunks_packed in main.py.
This file extracts that logic into a standalone function so it can be tested
independently of Spark and spaCy.
"""

import re


def clean_chunk(raw_text: str) -> str | None:
    """
    Mirror of the cleaning logic in extract_noun_chunks_packed.
    Returns the cleaned chunk, or None if it should be discarded.
    """
    cleaned = raw_text.lower()
    cleaned = re.sub(r"[^a-z\s]", " ", cleaned)
    cleaned = " ".join(cleaned.split())
    word_count = len(cleaned.split())
    if not cleaned or word_count < 1 or word_count > 4:
        return None
    return cleaned


# --- Basic cleaning ---

def test_lowercases_input():
    assert clean_chunk("SQL Server") == "sql server"


def test_strips_punctuation():
    assert clean_chunk("data-driven") == "data driven"


def test_strips_digits():
    assert clean_chunk("python3") == "python"


def test_collapses_internal_whitespace():
    result = clean_chunk("database   management")
    assert result == "database management"


def test_strips_leading_and_trailing_whitespace():
    assert clean_chunk("  sql  ") == "sql"


# --- Word count boundary conditions ---

def test_single_word_is_kept():
    assert clean_chunk("database") == "database"


def test_four_word_chunk_is_kept():
    assert clean_chunk("relational database management system") == "relational database management system"


def test_five_word_chunk_is_rejected():
    assert clean_chunk("relational database management systems design") is None


def test_empty_string_is_rejected():
    assert clean_chunk("") is None


def test_punctuation_only_is_rejected():
    # After stripping punctuation this becomes empty
    assert clean_chunk("---") is None


def test_chunk_that_becomes_too_long_after_cleaning_is_rejected():
    # Numbers get stripped, leaving 5 words
    result = clean_chunk("big 1 data 2 science 3 platform 4 architecture")
    # After stripping digits: "big data science platform architecture" = 5 words
    assert result is None


# --- Edge cases ---

def test_mixed_case_with_symbols():
    assert clean_chunk("NoSQL (Database)") == "nosql database"


def test_apostrophe_stripped():
    assert clean_chunk("master's degree") == "master s degree"


def test_hyphenated_compound_becomes_two_words():
    result = clean_chunk("end-to-end")
    assert result == "end to end"
