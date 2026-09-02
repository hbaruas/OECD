"""
Tests for the SOC code extraction regex used throughout the pipeline.

The raw soc_2020 field contains strings like "2433 - Data analysts".
The pipeline extracts just the 4-digit numeric code using:

    F.regexp_extract("soc_2020", r"(\d{4})", 1)

This file tests that regex in Python to verify it handles the real-world
variation in how SOC codes appear in the data.
"""

import re


def extract_soc4(raw: str) -> str:
    """Mirror of the regexp_extract pattern used in main.py."""
    match = re.search(r"(\d{4})", raw)
    return match.group(1) if match else ""


# --- Standard formats ---

def test_standard_format_with_dash():
    assert extract_soc4("2433 - Data analysts") == "2433"

def test_standard_format_no_label():
    assert extract_soc4("3133") == "3133"

def test_code_with_trailing_text():
    assert extract_soc4("3544 Statistical researchers") == "3544"

def test_code_with_leading_spaces():
    assert extract_soc4("  2136 - Programmers and software development professionals") == "2136"


# --- Edge cases ---

def test_empty_string_returns_empty():
    assert extract_soc4("") == ""

def test_no_digits_returns_empty():
    assert extract_soc4("Data analyst") == ""

def test_three_digit_code_not_matched():
    # SOC codes are always 4 digits — a 3-digit string should not match
    assert extract_soc4("213") == ""

def test_five_digit_string_extracts_first_four():
    # If somehow a 5-digit string appears, the regex takes the first 4
    result = extract_soc4("21360")
    assert result == "2136"

def test_code_embedded_in_longer_string():
    assert extract_soc4("occupation_code=3133_v2") == "3133"


# --- All known anchor SOC codes parse correctly ---

def test_all_anchor_soc_codes_parse():
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import config

    all_codes = {code for codes in config.SOC_GROUPS.values() for code in codes}
    for code in all_codes:
        assert extract_soc4(code) == code, (
            f"SOC code '{code}' did not round-trip through the extraction regex."
        )
