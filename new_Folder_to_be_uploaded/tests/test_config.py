"""
Tests for config.py invariants.

These run without Spark and verify that the configuration is internally
consistent before the pipeline is executed. If any of these fail, the
pipeline will either crash or produce wrong numbers silently.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def test_anchor_socs_are_subset_of_soc_groups():
    """Every code in ALL_ANCHOR_SOCS must exist in SOC_GROUPS.
    If not, the anchor frequency denominator is defined over a universe
    that doesn't match the codes being targeted, corrupting relative share."""
    all_defined_codes = {code for codes in config.SOC_GROUPS.values() for code in codes}
    missing = set(config.ALL_ANCHOR_SOCS) - all_defined_codes
    assert not missing, (
        f"ALL_ANCHOR_SOCS contains codes not defined in SOC_GROUPS: {missing}. "
        "Copy every SOC code from SOC_GROUPS into ALL_ANCHOR_SOCS."
    )


def test_soc_groups_not_empty():
    assert config.SOC_GROUPS, "SOC_GROUPS must contain at least one domain."
    for domain, codes in config.SOC_GROUPS.items():
        assert codes, f"SOC_GROUPS['{domain}'] has no SOC codes."


def test_gold_standard_not_empty():
    assert config.GOLD_STANDARD, "GOLD_STANDARD must contain at least one term."


def test_years_are_integers_in_valid_range():
    assert config.YEARS, "YEARS list is empty."
    for y in config.YEARS:
        assert isinstance(y, int), f"Year {y!r} is not an integer."
        assert 2000 <= y <= 2030, f"Year {y} looks wrong — expected between 2000 and 2030."


def test_thresholds_are_positive():
    assert config.REL_SHARE_THRESHOLD > 0, "REL_SHARE_THRESHOLD must be positive."
    assert config.SIM_GROUNDING > 0, "SIM_GROUNDING must be positive."
    assert config.DATA_THRESHOLD > 0, "DATA_THRESHOLD must be a positive integer."


def test_sim_grounding_is_valid_cosine_range():
    assert 0 < config.SIM_GROUNDING <= 1, (
        f"SIM_GROUNDING={config.SIM_GROUNDING} is outside (0, 1]. "
        "Cosine similarity is bounded to [-1, 1], meaningful range is (0, 1]."
    )


def test_alpha_economy_avg_is_positive():
    assert config.ALPHA_ECONOMY_AVG > 0
    assert config.ALPHA_LOW > 0
    assert config.ALPHA_LOW <= config.ALPHA_ECONOMY_AVG, (
        "ALPHA_LOW should be the conservative lower bound and must not exceed ALPHA_ECONOMY_AVG."
    )


def test_alpha_map_covers_all_expected_sic_codes():
    expected_sic_codes = {"A", "B-E", "F", "G-I", "J", "K", "L", "M-N", "O-Q", "R-T", "U"}
    missing = expected_sic_codes - set(config.ALPHA_MAP.keys())
    extra = set(config.ALPHA_MAP.keys()) - expected_sic_codes
    assert not missing, f"ALPHA_MAP is missing SIC codes: {missing}"
    assert not extra, f"ALPHA_MAP has unexpected SIC codes: {extra}"


def test_alpha_map_values_are_positive():
    for sic, alpha in config.ALPHA_MAP.items():
        assert alpha > 0, f"Alpha for sector {sic} must be positive, got {alpha}."


def test_sample_fraction_in_valid_range():
    assert 0 < config.SAMPLE_FRACTION <= 1.0, (
        f"SAMPLE_FRACTION={config.SAMPLE_FRACTION} must be in (0, 1]."
    )
