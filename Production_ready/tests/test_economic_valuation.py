"""
Tests for the economic valuation logic from Phase 5.

All three investment scenarios (sector-specific alpha, economy average,
conservative) and the GVA share calculation are tested here using
known inputs and hand-calculated expected outputs.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def compute_investment(alpha: float, comp_emp: float, data_share_pct: float) -> float:
    return alpha * comp_emp * (data_share_pct / 100)


def compute_gva_share(investment: float, gva: float) -> float:
    if gva <= 0:
        return 0.0
    return (investment / gva) * 100


# --- Investment formula ---

def test_sector_specific_alpha_investment():
    # Sector J (Information): alpha=2.97, COMP_EMP=£100bn, 5% data share
    result = compute_investment(2.97, 100_000_000_000, 5.0)
    assert abs(result - 14_850_000_000) < 1

def test_economy_average_alpha_investment():
    result = compute_investment(config.ALPHA_ECONOMY_AVG, 100_000_000_000, 5.0)
    assert abs(result - config.ALPHA_ECONOMY_AVG * 5_000_000_000) < 1

def test_conservative_alpha_investment():
    result = compute_investment(config.ALPHA_LOW, 100_000_000_000, 5.0)
    assert abs(result - config.ALPHA_LOW * 5_000_000_000) < 1

def test_raw_wages_scenario():
    # Alpha=1.0 means investment equals the wage bill for data-intensive workers
    result = compute_investment(1.0, 100_000_000_000, 10.0)
    assert abs(result - 10_000_000_000) < 1

def test_conservative_less_than_economy_average():
    comp_emp = 200_000_000_000
    share = 8.0
    conservative = compute_investment(config.ALPHA_LOW, comp_emp, share)
    avg = compute_investment(config.ALPHA_ECONOMY_AVG, comp_emp, share)
    assert conservative < avg

def test_sector_specific_alpha_for_every_sector():
    # Every sector in the alpha map should produce a positive investment
    for sic, alpha in config.ALPHA_MAP.items():
        result = compute_investment(alpha, 50_000_000_000, 3.0)
        assert result > 0, f"Sector {sic} produced non-positive investment"


# --- GVA share ---

def test_gva_share_calculation():
    investment = 18_100_000_000
    gva = 150_000_000_000
    share = compute_gva_share(investment, gva)
    assert abs(share - 12.0667) < 0.001

def test_full_gva_data_share_gives_alpha_percent():
    # If 100% of jobs are data-intensive, investment/GVA ≈ alpha * COMP_EMP/GVA
    alpha = 3.62
    comp_emp = 100_000_000_000
    gva = 200_000_000_000
    investment = compute_investment(alpha, comp_emp, 100.0)
    share = compute_gva_share(investment, gva)
    expected = alpha * (comp_emp / gva) * 100
    assert abs(share - expected) < 0.001

def test_zero_gva_returns_zero_share():
    assert compute_gva_share(1_000_000, 0) == 0.0

def test_zero_data_share_gives_zero_investment():
    assert compute_investment(3.62, 100_000_000_000, 0.0) == 0.0

def test_investment_scales_linearly_with_data_share():
    base = compute_investment(3.62, 100_000_000_000, 5.0)
    double = compute_investment(3.62, 100_000_000_000, 10.0)
    assert abs(double - 2 * base) < 1

def test_investment_scales_linearly_with_comp_emp():
    base = compute_investment(3.62, 100_000_000_000, 5.0)
    double = compute_investment(3.62, 200_000_000_000, 5.0)
    assert abs(double - 2 * base) < 1
