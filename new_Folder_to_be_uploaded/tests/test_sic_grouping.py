"""
Tests for the SIC code grouping logic from Phase 4.

The CASE WHEN expression in main.py maps 2-digit SIC codes to
the letter-coded sector groups used in the dashboard and SUT table.
This file tests that mapping directly in Python so it can be verified
without spinning up a Spark session.
"""


def sic2_to_sector(sic2: int) -> str | None:
    """
    Mirror of the CASE WHEN expression in Phase 4 (main.py:263).
    Maps a 2-digit SIC code to its letter-coded sector group.
    Returns None for unrecognised codes (filtered out downstream).
    """
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


# --- Boundary tests: first and last code of each sector ---

def test_agriculture_lower_bound():
    assert sic2_to_sector(1) == "A"

def test_agriculture_upper_bound():
    assert sic2_to_sector(3) == "A"

def test_manufacturing_lower_bound():
    assert sic2_to_sector(5) == "B-E"

def test_manufacturing_upper_bound():
    assert sic2_to_sector(39) == "B-E"

def test_construction_lower_bound():
    assert sic2_to_sector(41) == "F"

def test_construction_upper_bound():
    assert sic2_to_sector(43) == "F"

def test_wholesale_retail_lower_bound():
    assert sic2_to_sector(45) == "G-I"

def test_wholesale_retail_upper_bound():
    assert sic2_to_sector(56) == "G-I"

def test_information_lower_bound():
    assert sic2_to_sector(58) == "J"

def test_information_upper_bound():
    assert sic2_to_sector(63) == "J"

def test_finance_lower_bound():
    assert sic2_to_sector(64) == "K"

def test_finance_upper_bound():
    assert sic2_to_sector(66) == "K"

def test_real_estate():
    assert sic2_to_sector(68) == "L"

def test_professional_lower_bound():
    assert sic2_to_sector(69) == "M-N"

def test_professional_upper_bound():
    assert sic2_to_sector(82) == "M-N"

def test_public_admin_lower_bound():
    assert sic2_to_sector(84) == "O-Q"

def test_public_admin_upper_bound():
    assert sic2_to_sector(88) == "O-Q"

def test_arts_lower_bound():
    assert sic2_to_sector(90) == "R-T"

def test_arts_upper_bound():
    assert sic2_to_sector(98) == "R-T"

def test_extraterritorial():
    assert sic2_to_sector(99) == "U"


# --- Gap codes that should return None ---

def test_gap_between_agriculture_and_manufacturing():
    assert sic2_to_sector(4) is None

def test_gap_between_manufacturing_and_construction():
    assert sic2_to_sector(40) is None

def test_gap_between_construction_and_wholesale():
    assert sic2_to_sector(44) is None

def test_gap_between_wholesale_and_information():
    assert sic2_to_sector(57) is None

def test_gap_between_information_and_finance():
    assert sic2_to_sector(67) is None

def test_gap_between_real_estate_and_professional():
    # 67 is K upper, 68 is L, 69 is M-N — no gap here, but test code 83
    assert sic2_to_sector(83) is None

def test_gap_between_public_admin_and_arts():
    assert sic2_to_sector(89) is None

def test_zero_returns_none():
    assert sic2_to_sector(0) is None

def test_large_unknown_code_returns_none():
    assert sic2_to_sector(100) is None


# --- Typical real-world codes ---

def test_software_publishing_is_J():
    assert sic2_to_sector(58) == "J"

def test_computer_programming_is_J():
    assert sic2_to_sector(62) == "J"

def test_banking_is_K():
    assert sic2_to_sector(64) == "K"

def test_education_is_OQ():
    assert sic2_to_sector(85) == "O-Q"

def test_health_is_OQ():
    assert sic2_to_sector(86) == "O-Q"
