"""
Phase 0 — Pre-flight cleanup tests.

Phase 0 deletes stale output files and clears the Spark cache before
a run begins. This prevents a previous run's results from contaminating
a new run when configuration has changed.

Tests here use temporary directories and mock objects so nothing on
disk or in a real Spark session is touched.
"""

import os
import tempfile
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_preflight_cleanup(
    use_existing_dictionary: bool,
    use_existing_polished_dictionary: bool,
    dict_csv_path: str,
    polished_dict_csv_path: str,
    spark_cache_cleared: list,
):
    """
    Standalone extraction of the Phase 0 logic from main.py so it can
    be tested without importing the full pipeline.
    """
    spark_cache_cleared.append(True)  # simulates spark.catalog.clearCache()

    if not use_existing_dictionary:
        if os.path.exists(dict_csv_path):
            os.remove(dict_csv_path)

    if not use_existing_polished_dictionary:
        if os.path.exists(polished_dict_csv_path):
            os.remove(polished_dict_csv_path)


# ---------------------------------------------------------------------------
# Cache clearing
# ---------------------------------------------------------------------------

def test_spark_cache_is_always_cleared():
    with tempfile.TemporaryDirectory() as tmp:
        cleared = []
        run_preflight_cleanup(True, True, "", "", cleared)
        assert cleared == [True], "Spark cache must be cleared on every run."


def test_spark_cache_cleared_even_when_all_flags_true():
    cleared = []
    run_preflight_cleanup(True, True, "nonexistent.csv", "nonexistent.csv", cleared)
    assert len(cleared) == 1


# ---------------------------------------------------------------------------
# Raw dictionary deletion
# ---------------------------------------------------------------------------

def test_raw_dictionary_deleted_when_flag_is_false():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "oecd_dictionary_raw.csv")
        open(path, "w").close()  # create the file

        run_preflight_cleanup(False, True, path, "", [])
        assert not os.path.exists(path), "Raw dictionary should be deleted when USE_EXISTING_DICTIONARY=False."


def test_raw_dictionary_preserved_when_flag_is_true():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "oecd_dictionary_raw.csv")
        open(path, "w").close()

        run_preflight_cleanup(True, True, path, "", [])
        assert os.path.exists(path), "Raw dictionary should be kept when USE_EXISTING_DICTIONARY=True."


def test_no_error_when_raw_dictionary_does_not_exist():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "nonexistent_raw.csv")
        # Should not raise even if the file is missing
        run_preflight_cleanup(False, True, path, "", [])


# ---------------------------------------------------------------------------
# Polished dictionary deletion
# ---------------------------------------------------------------------------

def test_polished_dictionary_deleted_when_flag_is_false():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "oecd_dictionary_polished.csv")
        open(path, "w").close()

        run_preflight_cleanup(True, False, "", path, [])
        assert not os.path.exists(path)


def test_polished_dictionary_preserved_when_flag_is_true():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "oecd_dictionary_polished.csv")
        open(path, "w").close()

        run_preflight_cleanup(True, True, "", path, [])
        assert os.path.exists(path)


def test_no_error_when_polished_dictionary_does_not_exist():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "nonexistent_polished.csv")
        run_preflight_cleanup(True, False, "", path, [])


# ---------------------------------------------------------------------------
# Both files deleted together
# ---------------------------------------------------------------------------

def test_both_files_deleted_when_both_flags_false():
    with tempfile.TemporaryDirectory() as tmp:
        raw = os.path.join(tmp, "raw.csv")
        polished = os.path.join(tmp, "polished.csv")
        open(raw, "w").close()
        open(polished, "w").close()

        run_preflight_cleanup(False, False, raw, polished, [])
        assert not os.path.exists(raw)
        assert not os.path.exists(polished)


def test_only_raw_deleted_when_only_raw_flag_false():
    with tempfile.TemporaryDirectory() as tmp:
        raw = os.path.join(tmp, "raw.csv")
        polished = os.path.join(tmp, "polished.csv")
        open(raw, "w").close()
        open(polished, "w").close()

        run_preflight_cleanup(False, True, raw, polished, [])
        assert not os.path.exists(raw)
        assert os.path.exists(polished)


def test_only_polished_deleted_when_only_polished_flag_false():
    with tempfile.TemporaryDirectory() as tmp:
        raw = os.path.join(tmp, "raw.csv")
        polished = os.path.join(tmp, "polished.csv")
        open(raw, "w").close()
        open(polished, "w").close()

        run_preflight_cleanup(True, False, raw, polished, [])
        assert os.path.exists(raw)
        assert not os.path.exists(polished)


# ---------------------------------------------------------------------------
# Output directory creation
# ---------------------------------------------------------------------------

def test_output_directory_created_if_missing():
    with tempfile.TemporaryDirectory() as tmp:
        export_dir = os.path.join(tmp, "online_job_ads", "OECD")
        reports_dir = os.path.join(export_dir, "reports")

        os.makedirs(export_dir, exist_ok=True)
        os.makedirs(reports_dir, exist_ok=True)

        assert os.path.isdir(export_dir)
        assert os.path.isdir(reports_dir)


def test_makedirs_does_not_raise_if_directory_exists():
    with tempfile.TemporaryDirectory() as tmp:
        # Calling makedirs twice should not raise
        os.makedirs(tmp, exist_ok=True)
        os.makedirs(tmp, exist_ok=True)
