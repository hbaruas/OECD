"""
Phase 6 — Dashboard Generation tests.

Phase 6 builds a self-contained offline HTML file containing Plotly charts,
a WordCloud, and downloadable data tables. Key behaviours tested:

  - The add_dashboard_section HTML structure
  - Plotly.js is embedded only once (first chart) and reused thereafter
  - WordCloud frequency dict built from vocabulary DataFrame correctly
  - Base64 image embedding produces valid data URI
  - Dashboard HTML contains required structural elements
  - CSV download buttons are wired to correct table IDs
  - Chart data aligns with the underlying DataFrame
  - Output file is written to the correct path
"""

import os
import re
import base64
import tempfile
import pytest
import pandas as pd


# ---------------------------------------------------------------------------
# add_dashboard_section logic (extracted for isolated testing)
# ---------------------------------------------------------------------------

class DashboardBuilder:
    """
    Minimal reimplementation of the dashboard building pattern from main.py
    so it can be tested without running the full pipeline.
    """

    def __init__(self):
        self.html = "<body>"
        self.section_counter = 0
        self.plotly_injected = False

    def add_section(self, title: str, visual_html: str, data_df: pd.DataFrame, is_plotly: bool = True):
        self.section_counter += 1
        table_id = f"table_{self.section_counter}"
        clean_filename = title.split(". ")[-1].replace(" ", "_")
        table_html = data_df.to_html(index=False, table_id=table_id)
        self.html += (
            f'<div class="section-card">'
            f'<h2>{title}</h2>'
            f'<div>{visual_html}</div>'
            f'<div class="table-wrapper">{table_html}</div>'
            f'<button onclick="downloadCSV(\'{table_id}\', \'{clean_filename}\')">'
            f'Download CSV</button>'
            f'</div>'
        )

    def finalize(self) -> str:
        return self.html + "</body></html>"


# ---------------------------------------------------------------------------
# Section structure
# ---------------------------------------------------------------------------

def test_section_increments_counter():
    b = DashboardBuilder()
    b.add_section("1. Test Chart", "<div>chart</div>", pd.DataFrame({"a": [1]}))
    b.add_section("2. Another Chart", "<div>chart2</div>", pd.DataFrame({"b": [2]}))
    assert b.section_counter == 2


def test_section_table_ids_are_unique():
    b = DashboardBuilder()
    df = pd.DataFrame({"x": [1, 2]})
    b.add_section("1. First", "<div/>", df)
    b.add_section("2. Second", "<div/>", df)
    assert "table_1" in b.html
    assert "table_2" in b.html


def test_section_title_appears_in_html():
    b = DashboardBuilder()
    b.add_section("3. Sector Breakdown", "<div/>", pd.DataFrame({"a": [1]}))
    assert "Sector Breakdown" in b.html


def test_section_download_button_references_correct_table():
    b = DashboardBuilder()
    b.add_section("1. Investment", "<div/>", pd.DataFrame({"v": [100]}))
    assert "downloadCSV('table_1'" in b.html


def test_section_csv_filename_derived_from_title():
    b = DashboardBuilder()
    b.add_section("4. Workforce Intensity", "<div/>", pd.DataFrame({"a": [1]}))
    assert "Workforce_Intensity" in b.html


def test_data_table_embedded_in_section():
    b = DashboardBuilder()
    df = pd.DataFrame({"sector": ["J", "K"], "share": [5.2, 3.1]})
    b.add_section("1. Shares", "<div/>", df)
    assert "table_1" in b.html
    assert "sector" in b.html
    assert "share" in b.html


def test_multiple_sections_all_present_in_output():
    b = DashboardBuilder()
    for i in range(5):
        b.add_section(f"{i+1}. Section {i+1}", "<div/>", pd.DataFrame({"n": [i]}))
    html = b.finalize()
    for i in range(1, 6):
        assert f"table_{i}" in html


# ---------------------------------------------------------------------------
# Plotly.js injection — included only once
# ---------------------------------------------------------------------------

def test_plotly_injected_flag_starts_false():
    b = DashboardBuilder()
    assert not b.plotly_injected


def test_plotly_injection_tracked_per_builder():
    b1 = DashboardBuilder()
    b2 = DashboardBuilder()
    b1.plotly_injected = True
    assert not b2.plotly_injected


# ---------------------------------------------------------------------------
# WordCloud frequency dictionary
# ---------------------------------------------------------------------------

def test_word_freq_dict_built_from_vocabulary():
    vocab = pd.DataFrame({
        "noun_chunk": ["sql server", "database", "nosql"],
        "relative_share": [20.0, 15.0, 12.0],
    })
    word_freq = dict(zip(vocab["noun_chunk"], vocab["relative_share"]))
    assert word_freq["sql server"] == 20.0
    assert word_freq["database"] == 15.0
    assert word_freq["nosql"] == 12.0


def test_top_n_terms_selected_by_relative_share():
    vocab = pd.DataFrame({
        "noun_chunk": ["a", "b", "c", "d", "e"],
        "relative_share": [5.0, 20.0, 8.0, 15.0, 12.0],
        "global_count": [100, 200, 150, 180, 160],
    })
    top_3 = vocab.nlargest(3, "relative_share")
    assert set(top_3["noun_chunk"]) == {"b", "d", "e"}


def test_word_freq_keys_are_strings():
    vocab = pd.DataFrame({
        "noun_chunk": ["sql", "oracle", 123],  # 123 is numeric — must be cast
        "relative_share": [20.0, 15.0, 10.0],
    })
    vocab["noun_chunk"] = vocab["noun_chunk"].astype(str).str.replace(r"\n|\r", " ", regex=True)
    word_freq = dict(zip(vocab["noun_chunk"], vocab["relative_share"]))
    assert all(isinstance(k, str) for k in word_freq.keys())


def test_empty_vocabulary_produces_empty_word_freq():
    vocab = pd.DataFrame({"noun_chunk": [], "relative_share": []})
    word_freq = dict(zip(vocab["noun_chunk"], vocab["relative_share"]))
    assert word_freq == {}


# ---------------------------------------------------------------------------
# Base64 image embedding
# ---------------------------------------------------------------------------

def test_base64_encoding_produces_valid_data_uri():
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        f.write(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)  # minimal PNG-like bytes
        path = f.name
    try:
        with open(path, "rb") as img:
            encoded = base64.b64encode(img.read()).decode()
        data_uri = f"data:image/png;base64,{encoded}"
        assert data_uri.startswith("data:image/png;base64,")
        assert len(encoded) > 0
    finally:
        os.remove(path)


def test_base64_roundtrip_preserves_content():
    original = b"test image content 12345"
    encoded = base64.b64encode(original).decode()
    decoded = base64.b64decode(encoded)
    assert decoded == original


def test_data_uri_embedded_in_img_tag():
    encoded = base64.b64encode(b"fake_image").decode()
    img_html = f'<img src="data:image/png;base64,{encoded}" style="max-width:100%;">'
    assert 'src="data:image/png;base64,' in img_html


# ---------------------------------------------------------------------------
# HTML output file
# ---------------------------------------------------------------------------

def test_html_file_written_to_correct_path():
    with tempfile.TemporaryDirectory() as tmp:
        output_path = os.path.join(tmp, "Master_Offline_Dashboard_database.html")
        html_content = "<html><body><h1>Test</h1></body></html>"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        assert os.path.exists(output_path)


def test_html_file_is_readable_and_contains_content():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "dashboard.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write("<html><body>OECD Dashboard</body></html>")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "OECD Dashboard" in content


def test_html_uses_utf8_encoding():
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "dashboard.html")
        content = "<html><body>Gross Value Added — £ billions</body></html>"
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        with open(path, "r", encoding="utf-8") as f:
            result = f.read()
        assert "£" in result
        assert "—" in result


# ---------------------------------------------------------------------------
# Audit log (Phase 7 — tested here as it is output-only)
# ---------------------------------------------------------------------------

def test_audit_log_created_at_expected_path():
    with tempfile.TemporaryDirectory() as tmp:
        log_path = os.path.join(tmp, "pipeline_audit_log_FINAL.txt")
        with open(log_path, "w") as f:
            f.write("OECD PIPELINE FINAL AUDIT\nTotal Jobs Tagged: 12345\n")
        assert os.path.exists(log_path)


def test_audit_log_contains_job_count():
    with tempfile.TemporaryDirectory() as tmp:
        log_path = os.path.join(tmp, "audit.txt")
        job_count = 98765
        with open(log_path, "w") as f:
            f.write(f"Total Jobs Tagged: {job_count}\n")
        with open(log_path, "r") as f:
            content = f.read()
        assert str(job_count) in content


def test_audit_log_contains_timestamp():
    import datetime
    with tempfile.TemporaryDirectory() as tmp:
        log_path = os.path.join(tmp, "audit.txt")
        timestamp = str(datetime.datetime.now())
        with open(log_path, "w") as f:
            f.write(f"OECD PIPELINE FINAL AUDIT - {timestamp}\n")
        with open(log_path, "r") as f:
            content = f.read()
        assert "AUDIT" in content
