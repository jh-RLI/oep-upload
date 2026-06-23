"""Tests for the verify feature's pure logic (no network)."""

from __future__ import annotations

from oep_upload.verify import (
    TableVerification,
    VerificationReport,
    expected_db_rows,
)


def test_long_csv_expected(tmp_path):
    p = tmp_path / "t.csv"
    p.write_text("t;region;cf\n1;A;0.1\n2;B;0.2\n", encoding="utf-8")
    assert expected_db_rows(p, ";", "utf-8") == ("long", 2)


def test_wide_pivot_expected(tmp_path):
    p = tmp_path / "w.csv"
    # from/to/type header (3 header rows), 2 series columns, 3 data rows -> 6
    p.write_text(
        "from;s1;s2\n"
        "to;d1;d2\n"
        "type;flow;flow\n"
        "10;0.1;0.2\n"
        "11;0.3;0.4\n"
        "12;0.5;0.6\n",
        encoding="utf-8",
    )
    assert expected_db_rows(p, ";", "utf-8") == ("wide/pivot", 6)


def test_wide_pivot_two_row_header(tmp_path):
    p = tmp_path / "w2.csv"
    # from/to header only (2 header rows), 2 series, 2 data rows -> 4
    p.write_text("from;s1;s2\nto;d1;d2\n10;1;2\n11;3;4\n", encoding="utf-8")
    assert expected_db_rows(p, ";", "utf-8") == ("wide/pivot", 4)


def test_empty_csv(tmp_path):
    p = tmp_path / "e.csv"
    p.write_text("", encoding="utf-8")
    assert expected_db_rows(p, ";", "utf-8") == ("empty", 0)


def test_report_ok_allows_review():
    report = VerificationReport(
        results=[
            TableVerification("a", "long", 10, 10, "ok"),
            TableVerification("b", "wide/pivot", 6, 5, "review"),
        ]
    )
    assert report.ok is True


def test_report_not_ok_on_empty_or_mismatch():
    assert not VerificationReport(
        results=[TableVerification("a", "long", 10, 0, "empty")]
    ).ok
    assert not VerificationReport(
        results=[TableVerification("a", "long", 10, 9, "mismatch")]
    ).ok


def test_report_format_table_runs():
    report = VerificationReport(
        results=[TableVerification("a", "long", 10, 10, "ok")]
    )
    out = report.format_table()
    assert "table" in out and "a" in out