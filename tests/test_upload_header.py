"""Tests for the type-aware leaked-header detection in the upload step."""

from __future__ import annotations

from oep_upload.upload import datapackage as dp

COLDEFS = {
    "t": {"data_type": "timestamp without time zone"},
    "region": {"data_type": "text"},
    "cf": {"data_type": "double precision"},
}


def test_detects_header_when_name_lands_in_typed_column():
    # The exact failure mode: the timestamp column "t" holds the string "t".
    header = {"t": "t", "region": "region", "cf": "cf"}
    assert dp._looks_like_header_row(header, COLDEFS) is True


def test_real_data_row_is_not_flagged():
    row = {"t": "2018-01-01 00:00:00", "region": "AL012", "cf": "0.5"}
    assert dp._looks_like_header_row(row, COLDEFS) is False


def test_text_column_equal_to_its_name_alone_does_not_flag():
    # A text column whose value happens to equal a column name is not enough;
    # only a *non-text* column carrying a column name flags a header.
    coldefs = {"name": {"data_type": "text"}, "value": {"data_type": "numeric"}}
    assert dp._looks_like_header_row({"name": "name", "value": "3.5"}, coldefs) is False


def test_header_caught_via_numeric_column():
    coldefs = {"name": {"data_type": "text"}, "value": {"data_type": "numeric"}}
    # genuine header: "value" string reaches the numeric column
    assert dp._looks_like_header_row({"name": "name", "value": "value"}, coldefs) is True