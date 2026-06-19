"""Tests for resilient CSV encoding handling during metadata inference.

Reproduces the reported Windows failure where Frictionless auto-detects cp1252
and crashes on byte 0x9d (undefined in cp1252).
"""

from __future__ import annotations

from oep_upload.describe import csv_file

# A lone 0x9d byte is undefined in cp1252 and an invalid UTF-8 start byte.
BAD_CSV = b"id;name;value\n1;Hello\x9dWorld;3.5\n2;Foo;4.2\n"


def test_cp1252_cannot_decode_the_sample():
    # Sanity check: this is exactly the failure mode users hit.
    import pytest

    with pytest.raises(UnicodeDecodeError):
        BAD_CSV.decode("cp1252")


def test_transcode_to_utf8_recovers(tmp_path):
    csv = tmp_path / "x.csv"
    csv.write_bytes(BAD_CSV)
    out = csv_file._transcode_to_utf8(csv)
    try:
        assert out.exists()
        text = out.read_text(encoding="utf-8")
        assert "id;name;value" in text
    finally:
        out.unlink(missing_ok=True)


def test_inference_recovers_from_encoding_error(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    csv = data / "sample.csv"
    csv.write_bytes(BAD_CSV)

    meta = csv_file.generate_datapackage_for_csv(csv, tmp_path)

    res = meta["resources"][0]
    # original relative path is preserved (temp file path must not leak)
    assert res["path"] == "data/sample.csv"
    names = [f.get("name") for f in res.get("schema", {}).get("fields", [])]
    assert "id" in names and "value" in names