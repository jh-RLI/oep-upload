"""The OEP API client must build schemaless `tables/{table}/...` URLs."""

from __future__ import annotations

from oep_upload.api.oep import OEPApiClient


def _client():
    return OEPApiClient(base_url="https://example.org/api/v0", timeout_s=30)


def test_table_info_and_meta_paths():
    c = _client()
    assert c.join("tables", "t", "") == "https://example.org/api/v0/tables/t/"
    assert c.join("tables", "t", "meta", "") == "https://example.org/api/v0/tables/t/meta/"


def test_rows_paths():
    c = _client()
    assert c.join("tables", "t", "rows", "new") == "https://example.org/api/v0/tables/t/rows/new"
    # delete-all-rows endpoint (no row_id, no where)
    assert c.join("tables", "t", "rows", "") == "https://example.org/api/v0/tables/t/rows/"


def test_no_schema_segment_in_paths():
    c = _client()
    assert "schema/" not in c.join("tables", "t", "rows", "new")