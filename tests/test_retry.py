"""Tests for the failure journal and retry (pure logic, no network)."""

from __future__ import annotations

import json

from oep_upload.upload import datapackage as dp


def _result(table, *, failed_batches=0, first_error="", uploaded=0):
    return dp.TableUploadResult(
        schema="model_draft",
        table=table,
        uploaded_rows=uploaded,
        failed_rows=0 if not failed_batches else 5,
        failed_batches=failed_batches,
        first_error=first_error,
        csv_paths=[f"data/{table}.csv"],
    )


def test_result_ok_property():
    assert _result("a", uploaded=10).ok is True
    assert _result("a", failed_batches=1).ok is False
    assert _result("a", first_error="boom").ok is False


def test_journal_written_only_for_failures(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    dp._write_failure_journal(
        [_result("good", uploaded=10), _result("bad", failed_batches=1, first_error="400")]
    )
    path = dp._journal_path()
    assert path.is_file()
    data = json.loads(path.read_text(encoding="utf-8"))
    tables = [f["table"] for f in data["failed"]]
    assert tables == ["bad"]  # only the failed table is recorded
    assert data["failed"][0]["first_error"] == "400"


def test_journal_removed_on_full_success(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    dp._write_failure_journal([_result("bad", failed_batches=1)])
    assert dp._journal_path().is_file()
    # a later clean run wipes the stale journal
    dp._write_failure_journal([_result("good", uploaded=3)])
    assert not dp._journal_path().is_file()


def test_read_journal_roundtrip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    dp._write_failure_journal([_result("bad", failed_batches=1)])
    journal = dp._read_failure_journal()
    assert journal is not None
    assert journal["failed"][0]["table"] == "bad"


def test_read_journal_missing_returns_none(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert dp._read_failure_journal() is None


def test_retry_with_no_journal_is_noop(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # no journal present -> returns [] without touching the network
    assert dp.retry_failed_uploads() == []