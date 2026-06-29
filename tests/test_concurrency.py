"""Tests for concurrent/sequential batch posting and the concurrency setting."""

from __future__ import annotations

from oep_upload.config.models import UploadSettings
from oep_upload.upload import datapackage as dp


def test_concurrency_defaults_to_one():
    assert UploadSettings().concurrency == 1


def test_sequential_posting_aggregates(monkeypatch):
    monkeypatch.setattr(dp, "_post_rows", lambda s, t, rows: (bool(rows), ""))
    up, fr, fb, err = dp._post_batches_sequential("s", "t", [[{}, {}], [{}]])
    assert (up, fr, fb, err) == (3, 0, 0, "")


def test_concurrent_posting_aggregates(monkeypatch):
    seen = []

    def fake_post(schema, table, rows):
        seen.append(len(rows))
        ok = len(rows) != 2  # pretend batches of size 2 are rejected
        return ok, ("" if ok else "boom 400")

    monkeypatch.setattr(dp, "_post_rows", fake_post)
    batches = [[{}], [{}, {}], [{}, {}, {}], [{}, {}, {}, {}]]  # sizes 1,2,3,4

    up, fr, fb, err = dp._post_batches_concurrent("s", "t", iter(batches), concurrency=3)

    assert up == 1 + 3 + 4  # the size-2 batch failed
    assert fr == 2
    assert fb == 1
    assert err == "boom 400"
    assert sorted(seen) == [1, 2, 3, 4]  # every batch was attempted exactly once


def test_concurrent_matches_sequential_totals(monkeypatch):
    # same inputs -> same aggregate regardless of path
    monkeypatch.setattr(dp, "_post_rows", lambda s, t, rows: (len(rows) != 0, ""))
    batches = [[{}] * n for n in (5, 0, 3, 10)]
    seq = dp._post_batches_sequential("s", "t", list(batches))
    con = dp._post_batches_concurrent("s", "t", iter(batches), concurrency=4)
    assert seq[0] == con[0]  # uploaded rows match