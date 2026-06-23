"""Tests for the programmatic (library) API exposed from `oep_upload`."""

from __future__ import annotations

import os

import pytest

import oep_upload
from oep_upload.config.loader import _build_settings


@pytest.fixture(autouse=True)
def _isolate_env():
    # configure() writes to the process environment; fully restore it after.
    saved = dict(os.environ)
    _build_settings.cache_clear()
    yield
    os.environ.clear()
    os.environ.update(saved)
    _build_settings.cache_clear()


def test_public_api_surface():
    for name in ("run", "configure", "create_tables", "upload_rows", "upload_metadata"):
        assert callable(getattr(oep_upload, name))


def test_configure_overrides_win(tmp_path, monkeypatch):
    monkeypatch.delenv("ENV", raising=False)
    s = oep_upload.configure(
        target="remote",
        data_root=str(tmp_path),
        datapackage_file="dp.json",
        api_token="tok123",
        log_level="DEBUG",
    )
    # code overrides beat the packaged/local YAML
    assert s.api.target == "remote"
    assert s.effective_api_token == "tok123"
    assert s.app.log_level == "DEBUG"
    # endpoints still come from the packaged base config
    assert s.api.remote.host == "openenergyplatform.org"
    # path overrides flow through to the resolved datapackage path
    assert str(s.paths.resolved_datapackage_file).endswith("dp.json")
    assert str(s.paths.resolved_root) == str(tmp_path)


def test_configure_strategy_and_log_file(tmp_path, monkeypatch):
    monkeypatch.delenv("ENV", raising=False)
    log_path = tmp_path / "run.log"
    s = oep_upload.configure(
        api_token="tok",
        strategy="replace",
        log_file=str(log_path),
    )
    assert s.upload.strategy == "replace"
    assert s.app.log_file == str(log_path)
