"""Tests for setup_logging: idempotency and level resolution."""

from __future__ import annotations

import logging

import pytest

from oep_upload.config import logging as oeplog


def _strip_file_handlers():
    root = logging.getLogger()
    for h in list(root.handlers):
        if getattr(h, "_oep_file_handler", False):
            root.removeHandler(h)
            h.close()


@pytest.fixture(autouse=True)
def _reset_logging_state():
    oeplog._CONFIGURED = False
    yield
    _strip_file_handlers()  # don't leave handlers pointing at deleted tmp files
    oeplog._CONFIGURED = False


def test_returns_app_logger():
    assert oeplog.setup_logging().name == "oep_upload"


def test_is_idempotent():
    oeplog.setup_logging(level="WARNING")
    assert oeplog._CONFIGURED is True
    # A second call without force must not blow up and returns the app logger.
    assert oeplog.setup_logging(level="DEBUG").name == "oep_upload"


def test_level_from_argument():
    oeplog.setup_logging(level="ERROR", force=True)
    assert logging.getLogger().level == logging.ERROR


def test_level_from_env(monkeypatch):
    monkeypatch.setenv("OEP_LOG_LEVEL", "WARNING")
    oeplog.setup_logging(force=True)
    assert logging.getLogger().level == logging.WARNING


def test_default_level_is_info(monkeypatch):
    monkeypatch.delenv("OEP_LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    oeplog.setup_logging(level=None, force=True)
    assert logging.getLogger().level == logging.INFO


def test_writes_to_log_file(tmp_path):
    f = tmp_path / "run.log"
    log = oeplog.setup_logging(level="INFO", log_file=str(f), force=True)
    log.info("hello-file-marker")
    for h in logging.getLogger().handlers:
        h.flush()
    assert f.is_file()
    assert "hello-file-marker" in f.read_text(encoding="utf-8")


def test_log_file_directory_creates_per_run_file(tmp_path):
    log = oeplog.setup_logging(level="INFO", log_file=str(tmp_path) + "/", force=True)
    log.info("dir-marker")
    for h in logging.getLogger().handlers:
        h.flush()
    files = list(tmp_path.glob("oep-upload_*.log"))
    assert len(files) == 1
    assert "dir-marker" in files[0].read_text(encoding="utf-8")


def test_broken_logging_file_falls_back(tmp_path, monkeypatch):
    bad = tmp_path / "logging.yaml"
    bad.write_text("this: [is, not, valid: dictConfig", encoding="utf-8")
    monkeypatch.setenv("OEP_LOGGING_FILE", str(bad))
    # Must not raise even though the file is unusable.
    assert oeplog.setup_logging(force=True).name == "oep_upload"