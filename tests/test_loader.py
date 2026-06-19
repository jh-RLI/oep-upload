"""Tests for the layered YAML loader (base < env-specific < local)."""

from __future__ import annotations

import textwrap

import pytest

from oep_upload.config.loader import _build_settings, get_settings

BASE_YAML = """\
app:
  log_level: INFO
api:
  target: remote
  remote:
    host: openenergyplatform.org
    api_base_url: https://openenergyplatform.org/api/v0/
  local:
    host: localhost:8000
    api_base_url: http://localhost:8000/api/v0/
paths:
  root: data
oep_api_token: BASE_TOKEN
"""


def _write(cfg_dir, name, body):
    (cfg_dir / name).write_text(textwrap.dedent(body), encoding="utf-8")


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    _build_settings.cache_clear()
    yield
    _build_settings.cache_clear()


def test_base_only(tmp_path):
    _write(tmp_path, "settings.base.yaml", BASE_YAML)
    s = get_settings(config_dir=tmp_path, env_name="prod")
    assert s.api.target == "remote"
    assert s.app.log_level == "INFO"
    assert s.paths.root == "data"


def test_env_overrides_base(tmp_path):
    _write(tmp_path, "settings.base.yaml", BASE_YAML)
    _write(tmp_path, "settings.dev.yaml", "app:\n  log_level: DEBUG\napi:\n  target: local\n")
    s = get_settings(config_dir=tmp_path, env_name="dev")
    assert s.api.target == "local"
    assert s.app.log_level == "DEBUG"


def test_local_overrides_env_and_base(tmp_path):
    _write(tmp_path, "settings.base.yaml", BASE_YAML)
    _write(tmp_path, "settings.prod.yaml", "api:\n  target: remote\n")
    _write(
        tmp_path,
        "settings.local.yaml",
        "api:\n  target: local\npaths:\n  root: /my/local/data\n",
    )
    s = get_settings(config_dir=tmp_path, env_name="prod")
    # local wins over both base and env
    assert s.api.target == "local"
    assert s.paths.root == "/my/local/data"


def test_deep_merge_keeps_untouched_keys(tmp_path):
    _write(tmp_path, "settings.base.yaml", BASE_YAML)
    _write(tmp_path, "settings.local.yaml", "paths:\n  data_dir: sub\n")
    s = get_settings(config_dir=tmp_path, env_name="prod")
    # local only set data_dir; root from base must survive the merge
    assert s.paths.root == "data"
    assert s.paths.data_dir == "sub"


def test_missing_token_raises_helpful_error(tmp_path, monkeypatch):
    # Isolate from any real token in the environment / repo .env.
    monkeypatch.delenv("OEP_API_TOKEN", raising=False)
    monkeypatch.delenv("OEP_API_TOKEN_LOCAL", raising=False)
    body = BASE_YAML.replace("oep_api_token: BASE_TOKEN\n", "")
    _write(tmp_path, "settings.base.yaml", body)
    # Point at a nonexistent env file so the loader doesn't pick up the repo .env.
    fake_env = tmp_path / "nonexistent.env"
    with pytest.raises(RuntimeError, match="No OEP API token"):
        get_settings(config_dir=tmp_path, env_name="prod", env_file=str(fake_env))
