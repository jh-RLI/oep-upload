"""Tests for the layered YAML loader (base < env-specific < local)."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from oep_upload.config.loader import _build_settings, export_env_vars, get_settings

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


def test_env_does_not_flip_to_dev_after_export(tmp_path, monkeypatch):
    # Regression: with no ENV set, the first load uses 'prod' (remote). If the
    # reported env diverged ('dev') and was exported, a later reload would flip
    # onto dev/local. Loads must stay on 'prod'/remote.
    monkeypatch.delenv("ENV", raising=False)
    _write(tmp_path, "settings.base.yaml", BASE_YAML)
    _write(tmp_path, "settings.prod.yaml", "api:\n  target: remote\n")
    _write(tmp_path, "settings.dev.yaml", "api:\n  target: local\n")
    fake_env = tmp_path / "none.env"

    try:
        s1 = get_settings(config_dir=tmp_path, env_file=str(fake_env))
        assert s1.api.target == "remote"
        assert s1.env == "prod"  # reported env matches the loaded one

        export_env_vars(s1)
        assert os.environ.get("ENV") == "prod"

        # Force a real reload, as a fresh submodule import would.
        _build_settings.cache_clear()
        s2 = get_settings(config_dir=tmp_path, env_file=str(fake_env))
        assert s2.api.target == "remote"
        assert s2.endpoint.host == "openenergyplatform.org"
    finally:
        os.environ.pop("ENV", None)


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


def test_env_from_dotenv_selects_environment(tmp_path, monkeypatch):
    # Regression: ENV set in a .env must select the environment. Previously the
    # loader read ENV before loading .env and fell back to 'prod'.
    monkeypatch.delenv("ENV", raising=False)
    _write(tmp_path, "settings.base.yaml", BASE_YAML)  # target remote
    _write(tmp_path, "settings.dev.yaml", "api:\n  target: local\n")
    envf = tmp_path / ".env"
    envf.write_text("ENV=dev\n", encoding="utf-8")

    s = get_settings(config_dir=tmp_path, env_file=str(envf))
    assert s.env == "dev"  # honored ENV=dev from the .env
    assert s.api.target == "local"  # so settings.dev.yaml was loaded


def test_project_local_settings_overlay_from_cwd(tmp_path, monkeypatch):
    # Packaged config (endpoints + token + remote target) lives in `pkg`, while
    # the user's settings.local.yaml lives in their working directory. The CWD
    # file must win — this is what makes settings work for an installed package.
    monkeypatch.delenv("ENV", raising=False)
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    _write(pkg, "settings.base.yaml", BASE_YAML)

    proj = tmp_path / "proj"
    proj.mkdir()
    _write(
        proj,
        "settings.local.yaml",
        "api:\n  target: local\npaths:\n  root: /data/here\n",
    )
    monkeypatch.chdir(proj)

    s = get_settings(config_dir=pkg, env_name="prod", env_file=str(proj / "none.env"))
    assert s.api.target == "local"  # CWD overlay beat packaged base (remote)
    assert s.paths.root == "/data/here"
    assert s.endpoint.host == "localhost:8000"  # endpoints still from packaged base


def _settings_for_export(*, root="/abs", datapackage_file=None):
    from oep_upload.config.models import (
        APISettings,
        AppSettings,
        Endpoint,
        PathsSettings,
        Settings,
    )

    return Settings(
        app=AppSettings(),
        api=APISettings(
            target="remote",
            remote=Endpoint(host="h", api_base_url="https://x.org/api/"),
            local=Endpoint(host="l", api_base_url="http://l/api/"),
        ),
        paths=PathsSettings(root=root, datapackage_file=datapackage_file),
        oep_api_token="t",
    )


def test_export_does_not_clobber_user_oem_file(monkeypatch):
    monkeypatch.setenv("OEP_OEM_FILE", "user/datapackage.json")
    export_env_vars(_settings_for_export(datapackage_file=None))
    # datapackage_file unset -> the user's env value must survive (no empty wipe)
    assert os.environ["OEP_OEM_FILE"] == "user/datapackage.json"


def test_export_sets_resolved_oem_file_when_configured(monkeypatch):
    monkeypatch.delenv("OEP_OEM_FILE", raising=False)
    export_env_vars(_settings_for_export(root="/abs", datapackage_file="dp.json"))
    assert os.environ["OEP_OEM_FILE"] == str(Path("/abs/dp.json"))
