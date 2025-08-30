from __future__ import annotations

import os
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import yaml

# Optional: allow ENV to be read from .env before we choose YAML files
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv(override=False)
except Exception:
    pass

from pydantic_settings import BaseSettings, PydanticBaseSettingsSource
from .models import Settings

ROOT = Path(__file__).resolve().parents[1]
CFG_DIR = ROOT / "config"


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML at {path} must be a mapping")
    return data


def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """merge b into a (new dict)"""
    out = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


class YamlSettingsSource(PydanticBaseSettingsSource[Settings]):
    def __init__(self, settings_cls: type[BaseSettings], env_name: str) -> None:
        super().__init__(settings_cls)
        self.env_name = env_name

    def get_field_value(self, field, field_name: str):
        return None, field_name, False

    def __call__(self) -> Dict[str, Any]:
        base = _read_yaml(CFG_DIR / "settings.base.yaml")
        env_file = CFG_DIR / f"settings.{self.env_name}.yaml"
        env = _read_yaml(env_file)
        return _deep_merge(base, env)


class AppSettings(Settings):
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        env_name = os.getenv("ENV", "dev")
        yaml_source = YamlSettingsSource(settings_cls, env_name)
        # Precedence: init > env vars > .env > YAML > file secrets
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            yaml_source,
            file_secret_settings,
        )


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    s = AppSettings()
    # Fail fast in prod if secrets are missing
    if s.env == "prod" and not s.effective_api_token:
        raise RuntimeError("OEP_API_TOKEN is required in production.")
    return s


def export_env_vars(s: Settings) -> None:
    """
    For legacy code that expects env vars, export them here.
    """
    os.environ["OEP_API_TOKEN"] = s.effective_api_token or ""
    os.environ["OEP_API_URL"] = str(s.endpoint.api_base_url)
    os.environ["OEP_URL"] = s.endpoint.host
    os.environ["OEDIALECT_PROTOCOL"] = s.oedialect_protocol
    if s.oep_user:
        os.environ["OEP_USER"] = s.oep_user
