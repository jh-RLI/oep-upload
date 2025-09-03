from __future__ import annotations
import os
from pathlib import Path
from functools import lru_cache
from typing import Any, Dict

import yaml

try:
    from dotenv import load_dotenv
except Exception:

    def load_dotenv(*args, **kwargs):  # no-op fallback
        return False


from pydantic_settings import PydanticBaseSettingsSource
from oep_upload.config.models import Settings

ROOT = Path(__file__).resolve().parents[1]


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path or not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML at {path} must be a mapping")
    return data


def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


class YamlSettingsSource(PydanticBaseSettingsSource):
    def __init__(
        self, settings_cls, *, cfg_dir: Path, env_name: str, single_yaml: Path | None
    ):
        super().__init__(settings_cls)
        self.cfg_dir = cfg_dir
        self.env_name = env_name
        self.single_yaml = single_yaml

        if self.single_yaml:
            self._data = _read_yaml(self.single_yaml)
        else:
            base = _read_yaml(self.cfg_dir / "settings.base.yaml")
            env = _read_yaml(self.cfg_dir / f"settings.{self.env_name}.yaml")
            self._data = _deep_merge(base, env)

    def get_field_value(self, field, field_name: str):
        if isinstance(self._data, dict) and field_name in self._data:
            return self._data[field_name], field_name, False
        return None, field_name, False

    def __call__(self) -> Dict[str, Any]:
        return dict(self._data or {})


@lru_cache(maxsize=8)
def _build_settings(
    cfg_dir_str: str,
    env_file_str: str | None,
    env_name: str,
    single_yaml_str: str | None,
):
    if env_file_str:
        load_dotenv(env_file_str, override=True)
    else:
        if hint := os.getenv("OEP_ENV_FILE"):
            load_dotenv(hint, override=True)
        else:
            load_dotenv(Path(ROOT, ".env"), override=False)

    cfg_dir = Path(cfg_dir_str).resolve()
    single_yaml = Path(single_yaml_str).resolve() if single_yaml_str else None

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
            yaml_source = YamlSettingsSource(
                settings_cls,
                cfg_dir=cfg_dir,
                env_name=env_name,
                single_yaml=single_yaml,
            )
            return (
                init_settings,
                env_settings,
                dotenv_settings,
                yaml_source,
                file_secret_settings,
            )

    s = AppSettings()
    if not s.effective_api_token:
        raise RuntimeError("OEP_API_TOKEN is required in production.")
    return s


def get_settings(
    *,
    config_dir: str | os.PathLike | None = None,
    env_file: str | os.PathLike | None = None,
    env_name: str | None = None,
    settings_yaml: str | os.PathLike | None = None,
):
    default_cfg = os.getenv("OEP_CONFIG_DIR") or str(ROOT / "config")
    cfg_dir_str = str(Path(config_dir or default_cfg))
    single_yaml_str = (
        str(settings_yaml) if settings_yaml else os.getenv("OEP_SETTINGS_FILE")
    )
    env_name_eff = env_name or os.getenv("ENV", "dev")
    env_file_str = str(env_file) if env_file else None
    return _build_settings(cfg_dir_str, env_file_str, env_name_eff, single_yaml_str)


def export_env_vars(s: Settings) -> None:
    os.environ["OEP_API_TOKEN"] = s.effective_api_token or ""
    os.environ["OEP_API_TOKEN_LOCAL"] = s.effective_api_token or ""
    os.environ["OEP_API_URL"] = str(s.endpoint.api_base_url)
    os.environ["OEP_URL"] = s.endpoint.host
    os.environ["OEDIALECT_PROTOCOL"] = s.oedialect_protocol
    os.environ["OEP_OEM_FILE"] = s.paths.datapackage_file or ""
    if s.oep_user:
        os.environ["OEP_USER"] = s.oep_user
