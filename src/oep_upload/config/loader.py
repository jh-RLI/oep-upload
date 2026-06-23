from __future__ import annotations
import os
from pathlib import Path
from functools import lru_cache
from typing import Any, Dict

import yaml

from dotenv import load_dotenv, find_dotenv


from pydantic_settings import PydanticBaseSettingsSource
from oep_upload.config.models import Settings

ROOT = Path(__file__).resolve().parents[1]

# Keep this if you need it for your YAML paths
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]  # .../oep-upload


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


def _cwd_overlay_files() -> list[Path]:
    """Project-local settings discovered in the current working directory.

    These let a user's settings take effect even when the package is installed
    into site-packages (where the packaged config dir is not their project).
    """
    cwd = Path.cwd()
    return [cwd / "settings.local.yaml", cwd / "config" / "settings.local.yaml"]


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
            # Precedence (lowest -> highest):
            #   base < env-specific < packaged local < project-local (CWD).
            # settings.local.yaml is gitignored and holds machine-specific
            # overrides (paths, target, ...) so users never edit tracked files.
            base = _read_yaml(self.cfg_dir / "settings.base.yaml")
            env = _read_yaml(self.cfg_dir / f"settings.{self.env_name}.yaml")
            local = _read_yaml(self.cfg_dir / "settings.local.yaml")
            merged = _deep_merge(_deep_merge(base, env), local)

            # Overlay a project-local settings.local.yaml found in the working
            # directory, so settings work for an *installed* package (where
            # cfg_dir points into site-packages, not the user's project).
            pkg_local = (self.cfg_dir / "settings.local.yaml").resolve()
            for cand in _cwd_overlay_files():
                if cand.is_file() and cand.resolve() != pkg_local:
                    merged = _deep_merge(merged, _read_yaml(cand))

            self._data = merged

    def get_field_value(self, field, field_name: str):
        if isinstance(self._data, dict) and field_name in self._data:
            return self._data[field_name], field_name, False
        return None, field_name, False

    def __call__(self) -> Dict[str, Any]:
        return dict(self._data or {})


def _load_env_files(env_file_str: str | None) -> None:
    """Load variables from a .env file.

    Called *before* settings are resolved so that ENV / OEP_CONFIG_DIR /
    OEP_SETTINGS_FILE set in the .env actually influence *which* config loads.
    """
    if env_file_str:
        load_dotenv(env_file_str, override=True)
    elif hint := os.getenv("OEP_ENV_FILE"):
        load_dotenv(hint, override=True)
    elif found := find_dotenv(usecwd=True):
        # A .env discovered from the working directory upward.
        load_dotenv(found, override=False)
    else:
        # Fallback: repo root (two levels above the package root).
        load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=False)


@lru_cache(maxsize=8)
def _build_settings(
    cfg_dir_str: str,
    env_file_str: str | None,
    env_name: str,
    single_yaml_str: str | None,
):
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
            # --- bind outer variables here to avoid NameError ---
            _cfg_dir=cfg_dir,
            _env_name=env_name,
            _single_yaml=single_yaml,
        ):
            yaml_source = YamlSettingsSource(
                settings_cls,
                cfg_dir=_cfg_dir,
                env_name=_env_name,
                single_yaml=_single_yaml,
            )
            return (
                init_settings,
                env_settings,
                dotenv_settings,
                yaml_source,
                file_secret_settings,
            )

    s = AppSettings()

    # Pin the reported environment to the one we actually loaded. Without this,
    # `s.env` could keep its model default ('dev') while we loaded 'prod', and
    # export_env_vars() would then write ENV=dev — flipping a later
    # get_settings() onto the dev/local target. Keep them consistent so repeated
    # loads (every submodule import calls get_settings) are stable.
    if env_name in ("dev", "prod", "test"):
        s.env = env_name

    if not s.effective_api_token:
        token_hint = (
            "OEP_API_TOKEN_LOCAL (or OEP_API_TOKEN)"
            if s.api.target == "local"
            else "OEP_API_TOKEN"
        )
        raise RuntimeError(
            f"No OEP API token found for target='{s.api.target}'. "
            f"Set {token_hint} in your .env file (copy .env.example to .env). "
            "You can find your token in your OEP profile settings."
        )
    return s


def get_settings(
    *,
    config_dir: str | os.PathLike | None = None,
    env_file: str | os.PathLike | None = None,
    env_name: str | None = None,
    settings_yaml: str | os.PathLike | None = None,
):
    env_file_str = str(env_file) if env_file else None
    # Load .env FIRST so that ENV / OEP_CONFIG_DIR / OEP_SETTINGS_FILE defined in
    # it influence which config is selected below (otherwise ENV=dev in a .env was
    # read too late and the loader fell back to 'prod').
    _load_env_files(env_file_str)

    default_cfg = os.getenv("OEP_CONFIG_DIR") or str(ROOT / "config")
    cfg_dir_str = str(Path(config_dir or default_cfg))
    single_yaml_str = (
        str(settings_yaml) if settings_yaml else os.getenv("OEP_SETTINGS_FILE")
    )
    env_name_eff = env_name or os.getenv("ENV", "prod")
    return _build_settings(cfg_dir_str, env_file_str, env_name_eff, single_yaml_str)


def active_config_files(
    *,
    config_dir: str | os.PathLike | None = None,
    env_name: str | None = None,
) -> list[Path]:
    """The YAML files considered when loading settings, lowest precedence first.

    Useful for diagnostics ("why didn't my settings apply?"): callers can check
    which of these actually exist on disk.
    """
    # Load .env first so the reported env-specific file matches what get_settings
    # will actually load (e.g. ENV=dev in a .env selects settings.dev.yaml).
    _load_env_files(None)
    default_cfg = os.getenv("OEP_CONFIG_DIR") or str(ROOT / "config")
    cfg_dir = Path(config_dir or default_cfg)
    env_name_eff = env_name or os.getenv("ENV", "prod")
    return [
        cfg_dir / "settings.base.yaml",
        cfg_dir / f"settings.{env_name_eff}.yaml",
        cfg_dir / "settings.local.yaml",
        *_cwd_overlay_files(),
    ]


def export_env_vars(s: Settings) -> None:
    os.environ["ENV"] = s.env
    os.environ["OEP_LOG_LEVEL"] = s.app.log_level

    # Never overwrite an existing value with an empty string (that previously
    # wiped a token / OEP_OEM_FILE the user had set in their .env).
    token = s.effective_api_token
    if token:
        os.environ["OEP_API_TOKEN"] = token
        os.environ["OEP_API_TOKEN_LOCAL"] = token

    os.environ["OEP_API_URL"] = str(s.endpoint.api_base_url)
    os.environ["OEP_URL"] = s.endpoint.host
    os.environ["OEDIALECT_PROTOCOL"] = s.oedialect_protocol

    # Export the fully *resolved* datapackage path, and only when we actually
    # know it. If it is unset here, leave any user-provided OEP_OEM_FILE intact.
    dpf = s.paths.resolved_datapackage_file
    if dpf is not None:
        os.environ["OEP_OEM_FILE"] = str(dpf)

    if s.oep_user:
        os.environ["OEP_USER"] = s.oep_user
