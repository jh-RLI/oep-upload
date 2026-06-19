from __future__ import annotations

import logging
import logging.config
import os
from pathlib import Path

import yaml

# setup_logging() is called at import time by several modules, so guard against
# repeatedly re-configuring the logging stack.
_CONFIGURED = False

_PACKAGE_CONFIG_DIR = Path(__file__).resolve().parent
_DEFAULT_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def _candidate_yaml_paths() -> list[Path]:
    """Where to look for an optional dictConfig-style logging.yaml."""
    paths: list[Path] = []
    if env := os.getenv("OEP_LOGGING_FILE"):
        paths.append(Path(env))
    # Shipped with the package, then relative to the current working dir.
    paths.append(_PACKAGE_CONFIG_DIR / "logging.yaml")
    paths.append(Path("config") / "logging.yaml")
    return paths


def _resolve_level(level: str | int | None) -> str | int:
    return (
        level
        or os.getenv("OEP_LOG_LEVEL")
        or os.getenv("LOG_LEVEL")
        or "INFO"
    )


def setup_logging(level: str | int | None = None, *, force: bool = False):
    """Configure application logging once and return the app logger.

    Resolution order:
      1. an explicit ``level`` argument,
      2. the ``OEP_LOG_LEVEL`` / ``LOG_LEVEL`` environment variables,
      3. a sensible ``INFO`` default.

    If a ``logging.yaml`` is found (via ``OEP_LOGGING_FILE``, the package
    config dir, or ``config/logging.yaml``) it is applied via ``dictConfig``;
    otherwise a clean ``basicConfig`` format is used. Pass ``force=True`` to
    re-apply configuration (e.g. once real settings are loaded).
    """
    global _CONFIGURED
    logger = logging.getLogger("oep_upload")

    if _CONFIGURED and not force:
        return logger

    lvl = _resolve_level(level)

    configured_from_file = False
    for p in _candidate_yaml_paths():
        try:
            if p and p.is_file():
                with p.open("r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)
                if cfg:
                    logging.config.dictConfig(cfg)
                    configured_from_file = True
                    break
        except Exception:
            # A broken logging.yaml should never crash the app; fall back.
            pass

    if not configured_from_file:
        logging.basicConfig(level=lvl, format=_DEFAULT_FORMAT, force=force)

    # Ensure the requested level wins regardless of how we configured.
    try:
        logging.getLogger().setLevel(lvl)
    except (ValueError, TypeError):
        pass

    _CONFIGURED = True
    logger.debug(
        "Logging configured (level=%s, from_file=%s)", lvl, configured_from_file
    )
    return logger