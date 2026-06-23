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


def _attach_file_handler(root, log_file: str, level, logger) -> None:
    """Add a single file handler, replacing any we added before (no duplicates)."""
    for h in list(root.handlers):
        if getattr(h, "_oep_file_handler", False):
            root.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass

    path = Path(log_file).expanduser()
    # A directory (or a trailing separator) -> one timestamped file per run.
    if str(log_file).endswith(("/", os.sep)) or path.is_dir():
        from datetime import datetime

        path = path / f"oep-upload_{datetime.now():%Y%m%d-%H%M%S}.log"

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(path, mode="a", encoding="utf-8")
    except OSError as e:
        logger.warning("Could not open log file %s: %s", path, e)
        return

    fh.setFormatter(logging.Formatter(_DEFAULT_FORMAT))
    fh.setLevel(level)
    fh._oep_file_handler = True  # marker so we can replace it on re-config
    root.addHandler(fh)
    logger.info("Writing logs to %s", path)


def setup_logging(
    level: str | int | None = None,
    *,
    log_file: str | None = None,
    force: bool = False,
):
    """Configure application logging once and return the app logger.

    Resolution order for the level:
      1. an explicit ``level`` argument,
      2. the ``OEP_LOG_LEVEL`` / ``LOG_LEVEL`` environment variables,
      3. a sensible ``INFO`` default.

    If a ``logging.yaml`` is found (via ``OEP_LOGGING_FILE``, the package
    config dir, or ``config/logging.yaml``) it is applied via ``dictConfig``;
    otherwise a clean ``basicConfig`` format is used. Pass ``force=True`` to
    re-apply configuration (e.g. once real settings are loaded).

    ``log_file`` (or the ``OEP_LOG_FILE`` env var) additionally writes logs to a
    file: a file path appends; a directory creates one timestamped file per run.
    """
    global _CONFIGURED
    logger = logging.getLogger("oep_upload")

    need_base = force or not _CONFIGURED
    if not need_base and log_file is None and not os.getenv("OEP_LOG_FILE"):
        return logger

    lvl = _resolve_level(level)

    if need_base:
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

    root = logging.getLogger()
    # Ensure the requested level wins regardless of how we configured.
    try:
        root.setLevel(lvl)
    except (ValueError, TypeError):
        pass

    target = log_file if log_file is not None else os.getenv("OEP_LOG_FILE")
    if target:
        _attach_file_handler(root, target, lvl, logger)

    _CONFIGURED = True
    return logger