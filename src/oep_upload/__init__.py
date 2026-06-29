"""oep-upload — publish tabular data and metadata to the Open Energy Platform.

Two ways to use it.

**Command line**

    oep-upload init        # scaffold settings.local.yaml + .env in this folder
    oep-upload config      # show the resolved configuration & where it loaded from
    oep-upload             # run the full pipeline

**From your own Python code**

    import oep_upload

    # (a) rely on .env / settings.local.yaml discovered in the working directory:
    oep_upload.run()

    # (b) or configure everything in code — no config files needed:
    oep_upload.run(
        data_root="/data/my_dataset",
        datapackage_file="datapackage.json",
        target="remote",
        api_token="...",
    )

    # the individual steps are available too:
    oep_upload.create_tables()
    oep_upload.upload_rows()

Note: ``run()`` / the step helpers / ``configure()`` set up configuration before
importing the heavy submodules (which read settings at import time). Call one of
them — or set your environment — *before* importing
``oep_upload.upload``/``create``/``describe`` directly. Configure once per process.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from oep_upload.config import export_env_vars, get_settings
from oep_upload.config.loader import _build_settings

__all__ = [
    "configure",
    "run",
    "describe_datapackage",
    "create_tables",
    "upload_rows",
    "upload_metadata",
    "verify",
    "retry",
    "get_settings",
    "export_env_vars",
]


def configure(
    *,
    config_dir: str | os.PathLike | None = None,
    env_file: str | os.PathLike | None = None,
    env_name: str | None = None,
    settings_yaml: str | os.PathLike | None = None,
    target: str | None = None,
    data_root: str | os.PathLike | None = None,
    data_dir: str | os.PathLike | None = None,
    datapackage_file: str | os.PathLike | None = None,
    api_token: str | None = None,
    log_level: str | None = None,
    log_file: str | os.PathLike | None = None,
    strategy: str | None = None,
):
    """Apply configuration overrides for programmatic use and return the Settings.

    Values passed here take precedence over settings files and any existing
    environment variables. They are written to the process environment, so call
    this once near the start of your program. Returns the resolved ``Settings``
    so you can inspect the active target, paths, etc.

    ``strategy`` is the upload strategy (``"append"`` | ``"replace"``) and
    ``log_file`` additionally writes logs to a file (or a directory for one file
    per run).
    """
    overrides = {
        "OEP_CONFIG_DIR": config_dir,
        "OEP_ENV_FILE": env_file,
        "ENV": env_name,
        "OEP_SETTINGS_FILE": settings_yaml,
        "API__TARGET": target,
        "PATHS__ROOT": data_root,
        "PATHS__DATA_DIR": data_dir,
        "PATHS__DATAPACKAGE_FILE": datapackage_file,
        "OEP_API_TOKEN": api_token,
        "APP__LOG_LEVEL": log_level,
        "APP__LOG_FILE": log_file,
        "UPLOAD__STRATEGY": strategy,
    }
    for key, val in overrides.items():
        if val is not None:
            os.environ[key] = str(val)

    _build_settings.cache_clear()  # ensure the overrides take effect
    settings = get_settings()
    export_env_vars(settings)
    return settings


def run(**overrides: Any) -> int:
    """Run the full pipeline. Accepts the same keyword overrides as ``configure``.

    Returns a process exit code (0 = success).
    """
    if overrides:
        configure(**overrides)
    from oep_upload.cli import cmd_run

    return cmd_run()


def describe_datapackage(package_hint: Any = None, **overrides: Any):
    """Infer / refresh datapackage metadata. Returns the per-CSV results."""
    if overrides:
        configure(**overrides)
    from oep_upload.describe.csv_file import process_single_datapackage

    return process_single_datapackage(package_hint=package_hint)


def create_tables(data_dir: str | os.PathLike | None = None, **overrides: Any):
    """Create the tables on the OEP from the datapackage metadata."""
    if overrides:
        configure(**overrides)
    from oep_upload.create.tables import create_tables_on_oedb

    path = Path(data_dir) if data_dir else get_settings().paths.resolved_data_dir
    return create_tables_on_oedb(path)


def upload_rows(**overrides: Any):
    """Stream and upload the data rows for every table in the datapackage."""
    if overrides:
        configure(**overrides)
    from oep_upload.upload.datapackage import upload_tabular_data

    return upload_tabular_data()


def upload_metadata(
    package_hint: Any = None,
    *,
    extra_keywords: list[str] | None = None,
    **overrides: Any,
):
    """Upload normalized per-resource metadata, one document per table."""
    if overrides:
        configure(**overrides)
    from oep_upload.upload.metadata.resource import (
        upload_resource_metadata_for_package,
    )

    return upload_resource_metadata_for_package(
        package_hint=package_hint, extra_keywords=extra_keywords
    )


def verify(**overrides: Any):
    """Verify the upload: compare local CSV row counts with the OEP table counts.

    Returns a ``VerificationReport`` (``.ok`` is True when nothing is wrong,
    ``.format_table()`` renders a summary). Accepts the same keyword overrides as
    :func:`configure`, so it works standalone or as a pipeline step after
    :func:`run`.
    """
    if overrides:
        configure(**overrides)
    from oep_upload.verify import verify_uploaded_data

    return verify_uploaded_data()


def retry(*, strategy: str = "replace", **overrides: Any):
    """Re-upload only the tables that failed in the last run.

    Reads the failure journal written by the previous upload and re-uploads
    just those tables (default strategy ``replace``). Accepts the same keyword
    overrides as :func:`configure`. Returns the list of per-table results.
    """
    if overrides:
        configure(**overrides)
    from oep_upload.upload.datapackage import retry_failed_uploads

    return retry_failed_uploads(strategy=strategy)