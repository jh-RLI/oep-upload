from pathlib import Path
import sys

from oep_upload.config.logging import setup_logging
from oep_upload.config import get_settings, export_env_vars

# 1) Describe/generate datapackage (single package per run)
from oep_upload.describe.csv_file import (
    process_single_datapackage,
    PackageSelectError,
)

# 2) Create tables on OEP
from oep_upload.create.tables import create_tables_on_oedb

# 3) Upload tabular rows
from oep_upload.upload.datapackage import upload_tabular_data

# 4) Upload normalized per-resource metadata
from oep_upload.upload.metadata.resource import upload_resource_metadata_for_package


if __name__ == "__main__":
    loggi = setup_logging()
    settings = get_settings()
    export_env_vars(settings)  # keep compatibility with code using env vars

    loggi.info(
        "Starting with target=%s, base_url=%s",
        settings.api.target,
        settings.endpoint.api_base_url,
    )

    # ------------------------------------------------------------------
    # Select which datapackage to process this run
    # Prefer an explicit package hint (e.g. 'agora', 'datapackages/example'),
    # falling back to auto-detection in the describe step.
    # ------------------------------------------------------------------
    package_hint = getattr(settings.paths, "data_dir", None)

    # ------------------------------------------------------------------
    # 1) Infer/refresh datapackage metadata (safe if it already exists)
    # ------------------------------------------------------------------
    try:
        loggi.info("Inferring metadata for datapackage (hint=%s)...", package_hint)
        process_single_datapackage(
            package_hint=package_hint,
            overwrite_legacy=False,  # set True to also refresh datapackage.json (will backup if enabled)
            stop_on_error=False,
        )
    except PackageSelectError as e:
        loggi.error("Could not resolve a datapackage to process: %s", e)
        # Not fatal: you may already have a datapackage.json in place
    except Exception as e:
        loggi.exception("Unexpected error during metadata inference: %s", e)
        # Keep going; downstream may still work with an existing datapackage

    # ------------------------------------------------------------------
    # 2) Create tables on OEP (reads metadata files from the configured data_dir)
    #    If you want to override, set `path = Path('datapackages/example')` etc.
    # ------------------------------------------------------------------
    try:
        path = Path(settings.paths.data_dir)
        loggi.info("Creating tables on OEP from directory: %s", path)
        create_tables_on_oedb(path)
    except Exception as e:
        loggi.exception("Table creation failed: %s", e)
        sys.exit(1)

    # ------------------------------------------------------------------
    # 3) Upload tabular data rows
    #    This discovers the datapackage path via config/env and streams CSVs.
    # ------------------------------------------------------------------
    try:
        loggi.info("Uploading tabular data rows...")
        upload_tabular_data()
    except SystemExit as e:
        # upload_tabular_data may call SystemExit on configuration issues
        loggi.error("Uploading rows aborted: %s", e)
        sys.exit(1)
    except Exception as e:
        loggi.exception("Uploading rows failed: %s", e)
        sys.exit(1)

    # ------------------------------------------------------------------
    # 4) Upload normalized per-resource metadata (one resource per request)
    #    Uses the same normalizers as the row upload, so names/columns match DB.
    # ------------------------------------------------------------------
    try:
        loggi.info("Uploading per-resource metadata (normalized)...")
        upload_resource_metadata_for_package(
            package_hint=package_hint, extra_keywords=["XYZ"]
        )
    except Exception as e:
        loggi.exception("Uploading resource metadata failed: %s", e)
        sys.exit(1)

    loggi.info("All steps completed successfully.")
