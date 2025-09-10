from pathlib import Path
import sys

from oep_upload.config.logging import setup_logging
from oep_upload.create.tables import create_tables_on_oedb
from oep_upload.upload.datapackage import upload_tabular_data
from oep_upload.config import get_settings, export_env_vars

# NEW: import the generator (single-package) step
from oep_upload.describe.csv import (
    process_single_datapackage,
    PackageSelectError,
)


if __name__ == "__main__":
    loggi = setup_logging()
    settings = get_settings()
    export_env_vars(settings)  # keeps compatibility with code using env vars

    loggi.info(
        "Starting with target=%s, base_url=%s",
        settings.api.target,
        settings.endpoint.api_base_url,
    )

    # -----------------------------
    # 1) Infer datapackage metadata to prepare for next step
    # -----------------------------
    # Uses settings.paths.datapackage_dir as the selector for which datapackage to process.
    # You can also pass a concrete path or name via that setting (e.g., "agora" or "datapackages/example").
    package_hint = getattr(settings.paths, "data_dir", None)
    try:
        loggi.info("Inferring metadata for datapackage (hint=%s)...", package_hint)
        process_single_datapackage(
            package_hint=package_hint,
            overwrite_legacy=False,  # set True to also refresh datapackage.json (backs up if enabled in settings)
            stop_on_error=False,
        )
    except PackageSelectError as e:
        # If no package could be resolved, we log and continue—your flow may still work with existing metadata.
        loggi.error("Could not resolve a datapackage to process: %s", e)
    except Exception as e:
        # Any unexpected error during inference—log and continue to avoid blocking existing workflows.
        loggi.exception("Unexpected error during metadata inference: %s", e)

    # -----------------------------
    # 2) You datapackage.json fiel is available and correct
    # -----------------------------
    # If you are confused how to set the path to the datapackage directory, you can override settings here:
    # path = Path("datapackages/example/")

    # Load the path from settings which have been configured by the user
    # path = Path(settings.paths.data_dir)

    # create_tables_on_oedb(path)
    # upload_tabular_data()
