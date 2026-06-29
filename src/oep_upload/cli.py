"""Command-line entry point for oep-upload.

Subcommands:
  run     Run the full pipeline: describe -> create -> upload rows -> upload metadata (default).
  init    Create starter settings.local.yaml and .env in a directory of your choice.
  config  Show the active configuration and exactly which files it was loaded from.

Available as the ``oep-upload`` console command, via ``python -m oep_upload``,
and via the repo's ``main.py``. Config and ``.env`` are discovered from the
current working directory, so it behaves the same however it was installed.
"""

from __future__ import annotations

import argparse
import sys
import os
from pathlib import Path

from oep_upload.config import export_env_vars, get_settings
from oep_upload.config.loader import _build_settings, active_config_files
from oep_upload.config.logging import setup_logging

SETTINGS_LOCAL_TEMPLATE = """\
# oep-upload — your machine-specific settings.
# This file is read from the directory you run oep-upload in, and overrides the
# packaged defaults. Keep it out of version control.
#
# Precedence: packaged defaults < this file < environment variables / .env

api:
  # Which OEP to talk to: "remote" (public openenergyplatform.org) or "local".
  target: remote

paths:
  # Folder holding your datapackage (datapackage.json + data/*.csv).
  # Relative paths are resolved from where you run oep-upload. ~ and $VARS work.
  root: data/my_dataset
  # Optional: subfolder with the CSVs, relative to root (defaults to root).
  # data_dir: .
  # Optional: the datapackage.json, relative to data_dir (or absolute).
  datapackage_file: datapackage.json

app:
  log_level: INFO   # DEBUG | INFO | WARNING | ERROR | CRITICAL
"""

ENV_TEMPLATE = """\
# oep-upload credentials & overrides. NEVER commit this file.

# Your OEP API token (find it in your OEP profile settings).
OEP_API_TOKEN=

# Optional: which environment to load (prod = public OEP). Usually leave unset.
# ENV=prod

# Optional: point at your datapackage without editing YAML (relative to CWD or absolute).
# OEP_OEM_FILE=data/my_dataset/datapackage.json
"""


def _phase(loggi, title: str) -> None:
    bar = "=" * 64
    loggi.info(bar)
    loggi.info("PHASE: %s", title)
    loggi.info(bar)


def cmd_run(
    strategy: str | None = None,
    log_file: str | None = None,
    verify_after: bool = False,
) -> int:
    """Run the full upload pipeline. Returns a process exit code (0 = success)."""
    # Apply CLI overrides via the environment BEFORE settings are built or the
    # heavy submodules are imported (they read settings at import time).
    if strategy:
        os.environ["UPLOAD__STRATEGY"] = strategy
        _build_settings.cache_clear()

    loggi = setup_logging()
    settings = get_settings()
    export_env_vars(settings)  # keep compatibility with code using env vars
    loggi = setup_logging(
        level=settings.app.log_level,
        log_file=log_file or settings.app.log_file,
        force=True,
    )

    loggi.info(
        "Starting with target=%s, base_url=%s, strategy=%s",
        settings.api.target,
        settings.endpoint.api_base_url,
        settings.upload.strategy,
    )

    # Imported lazily (after settings/env are in place): these modules build
    # settings at import time, so this also lets `init`/`config` work before the
    # user has valid config.
    from oep_upload.describe.csv_file import (
        PackageSelectError,
        process_single_datapackage,
    )
    from oep_upload.create.tables import create_tables_on_oedb
    from oep_upload.upload.datapackage import upload_tabular_data
    from oep_upload.upload.metadata.resource import (
        upload_resource_metadata_for_package,
    )

    package_hint = getattr(settings.paths, "data_dir", None)

    # 1) Infer/refresh datapackage metadata (best effort; safe if it exists)
    _phase(loggi, "1/4 Describe — infer datapackage metadata")
    try:
        loggi.info("Inferring metadata for datapackage (hint=%s)...", package_hint)
        process_single_datapackage(
            package_hint=package_hint, overwrite_legacy=False, stop_on_error=False
        )
    except PackageSelectError as e:
        loggi.error("Could not resolve a datapackage to process: %s", e)
    except Exception as e:  # noqa: BLE001 - keep going; an existing datapackage may work
        loggi.exception("Unexpected error during metadata inference: %s", e)

    # 2) Create tables on OEP
    _phase(loggi, "2/4 Create — tables on the OEP")
    try:
        path = settings.paths.resolved_data_dir
        loggi.info("Creating tables on OEP from directory: %s", path)
        create_tables_on_oedb(path)
    except Exception as e:  # noqa: BLE001
        loggi.exception("Table creation failed: %s", e)
        return 1

    # 3) Upload tabular data rows
    _phase(loggi, "3/4 Upload — data rows (strategy=%s)" % settings.upload.strategy)
    try:
        loggi.info("Uploading tabular data rows...")
        upload_tabular_data()
    except SystemExit as e:
        loggi.error("Uploading rows aborted: %s", e)
        return 1
    except Exception as e:  # noqa: BLE001
        loggi.exception("Uploading rows failed: %s", e)
        return 1

    # 4) Upload normalized per-resource metadata
    _phase(loggi, "4/4 Upload — per-resource metadata")
    try:
        loggi.info("Uploading per-resource metadata (normalized)...")
        upload_resource_metadata_for_package(package_hint=package_hint)
    except Exception as e:  # noqa: BLE001
        loggi.exception("Uploading resource metadata failed: %s", e)
        return 1

    loggi.info("All steps completed successfully.")

    if verify_after:
        _phase(loggi, "Verify — local data vs OEP row counts")
        from oep_upload.verify import verify_uploaded_data

        report = verify_uploaded_data()
        for line in report.format_table().splitlines():
            loggi.info(line)
        if not report.ok:
            loggi.error("Verification found problems (see the table above).")
            return 1

    return 0


def cmd_verify() -> int:
    """Compare the local datapackage data with the OEP row counts."""
    settings = get_settings()
    export_env_vars(settings)
    setup_logging(level=settings.app.log_level, log_file=settings.app.log_file, force=True)

    from oep_upload.verify import verify_uploaded_data

    report = verify_uploaded_data()
    print(report.format_table())
    print()
    print("Result:", "OK" if report.ok else "PROBLEMS FOUND")
    return 0 if report.ok else 1


def cmd_retry(strategy: str = "replace", log_file: str | None = None) -> int:
    """Re-upload only the tables that failed in the last run."""
    settings = get_settings()
    export_env_vars(settings)
    setup_logging(
        level=settings.app.log_level,
        log_file=log_file or settings.app.log_file,
        force=True,
    )

    from oep_upload.upload.datapackage import retry_failed_uploads

    results = retry_failed_uploads(strategy=strategy)
    if not results:
        print("Nothing to retry.")
        return 0

    still_failing = [r for r in results if not r.ok]
    for r in results:
        state = "OK" if r.ok else "FAILED"
        print(
            f"  {r.table}: uploaded={r.uploaded_rows} failed_rows={r.failed_rows}"
            f"  -> {state}"
        )
    print()
    if still_failing:
        print(f"Result: {len(still_failing)} table(s) still failing.")
        return 1
    print("Result: all retried tables uploaded.")
    return 0


def cmd_init(target_dir: str, force: bool) -> int:
    """Scaffold settings.local.yaml and .env in `target_dir`."""
    d = Path(target_dir).expanduser().resolve()
    d.mkdir(parents=True, exist_ok=True)

    for name, content in (
        ("settings.local.yaml", SETTINGS_LOCAL_TEMPLATE),
        (".env", ENV_TEMPLATE),
    ):
        p = d / name
        if p.exists() and not force:
            print(f"•  {p}  already exists — skipped (use --force to overwrite)")
            continue
        p.write_text(content, encoding="utf-8")
        print(f"✓  wrote {p}")

    print()
    print("Next steps:")
    print(f"  1. Add your OEP API token in   {d / '.env'}   (OEP_API_TOKEN=...)")
    print(f"  2. Set your data location in   {d / 'settings.local.yaml'}   (paths.root)")
    print(f"  3. From {d}, run:  oep-upload   (or 'oep-upload config' to verify)")
    return 0


def cmd_config() -> int:
    """Show which config files are seen and the resulting active settings."""
    print("Config files (✓ found · absent), lowest -> highest precedence:")
    for f in active_config_files():
        print(f"  {'✓' if f.is_file() else '·'}  {f}")
    print()
    try:
        s = get_settings()
        export_env_vars(s)
    except Exception as e:  # noqa: BLE001
        print(f"⚠  Could not build settings: {e}")
        return 1

    print(f"Environment        : {s.env}")
    print(f"Target             : {s.api.target}  ->  {s.endpoint.api_base_url}")
    print(f"API token set      : {bool(s.effective_api_token)}")
    print(f"Data root          : {s.paths.resolved_root}")
    print(f"Data dir           : {s.paths.resolved_data_dir}")
    print(f"Datapackage file   : {s.paths.resolved_datapackage_file}")
    print(f"Upload strategy    : {s.upload.strategy}")
    print(f"Batch / concurrency: {s.upload.batch_size} rows × {s.upload.concurrency} parallel")
    print(f"Log level / file   : {s.app.log_level} / {s.app.log_file or '(console only)'}")
    return 0


def _add_run_options(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--strategy",
        choices=["append", "replace"],
        default=None,
        help="Upload strategy: append (default) or replace (clear table first).",
    )
    p.add_argument(
        "--log-file",
        dest="log_file",
        default=None,
        metavar="PATH",
        help="Also write logs to PATH (a directory makes one file per run).",
    )
    p.add_argument(
        "--verify",
        action="store_true",
        help="After uploading, verify local row counts against the OEP.",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="oep-upload",
        description="Upload tabular data and metadata to the Open Energy Platform.",
    )
    # Allow `oep-upload --strategy ... --log-file ...` with no subcommand (= run).
    _add_run_options(parser)
    sub = parser.add_subparsers(dest="command")
    _add_run_options(sub.add_parser("run", help="Run the full upload pipeline (default)."))
    p_init = sub.add_parser(
        "init", help="Create starter settings.local.yaml and .env."
    )
    p_init.add_argument(
        "--dir", default=".", help="Where to create the files (default: current dir)."
    )
    p_init.add_argument(
        "--force", action="store_true", help="Overwrite existing files."
    )
    sub.add_parser(
        "config", help="Show the active configuration and where it loaded from."
    )
    sub.add_parser(
        "verify",
        help="Compare local datapackage row counts with the OEP (no upload).",
    )
    p_retry = sub.add_parser(
        "retry",
        help="Re-upload only the tables that failed in the last run.",
    )
    p_retry.add_argument(
        "--strategy",
        choices=["append", "replace"],
        default="replace",
        help="How to re-upload failed tables (default: replace = clear first).",
    )
    p_retry.add_argument(
        "--log-file",
        dest="log_file",
        default=None,
        metavar="PATH",
        help="Also write logs to PATH (a directory makes one file per run).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "init":
        return cmd_init(args.dir, args.force)
    if args.command == "config":
        return cmd_config()
    if args.command == "verify":
        return cmd_verify()
    if args.command == "retry":
        return cmd_retry(strategy=args.strategy, log_file=args.log_file)
    # default and "run"
    return cmd_run(
        strategy=args.strategy, log_file=args.log_file, verify_after=args.verify
    )


if __name__ == "__main__":
    sys.exit(main())
