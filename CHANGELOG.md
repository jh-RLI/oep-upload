# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Faster uploads (opt-in concurrency) + throughput logging**: a new
  `upload.concurrency` setting (default `1` = unchanged) uploads N batches in
  parallel per table via a thread pool, overlapping the per-batch network/server
  wait. The connection pool is sized to match. The upload now logs per-batch and
  per-table **rows/s**, so the bottleneck is measurable. Batch preparation
  (mapping) was refactored into a producer so posting can run sequentially or
  concurrently without changing the mapping logic. See the performance analysis
  note for why uploads are slow (mostly the OEP's change-tracked insert path).
- **Retry only the failed uploads**: a partial upload now records the tables that
  failed to a journal (`upload.failure_log`, default `.oep-upload/last-run.json`),
  and `oep-upload retry` (or `oep_upload.retry()`) re-uploads **only those tables**
  — by default with the `replace` strategy so it's a clean re-upload, not an
  append onto a partial one. The journal is rewritten with whatever still fails,
  so you can iterate fix → retry → fix; a fully successful run clears it. The row
  upload also no longer aborts the whole run when one table errors — it records
  the failure and continues.
- **Upload verification**: a built-in `verify` feature that compares each table's
  row count on the OEP with the count expected from the local CSVs (pivot-aware:
  wide `from`/`to`/`type` files expand to one row per timestamp × series).
  Available as `oep-upload verify`, `oep-upload run --verify`, and
  `oep_upload.verify(...)` (returns a `VerificationReport` with `.ok` /
  `.format_table()`). Exits non-zero on real problems so it works as a pipeline
  gate. Uses the OEP `tables/{table}/rowcount` endpoint.
- **Log to a file**: `--log-file PATH` (or `app.log_file` / `OEP_LOG_FILE`) also
  writes logs to a file — a file path appends, a directory creates one
  timestamped file per run. Pipeline logs now include `PHASE:` banners so a
  single log file is easy to navigate.
- **Upload strategy**: `upload.strategy` = `append` (default) or `replace`.
  `replace` clears the table's existing rows first for a fresh upload. Available
  via `--strategy`, `settings.local.yaml`, and `oep_upload.configure(strategy=...)`.
- **`oep-upload` console command** (plus `python -m oep_upload`) with
  subcommands: `run` (default), `init` (scaffold `settings.local.yaml` + `.env`
  in the current directory), and `config` (print the resolved settings and
  exactly which files were loaded — for diagnosing "my settings have no effect").
- **Programmatic / library API** on the `oep_upload` package: `run()`,
  `configure()`, `describe_datapackage()`, `create_tables()`, `upload_rows()`,
  `upload_metadata()`. `configure()` lets integrators set target/paths/token in
  code (overrides win over files); importing `oep_upload` stays lightweight.
- **Project-local config discovery**: a `settings.local.yaml` in the current
  working directory (or `./config/`) is now merged on top of the packaged
  defaults, so settings take effect even when the package is `pip install`-ed
  into site-packages.
- **Layered, machine-local config**: a new gitignored `settings.local.yaml`
  layer for machine-specific overrides. Settings now merge with the precedence
  `settings.base.yaml < settings.<env>.yaml < settings.local.yaml < env vars / .env`.
- **Path expansion and resolution helpers** on `PathsSettings`: `root`,
  `data_dir` and `datapackage_file` now expand `~` and `$ENV_VARS`, tolerate
  whitespace/empty values, and expose `resolved_root`, `resolved_data_dir` and
  `resolved_datapackage_file` properties that centralize all path joining.
  `data_dir`/`datapackage_file` resolve relative to `root` (absolute overrides).
- **Configurable, level-aware logging**: `setup_logging()` honors the configured
  log level (argument → `OEP_LOG_LEVEL`/`LOG_LEVEL` → `INFO`), loads an optional
  `logging.yaml` if present, and is now idempotent.
- **Test suite** (`tests/`, pytest via `uv`): 28 tests covering path validators
  and resolution, token selection, endpoint protocol derivation, the layered
  YAML loader and its precedence, and logging behavior. Added `pytest` as a dev
  dependency and pytest configuration in `pyproject.toml`.
- **Documentation**: new `docs/usage.md` straightforward getting-started guide.

### Changed

- **Migrated to the current OEP API paths**: table calls (insert, info, meta,
  and the new delete-rows) now use the schemaless `tables/{table}/...` endpoints.
  The legacy `schema/{schema}/tables/...` paths are not redirected for
  POST/PUT/DELETE, so relying on them could silently drop writes.
- `settings.base.yaml` is now generic and safe to commit — relative `root: data`,
  `target: remote`, and documented options. Personal/absolute paths belong in
  `settings.local.yaml`.
- All consumers (`main.py`, `describe`, `upload`, `create`) use the new
  `resolved_*` path properties instead of each re-implementing `root / data_dir`
  joins.
- `main.py` re-applies logging once settings are loaded so `app.log_level` takes
  effect; `export_env_vars` now exports `OEP_LOG_LEVEL`.
- Clearer error when no API token is found — names the exact variable to set for
  the active `api.target` instead of "OEP_API_TOKEN is required in production".
- Updated `docs/hands_on_guide.md` to describe `settings.local.yaml` and relative
  paths instead of editing absolute paths into the shared `settings.base.yaml`.

### Fixed

- **Leaked CSV header row poisoning the first upload batch**: when the header
  heuristics missed a file's header, the header row was sent as data and the OEP
  rejected the *entire* first batch (e.g. `invalid input syntax for type
  timestamp: "t"` — the column name `t` landing in the timestamp column). Added a
  type-aware safety net that drops a leading row when a column name appears in a
  non-text column (near-zero false positives), and made failures impossible to
  miss: a rejected batch now logs `POST FAILED` with a sample row and is reported
  as failed (not "Uploaded"), with a per-table summary of failed/uploaded rows.
- **`.env` `ENV` was read too late to select the environment**: `get_settings`
  resolved the environment name (and `OEP_CONFIG_DIR`/`OEP_SETTINGS_FILE`) from
  `os.environ` *before* loading the `.env`, so `ENV=dev` in a `.env` was ignored
  and the loader fell back to `prod` (e.g. reporting `Environment: prod` while the
  target was `local`). The `.env` is now loaded first, and `oep-upload config`
  lists the env-specific file that is actually used.
- **Installed-package settings were ignored / `.env` had no effect**: config was
  only read from inside the installed package (site-packages), and
  `export_env_vars` overwrote a user-set `OEP_OEM_FILE` (and tokens) with an
  empty string. Now project-local `settings.local.yaml` is discovered from the
  working directory, empty values never clobber existing env vars, and the
  fully *resolved* datapackage path is exported.
- **Developer's local config leaked into wheels**: `settings.local.yaml` is now
  excluded from the built package (`exclude-package-data`), so one machine's
  paths/target can't be shipped to others.
- **`effective_api_token` dead branch**: removed the unreachable
  `api.target == "production"` check (`target` is only `remote`/`local`). For a
  `local` target it now prefers `OEP_API_TOKEN_LOCAL` and falls back to
  `OEP_API_TOKEN`.
- **Endpoint protocol derivation never ran**: the `protocol` field was never
  derived from the URL when left unset (a `mode="before"` field validator does
  not run on default values). Reimplemented as a `model_validator(mode="after")`
  that derives `http`/`https` from `api_base_url`.
- **Environment flipped to dev/local by default**: with `ENV` unset the loader
  loaded `prod` (remote) but the `env` field defaulted to `dev`, which
  `export_env_vars` wrote back to `ENV`, so the next `get_settings()` (triggered
  by every submodule import) reloaded `dev` and silently targeted `localhost`.
  The reported `env` is now pinned to the environment actually loaded, and the
  `env` field defaults to `prod` to match the loader.
- **CSV encoding crashes during metadata inference**: the describe step now
  retries against a UTF-8 normalized copy when Frictionless/omi mis-detects the
  encoding (e.g. cp1252 on Windows) and fails on a stray byte such as `0x9d`.
  Tries the configured `files.encoding`, then UTF-8, cp1252 and Latin-1.
- **Logging fallback**: previously always fell back to a bare `basicConfig`
  (the referenced `config/logging.yaml` did not exist) and re-configured the
  stack on every import. Now configures once and respects the chosen level.
- Renamed the example environment file `.env.exmple` → `.env.example` to match
  the README and guides.
- `tools/xls_to_csv` usage is now documented with its required extra
  dependencies (`pandas`, `openpyxl`).

### Known issues / not yet addressed

- `tools/xls_to_csv.py` depends on `pandas` and `openpyxl`, which are not
  declared in project dependencies (install them manually, or they should be
  added as an optional dependency group).
- `.pre-commit-config.yaml` defines three separate top-level `repos:` keys (only
  the last is used) and references `requirements.in`/`requirements.txt` that do
  not exist.
- `upload/null_type_helpers.py` is currently unused by the upload pipeline.
- The dataset creation/assignment helpers in `api/oep.py`
  (`ensure_dataset_from_datapackage`) are not wired into `main.py`.
