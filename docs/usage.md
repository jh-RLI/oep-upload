# Using oep-upload

A step-by-step guide to publish your tabular data and metadata to the
[Open Energy Platform](https://openenergyplatform.org) (OEP).

> **In one sentence:** point the tool at a folder of CSV files described by a
> `datapackage.json`, run it once, and it creates the tables on the OEP, uploads
> the rows, and uploads the metadata.

> [!tip] Uploading to a local Docker OEP?
> See the focused walkthrough in
> [local-oep-quickstart.md](local-oep-quickstart.md) — from-scratch setup plus
> fresh-vs-continue uploads and a verification step (CLI and a Python script).

---

## What the tool does

When you run it, four steps happen in order:

| Step | Module | What it does |
| --- | --- | --- |
| 1. Describe | `oep_upload.describe` | Looks at your CSV files and infers/refreshes metadata into `datapackage.generated.json`. Safe to skip if you already have a good `datapackage.json`. |
| 2. Create | `oep_upload.create` | Reads the metadata, normalizes table/column names to OEP rules, and creates the tables on the OEP (in `model_draft`). |
| 3. Upload rows | `oep_upload.upload` | Streams each CSV (handles large files), maps columns to the created tables, and uploads the data in batches — parents before children (foreign-key order). |
| 4. Upload metadata | `oep_upload.upload.metadata` | Uploads one metadata document per table. |

---

## Before you start

You need:

1. **Python 3.13** and the [`uv`](https://docs.astral.sh/uv/) package manager.
2. **An OEP account and API token.** Find the token in your OEP **profile settings**.
3. **Your data as CSV files**, organized as a datapackage (see [Step 2](#step-2-organize-your-data)).

---

## Step 1: Install

```bash
git clone https://github.com/jh-RLI/oep-upload
cd oep-upload
uv venv --python 3.13
source .venv/bin/activate
uv pip install .          # or `uv pip install -e .` for development
```

This installs the `oep-upload` command. Verify with `oep-upload --help`.

> [!tip]
> Config and `.env` are read from **the directory you run `oep-upload` in**, not
> from inside the installed package — so you can run it from any project folder.

---

## Step 2: Organize your data

Put your dataset in a folder with this shape:

```text
my_dataset/
├── datapackage.json        # the OEMetadata description (a datapackage.json)
└── data/
    ├── table_one.csv
    └── table_two.csv
```

Rules that make the upload succeed:

- **Wide format** — one value per cell, **one data type per column** (a column can
  be empty, but never mix e.g. numbers and the text "same as above").
- **Names** — letters, digits and `_` only; keep table and column names under
  50 characters (longer names are auto-truncated).
- **Primary key** — only a column literally named `id` can be the primary key.
  If you don't have one, the OEP adds it automatically.
- **Foreign keys** must reference a column in a table that also exists in your
  datapackage.
- **No `"any"` data types** — that means a column holds mixed types and the upload
  will fail. Fix the column so it has a single type.

Don't have a `datapackage.json` yet? Drop your CSVs under `data/` and the tool's
describe step will infer a starting `datapackage.generated.json` for you. Rename
it to `datapackage.json` and refine it.

---

## Step 3: Configure the tool

There are two things to set: **your token** and **where your data is**.

The fastest way is to scaffold both files in your current folder:

```bash
oep-upload init        # writes settings.local.yaml + .env here
```

Then edit the two files as described below, and verify with:

```bash
oep-upload config      # shows the resolved settings + which files were loaded
```

### 3a. Your API token

`oep-upload init` created a `.env` (or copy `.env.example` to `.env`). Add your token:

```ini
OEP_API_TOKEN=your-token-here
```

Never commit `.env` — it is gitignored.

### 3b. Where your data lives

Settings are layered, lowest to highest precedence:

```text
settings.base.yaml  <  settings.<env>.yaml  <  settings.local.yaml  <  .env / env vars
```

`settings.base.yaml` is shared and stays generic. **Put your own paths in a
`settings.local.yaml` in the folder you run `oep-upload` from** (this is what
`oep-upload init` creates). It is discovered from your working directory and
overrides the packaged defaults — so it works whether you run from a checkout or
an installed package:

```yaml
# ./settings.local.yaml  (in your project folder)
api:
  target: remote        # the public OEP. Use "local" only for a local OEP instance.

paths:
  root: data/my_dataset # the folder that holds your datapackage
  # datapackage_file defaults to nothing; set it if your file isn't auto-detected
  datapackage_file: datapackage.json
```

> [!tip]
> Run `oep-upload config` to confirm your `settings.local.yaml` is picked up
> (look for a ✓ next to it) and to see the resolved target and paths.

Path tips:

- Relative paths are resolved from where you run the tool (your working directory).
- `~` and `$ENV_VARS` are expanded.
- `data_dir` and `datapackage_file` are resolved **relative to `root`**; an
  absolute value simply overrides the parent. In the simple case you only need
  `root`. No need to worry about leading/trailing slashes.

---

## Step 4: Run it

Any of these run the full pipeline (they're equivalent):

```bash
oep-upload                # the installed command (recommended)
python -m oep_upload       # module form
python main.py             # from a checkout
```

Watch the logs. They tell you which target you're uploading to, which datapackage
was selected, the upload order, and how many rows went to each table.

### Options

```bash
# Write the full log to a file (a directory makes one timestamped file per run):
oep-upload --log-file logs/

# Fresh upload — clear each table's existing rows first (default is "append"):
oep-upload --strategy replace

# Verify row counts against your local data after uploading:
oep-upload --strategy replace --verify

# Re-upload only the tables that failed last run (after fixing the data):
oep-upload retry
```

You can also set these in `settings.local.yaml`:

```yaml
app:
  log_file: logs/         # file path appends; a directory = one file per run
upload:
  strategy: replace       # append (default) | replace
  concurrency: 4          # upload N batches in parallel (1 = sequential, default)
  # batch_size: 5000      # rows per request (default 5000)
```

> [!tip] Speeding up slow uploads
> Uploads are network/server-bound. Raising `concurrency` (try 4–8) overlaps the
> per-batch wait and is usually the biggest win. The logs now report per-batch and
> per-table **rows/s** so you can tune by measurement. Don't set concurrency too
> high — the OEP is synchronous with limited workers, so a handful of parallel
> requests is the sweet spot.

> [!warning] Mind the memory
> Roughly **`batch_size × concurrency`** rows (plus their JSON) are held in memory
> at once. The default `batch_size` (5000) is a safe laptop value; only raise it for
> narrow tables and lower it for very wide ones. If you bump both `batch_size` and
> `concurrency`, watch RAM — e.g. `20000 × 4` can be hundreds of MB for wide tables.

> [!warning]
> `replace` deletes all existing rows of each uploaded table before inserting.
> If clearing fails, the upload aborts rather than appending onto old data.

---

## Step 5: Read the logs and fix data issues

The tool automates the mechanics, but it can't fix fundamental data problems.
Most failures come down to:

- **Wrong CSV delimiter** — set the correct `delimiter` per resource in your
  `datapackage.json`.
- **Mixed types in a column** (`"any"` type) — split or clean the column.
- **Invalid names** — see the naming rules in [Step 2](#step-2-organize-your-data).
- **Missing token / permissions** — re-check `.env` and that `api.target` matches
  the OEP instance your token is for.

Fix, re-run `oep-upload`, repeat until it succeeds. Re-runs are safe.

---

## Troubleshooting

**"No OEP API token found for target=..."**
Set `OEP_API_TOKEN` in `.env`. If `api.target: local`, set `OEP_API_TOKEN_LOCAL`
(or it falls back to `OEP_API_TOKEN`).

**"No datapackage found" / "Could not resolve a datapackage"**
Set `paths.root` (and `datapackage_file` if needed) in `settings.local.yaml`, and
make sure the path exists. Run `oep-upload config` to confirm your file is loaded
(✓) and see the resolved paths. You can also point at the file directly via
`OEP_OEM_FILE=path/to/datapackage.json` in `.env`.

**"No local tabular paths found for &lt;table&gt;"**
Each resource in your `datapackage.json` needs a relative `path` to its CSV file,
e.g. `"path": "data/table_one.csv"`.

**"'charmap' codec can't decode byte ..." during the describe step**
This is a CSV encoding mismatch (common on Windows). The tool now automatically
retries by normalizing the file to UTF-8, so it should recover on its own. If a
file still fails, save it as UTF-8 (e.g. "UTF-8" in Excel's *Save As*, or set
`files.encoding` in `settings.local.yaml`). These describe-step errors are
non-fatal: if a valid `datapackage.json` already exists, the upload continues.

**Want more detail in the logs?**
Set `app.log_level: DEBUG` in `settings.local.yaml`, or `LOG_LEVEL=DEBUG` in `.env`.

---

## Use it from Python (library)

You can drive the same pipeline from your own code instead of the CLI.

```python
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

# individual steps are available too:
oep_upload.describe_datapackage()
oep_upload.create_tables()
oep_upload.upload_rows()
oep_upload.upload_metadata(extra_keywords=["my_project"])

# verify the upload (row counts) — great as a pipeline gate:
report = oep_upload.verify()
if not report.ok:
    raise SystemExit(report.format_table())

# after fixing the data, re-upload ONLY the tables that failed last run:
oep_upload.retry()   # defaults to the replace strategy

# inspect the resolved configuration without running anything:
settings = oep_upload.configure(target="remote")
print(settings.api.target, settings.paths.resolved_datapackage_file)
```

> [!important]
> `run()` / `configure()` / the step helpers set up configuration before
> importing the heavy submodules (which read settings at import time). Call one
> of them — or set your environment — **before** importing
> `oep_upload.upload`/`create`/`describe` directly, and configure once per process.
> Values passed to `configure()`/`run()` override `.env` and the YAML files.

---

## Extra: Excel files

If your source is `.xlsx`, convert each sheet to CSV first. This helper needs
`pandas` and `openpyxl`, which aren't installed by default:

```bash
uv pip install pandas openpyxl
python -m oep_upload.tools.xls_to_csv path/to/file.xlsx -o my_dataset/data
```

Then continue from [Step 2](#step-2-organize-your-data).
