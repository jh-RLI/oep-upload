# Using oep-upload

A step-by-step guide to publish your tabular data and metadata to the
[Open Energy Platform](https://openenergyplatform.org) (OEP).

> **In one sentence:** point the tool at a folder of CSV files described by a
> `datapackage.json`, run it once, and it creates the tables on the OEP, uploads
> the rows, and uploads the metadata.

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
uv pip install .
```

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

### 3a. Your API token

Copy the example env file and add your token:

```bash
cp .env.example .env
```

Then edit `.env`:

```ini
OEP_API_TOKEN=your-token-here
```

Never commit `.env` — it is gitignored.

### 3b. Where your data lives

Settings are layered, lowest to highest precedence:

```text
settings.base.yaml  <  settings.<env>.yaml  <  settings.local.yaml  <  .env / env vars
```

`settings.base.yaml` is shared and stays generic. **Put your own paths in
`settings.local.yaml`** (create it next to the others under
`src/oep_upload/config/`). It is gitignored, so your machine-specific paths are
never committed:

```yaml
# src/oep_upload/config/settings.local.yaml
api:
  target: remote        # the public OEP. Use "local" only for a local OEP instance.

paths:
  root: data/my_dataset # the folder that holds your datapackage
  # datapackage_file defaults to nothing; set it if your file isn't auto-detected
  datapackage_file: datapackage.json
```

Path tips:

- Relative paths are resolved from where you run the tool (usually the repo root).
- `~` and `$ENV_VARS` are expanded.
- `data_dir` and `datapackage_file` are resolved **relative to `root`**; an
  absolute value simply overrides the parent. In the simple case you only need
  `root`. No need to worry about leading/trailing slashes.

---

## Step 4: Run it

```bash
python main.py
```

Watch the logs. They tell you which target you're uploading to, which datapackage
was selected, the upload order, and how many rows went to each table.

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

Fix, re-run `python main.py`, repeat until it succeeds. Re-runs are safe.

---

## Troubleshooting

**"No OEP API token found for target=..."**
Set `OEP_API_TOKEN` in `.env`. If `api.target: local`, set `OEP_API_TOKEN_LOCAL`
(or it falls back to `OEP_API_TOKEN`).

**"No datapackage found" / "Could not resolve a datapackage"**
Set `paths.root` (and `datapackage_file` if needed) in `settings.local.yaml`, and
make sure the path exists.

**"No local tabular paths found for &lt;table&gt;"**
Each resource in your `datapackage.json` needs a relative `path` to its CSV file,
e.g. `"path": "data/table_one.csv"`.

**Want more detail in the logs?**
Set `app.log_level: DEBUG` in `settings.local.yaml`, or `LOG_LEVEL=DEBUG` in `.env`.

---

## Extra: Excel files

If your source is `.xlsx`, convert each sheet to CSV first. This helper needs
`pandas` and `openpyxl`, which aren't installed by default:

```bash
uv pip install pandas openpyxl
python -m oep_upload.tools.xls_to_csv path/to/file.xlsx -o my_dataset/data
```

Then continue from [Step 2](#step-2-organize-your-data).