# Quick guide: upload to a local (Docker) OEP

A focused, copy‑paste walkthrough for uploading your **preprocessed datapackage**
to a **local OEP instance** running in Docker at `http://localhost:8000`. It starts
from zero (no `.env`, no settings yet) and covers both:

- **Fresh upload** — clear the existing rows and start over (`replace`).
- **Continue** — add to what's already there (`append`, the default).

Two ways to do everything: the **`oep-upload` CLI** and a **Python script**
([example_local_upload.py](example_local_upload.py)).

> [!info] Assumptions
> - Your local OEP is running (Docker) and reachable at `http://localhost:8000`.
> - You have a **local** API token (from your local instance's profile settings).
> - Your data lives in `data/postprocessed/` as `datapackage.json` + `data/*.csv`.
>   Adjust the paths below if yours differ.

---

## 1. Install

```bash
uv venv --python 3.13
source .venv/bin/activate          # Windows: .venv\Scripts\activate
uv pip install -e .                # editable: handy while iterating
```

This gives you the `oep-upload` command. Check it: `oep-upload --help`.

---

## 2. Create config from scratch

```bash
oep-upload init                    # writes settings.local.yaml + .env here
```

**Edit `.env`** — add your **local** token:

```ini
OEP_API_TOKEN_LOCAL=your-local-oep-token
```

**Edit `settings.local.yaml`** — point at the local OEP and your data:

```yaml
api:
  target: local                    # -> http://localhost:8000/api/v0/

paths:
  root: data/postprocessed         # folder with datapackage.json + data/
  datapackage_file: datapackage.json
```

> [!tip] Token resolution
> For `target: local` the tool uses `OEP_API_TOKEN_LOCAL`, falling back to
> `OEP_API_TOKEN` if you only set that one.

---

## 3. Verify your setup before uploading

```bash
oep-upload config
```

You should see something like:

```text
Environment        : prod
Target             : local  ->  http://localhost:8000/api/v0/
API token set      : True
Data root          : /…/data/postprocessed
Datapackage file   : /…/data/postprocessed/datapackage.json
Upload strategy    : append
```

If `Target` is not `localhost:8000`, fix `api.target: local`. If `API token set`
is `False`, fix `.env`.

---

## 4. Upload

### Option A — Fresh upload (clear & restart)

You already have data on the local OEP and want a clean slate. `replace` clears
each table's rows first, then uploads the (possibly fixed) CSVs:

```bash
oep-upload --strategy replace
```

> [!warning] `replace` deletes rows
> It removes all existing rows of each uploaded table before inserting. If
> clearing a table fails, the run aborts instead of appending onto old data.

### Option B — Append / continue

Add to whatever is already there (this is the default):

```bash
oep-upload                         # same as: oep-upload --strategy append
```

> [!note] About "continue"
> A failed batch inserts **nothing** (the OEP rejects it whole), so re-running in
> append mode re-sends the batches that didn't make it. Batches that *did* succeed
> will be re-sent too and can duplicate if a table has a serial `id` — when in
> doubt about duplicates, prefer **Option A (`replace`)** for a clean result.

### Keep a log file

```bash
oep-upload --strategy replace --log-file logs/
```

A directory (`logs/`) creates one timestamped file per run; a file path appends.
Logs include `PHASE:` banners so a single file is easy to scan.

---

## 5. Same thing from Python

Use [example_local_upload.py](example_local_upload.py). Set the paths/token at the
top (or rely on `.env`), then:

```bash
python docs/example_local_upload.py            # append + verify
python docs/example_local_upload.py --replace  # clear & restart, then verify
python docs/example_local_upload.py --verify-only
```

The core of it is just:

```python
import oep_upload

oep_upload.run(
    target="local",
    data_root="data/postprocessed",
    datapackage_file="datapackage.json",
    strategy="replace",            # or "append"
    log_file="logs/",
)
```

### Step by step (drive the phases yourself)

If you'd rather run the phases individually — to inspect/gate between them, or to
re-run only a later step — see [example_step_by_step.py](example_step_by_step.py):

```python
import oep_upload

oep_upload.configure(target="local", data_root="data/postprocessed")
oep_upload.describe_datapackage()   # infer/refresh metadata
oep_upload.create_tables()          # create tables on the OEP
oep_upload.upload_rows()            # stream + upload (pass strategy="replace" for fresh)
oep_upload.upload_metadata()        # per-table metadata
report = oep_upload.verify()        # optional gate
```

Re-running just one step later is the same idea: `configure(...)`, then call only
that function (e.g. `oep_upload.upload_rows(strategy="replace")`).

---

## 6. Verify the upload against your local data

`verify` is a built-in feature — compare each table's row count on the OEP with
what your local CSVs should produce.

**As a CLI step** (e.g. in a data pipeline):

```bash
oep-upload verify           # standalone: just check, no upload
oep-upload --strategy replace --verify   # upload, then verify in one go
```

`verify` exits non-zero if it finds a problem, so a pipeline step fails loudly.

**From Python** (standalone or after a `run()`):

```python
import oep_upload

oep_upload.run(target="local", data_root="data/postprocessed", strategy="replace")

report = oep_upload.verify(target="local", data_root="data/postprocessed")
print(report.format_table())
if not report.ok:
    raise SystemExit("Upload verification failed")
```

Either way you get a per-table comparison of the **expected** count from your
local CSV and the **actual** count on the OEP:

```text
table                                    kind         local~        oep  result
--------------------------------------------------------------------------------------
scalars                                  long           1234       1234  ok
cf_wind_offshore_foundation              long           8761       8761  ok
load                                     wide/pivot   385440     385440  ok
```

How the expected count is derived:

- **long** CSVs (normal header): `rows - 1`.
- **wide/pivot** CSVs (a `from` / `to` / `type` header): the tool pivots them to one
  row per *timestamp × series*, so expected = `data_rows × series`. This is an
  estimate, so a mismatch there is reported as `review`, not a failure.

An `empty` result means the table has 0 rows on the OEP while your CSV has data —
a real problem (see troubleshooting). The `--verify-only` flag of
[example_local_upload.py](example_local_upload.py) does the same via the script.

> [!note] This is a sanity check, not a full diff
> It compares row counts, not values. Value-level spot-checks may come later.

---

## Troubleshooting (local‑specific)

| Symptom | Likely cause / fix |
| --- | --- |
| `Connection refused` / timeouts | The Docker OEP isn't running or isn't on `localhost:8000`. Start it / check the port. |
| `401` / `403` | Wrong or missing **local** token. Set `OEP_API_TOKEN_LOCAL` in `.env`; confirm with `oep-upload config`. |
| `Target` shows the public OEP | `api.target` isn't `local`. Set it in `settings.local.yaml`. |
| `empty` result in verify | Nothing uploaded for that table — check the run's `POST FAILED` log lines (often a leaked header row or a type/delimiter issue). See [usage.md](usage.md#troubleshooting). |
| `replace` errors on clearing | Your token needs **delete** permission on the table. |
| `'charmap' codec…` during describe | CSV encoding mismatch; the tool auto‑retries as UTF‑8. See [usage.md](usage.md#troubleshooting). |

For the general workflow and data rules, see [usage.md](usage.md).
