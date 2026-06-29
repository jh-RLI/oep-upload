from __future__ import annotations

import os
import json
import re
import ast
import gzip
import bz2
import lzma
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.csv as pacsv

import oem2orm
import oem2orm.normalizer

from oep_upload.config import get_settings, export_env_vars
from oep_upload.api.oep import TablesService, OEPApiClient
from oep_upload.config.logging import setup_logging

# =========================
# SETTINGS / CONSTANTS
# =========================
loggi = setup_logging()
_s = get_settings()
export_env_vars(_s)  # keep legacy env-var consumers happy

# Upload behavior
BATCH_SIZE: int = int(_s.upload.batch_size)
DRY_RUN: bool = bool(_s.upload.dry_run)
MAX_RETRIES: int = int(_s.upload.max_retries)
RETRY_BASE_DELAY: float = float(_s.upload.retry_base_delay)
DEFAULT_SCHEMA: str = _s.upload.default_schema
UPLOAD_STRATEGY: str = _s.upload.strategy  # "append" | "replace"
FAILURE_LOG: str = _s.upload.failure_log  # where failed tables are recorded
UPLOAD_CONCURRENCY: int = max(1, int(getattr(_s.upload, "concurrency", 1)))

# Null tokens
NULL_TOKENS: set[str] = set(map(lambda s: s.lower(), _s.upload.null_tokens))

# Paths
ROOT = _s.paths.resolved_root
DATA_ROOT = _s.paths.resolved_data_dir
OEM_FILE = _s.paths.resolved_datapackage_file

# Global override map (filled later)
RESOURCES_BY_TABLE: dict[str, list["Resource"]] = {}
_TABLES = TablesService(OEPApiClient.from_settings())

# Try to mirror the normalizer’s max column length for suffix truncation (safety)
try:
    from oem2orm.normalizer import MAX_COLUMN_LEN as _NORM_MAX_LEN  # type: ignore
except Exception:
    _NORM_MAX_LEN = 50


# =========================
# MODELS
# =========================
@dataclass(slots=True, frozen=True)
class Resource:
    path: str
    delimiter: str | None = None
    encoding: str | None = None
    csv_fields: list[str] | None = None
    db_columns: list[str] | None = None


@dataclass
class TableUploadResult:
    """Outcome of uploading a single table — used for the retry journal."""

    schema: str
    table: str
    uploaded_rows: int = 0
    failed_rows: int = 0
    failed_batches: int = 0
    first_error: str = ""
    csv_paths: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.failed_batches == 0 and not self.first_error


# =========================
# SMALL UTILITIES
# =========================
def normalize_table_key(name: str) -> str:
    name = (name or "").strip()
    bare = name.split(".", 1)[-1]
    return bare.lower()


def split_ident(ident: str, default_schema: str) -> tuple[str, str]:
    ident = ident.strip()
    if "." in ident:
        s, t = ident.split(".", 1)
        return s, t
    return default_schema, ident


def is_url(s: str | None) -> bool:
    return bool(s) and (s.startswith("http://") or s.startswith("https://"))


def looks_tabular_path(path: str | None) -> bool:
    if not path or is_url(path):
        return False
    p = path.lower()
    return p.endswith(".csv") or p.endswith(".tsv")


def guess_delimiter_from_path(path: Path | str | None) -> str:
    if not path:
        return ","
    p = str(path).lower()
    if p.endswith(".tsv"):
        return "\t"
    if p.endswith(".csv"):
        return ","
    return ","


def resolve_csv_path(raw: str) -> Path:
    """
    Resolution:
    - absolute -> use it
    - relative -> DATA_ROOT/<raw>
    """
    raw_path = Path(raw)
    if raw_path.is_absolute():
        return raw_path
    return (DATA_ROOT / raw_path).resolve()


# =========================
# OEMetadata / datapackage
# =========================
def load_oem_resources(oem_path: Path) -> dict[str, list[Resource]]:
    """
    Parse datapackage/OEM file and return:
      { <normalized table>: [Resource(...), ...] }
    Only local CSV/TSV are included. Delimiter falls back to extension.
    """
    with oem_path.open("r", encoding="utf-8") as f:
        meta = json.load(f)

    out: dict[str, list[Resource]] = defaultdict(list)
    resources = meta.get("resources", [])
    if not isinstance(resources, list):
        return {}

    for res in resources:
        if not isinstance(res, dict):
            continue
        table_name = (res.get("name") or "").strip()
        path = (res.get("path") or "").strip()
        if not table_name or not path or not looks_tabular_path(path):
            continue

        # used for delimiter guessing only; actual path resolution later
        full_path = DATA_ROOT / path

        dialect = res.get("dialect") if isinstance(res.get("dialect"), dict) else {}
        if dialect:
            delimiter = dialect.get("delimiter") or guess_delimiter_from_path(full_path)
            encoding = res.get("encoding") or dialect.get("encoding")
        else:
            loggi.warning(
                f"Resource {path} for table {table_name} missing dialect info to read the CSV data. "
                f"Fall back to default encoding {_s.files.encoding} and delimiter: {_s.files.delimiter}. "
                "Set files.delimiter in the config file if needed."
            )
            encoding = _s.files.encoding
            delimiter = _s.files.delimiter

        key = oem2orm.normalizer.TABLE_NORMALIZER(str(table_name))
        fields = res.get("schema", {}).get("fields")
        csv_fields: list[str] | None = None
        if isinstance(fields, list):
            csv_fields = [
                f.get("name") for f in fields if isinstance(f, dict) and f.get("name")
            ]

        out[key].append(
            Resource(
                path=path, delimiter=delimiter, encoding=encoding, csv_fields=csv_fields
            )
        )

    return dict(out)


def find_datapackage() -> Path | None:
    """
    Discovery priority:
      1) env OEP_OEM_FILE (if present)
      2) config.paths.datapackage_file (if provided)
    """
    candidates: list[Path] = []

    env_val = os.environ.get("OEP_OEM_FILE")
    if env_val:
        p = Path(env_val)
        candidates.append(p if p.is_absolute() else (Path.cwd() / p).resolve())

    if OEM_FILE:
        candidates.append(OEM_FILE)

    for c in candidates:
        if c and c.exists():
            return c
    return None


def find_tabulars_in_meta(meta_dict: dict) -> list[Resource]:
    """
    Extract tabular resources from a table's /meta/:
      returns [Resource(...), ...]
    Prefers meta['resources']; falls back to deep-scan.
    """
    results: list[Resource] = []

    res_list = meta_dict.get("resources")
    if isinstance(res_list, list):
        for res in res_list:
            if not isinstance(res, dict):
                continue
            path = (res.get("path") or "").strip()
            if not looks_tabular_path(path):
                continue
            dialect = res.get("dialect") if isinstance(res.get("dialect"), dict) else {}
            delimiter = dialect.get("delimiter") or guess_delimiter_from_path(path)
            encoding = res.get("encoding") or dialect.get("encoding")
            results.append(Resource(path=path, delimiter=delimiter, encoding=encoding))

    if not results:

        def walk(obj: Any) -> None:
            if isinstance(obj, dict):
                p = (obj.get("path") or "").strip() if "path" in obj else ""
                if p and looks_tabular_path(p):
                    results.append(
                        Resource(
                            path=p,
                            delimiter=guess_delimiter_from_path(p),
                            encoding=None,
                        )
                    )
                for v in obj.values():
                    walk(v)
            elif isinstance(obj, list):
                for it in obj:
                    walk(it)

        walk(meta_dict)

    seen: set[str] = set()
    uniq: list[Resource] = []
    for r in results:
        if r.path not in seen:
            uniq.append(r)
            seen.add(r.path)
    return uniq


# =========================
# CSV streaming
# =========================
def _open_binary_any(path: Path):
    """
    Open file as binary, transparently handling common compression formats.
    """
    p = str(path).lower()
    if p.endswith(".gz"):
        return gzip.open(path, "rb")
    if p.endswith(".bz2") or p.endswith(".bzip2"):
        return bz2.open(path, "rb")
    if p.endswith(".xz") or p.endswith(".lzma"):
        return lzma.open(path, "rb")
    return path.open("rb")


def stream_csv_batches(
    csv_path: Path,
    delimiter: str | None,
    encoding: str | None,
    include_columns: list[str],  # kept for signature compatibility
    csv_fields: list[str] | None,
    batch_size: int,
) -> Iterable[list[dict[str, Any]]]:
    """
    Stream CSV as batches of dicts using PyArrow only.
    - Uses auto-generated column names (f0, f1, ...) to avoid duplicate-name collisions.
    - Yields lists of rows sized up to `batch_size` for POSTing.
    """
    delim = delimiter if (isinstance(delimiter, str) and len(delimiter) == 1) else ","
    enc = encoding or "utf8"  # Arrow canonical name

    read_opts = pacsv.ReadOptions(
        block_size=10 << 20,  # 10 MiB
        encoding=enc,
        autogenerate_column_names=True,  # IMPORTANT: no header row; names f0,f1,...
    )
    parse_opts = pacsv.ParseOptions(delimiter=delim, newlines_in_values=True)
    convert_opts = pacsv.ConvertOptions(
        include_columns=None,  # read all; we'll map later by index
        null_values=list(NULL_TOKENS) if NULL_TOKENS else None,
        strings_can_be_null=True,
        timestamp_parsers=["ISO8601"],
    )

    lower = str(csv_path).lower()
    compressed = lower.endswith((".gz", ".bz2", ".bzip2", ".xz", ".lzma"))
    if compressed:
        fbin = _open_binary_any(csv_path)
        source = pa.input_stream(fbin)
    else:
        source = str(csv_path)
        fbin = None

    reader = pacsv.open_csv(
        source,
        read_options=read_opts,
        parse_options=parse_opts,
        convert_options=convert_opts,
    )
    try:
        while True:
            try:
                rb = reader.read_next_batch()
            except StopIteration:
                break
            if rb is None or rb.num_rows == 0:
                break

            rows = rb.to_pylist()  # each row keys: f0, f1, f2, ...

            if batch_size and batch_size > 0 and len(rows) > batch_size:
                for i in range(0, len(rows), batch_size):
                    yield rows[i : i + batch_size]
            else:
                yield rows
    finally:
        try:
            reader.close()
        except Exception:
            pass
        if fbin is not None:
            try:
                fbin.close()
            except Exception:
                pass


# =========================
# PASS-THROUGH MAPPER
# =========================
def _parse_composite_string(s: str) -> Any:
    txt = s.strip()
    if not txt:
        return s
    if (txt[0], txt[-1]) not in {("[", "]"), ("{", "}")}:
        return s
    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        pass
    try:
        val = ast.literal_eval(txt)
        if isinstance(val, (list, dict)):
            return val
    except (ValueError, SyntaxError):
        pass
    return s


def convert_row_passthrough(
    row: dict[str, Any],
    column_names: list[str],
    required_nonnull: set[str],
    serial_pk: bool,
    keymap: dict[str, str] | None = None,  # normalized_db_col -> 'f{i}'
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col in column_names:
        src = keymap.get(col, col) if keymap else col
        v = row.get(src)

        if isinstance(v, str):
            s = v.strip()
            if s.lower() in NULL_TOKENS:
                v = None
            else:
                v = _parse_composite_string(s)

        out[col] = v

    for col in required_nonnull:
        if out.get(col) is None:
            raise ValueError(f"column {col} is NOT NULL but value is missing/NULL")

    if serial_pk and (out.get("id") is None):
        out.pop("id", None)

    return out


# =========================
# CLASSIC HEADER DETECTION
# =========================
def looks_like_classic_header_row(
    first_row: dict[str, Any],
    csv_fields: list[str] | None,
    norm_by_index: list[str] | None,
    db_column_names: list[str],
) -> bool:
    """
    Detect a typical 1-row CSV header (even though Arrow reads it as data).

    Robust against DB normalization/truncation:
      - Prefer raw match vs OEM csv_fields
      - Then normalized match vs norm_by_index (built from csv_fields with unique suffix decisions)
      - Finally fallback: normalized match vs DB column names
    """

    def key_idx(k: str) -> int:
        if k.startswith("f") and k[1:].isdigit():
            return int(k[1:])
        return 10**9

    keys = sorted(first_row.keys(), key=key_idx)
    cells: list[str] = []
    for k in keys:
        v = first_row.get(k)
        cells.append(v.strip() if isinstance(v, str) else "")

    if not cells:
        return False

    nonempty = sum(1 for c in cells if c)
    if nonempty < max(1, int(0.6 * len(cells))):
        return False

    # 1) raw match vs csv_fields
    if csv_fields:
        a = [c.lower() for c in cells[: len(csv_fields)]]
        b = [c.lower() for c in csv_fields]
        eq = sum(1 for x, y in zip(a, b) if x == y)
        if eq >= max(1, int(0.8 * min(len(a), len(b)))):
            return True

    # 2) normalized match vs norm_by_index
    norm = oem2orm.normalizer.COLUMN_NORMALIZER
    norm_cells = [norm(c) for c in cells if c]

    if norm_by_index:
        exp = norm_by_index[: len(norm_cells)]
        eqn = sum(1 for x, y in zip(norm_cells, exp) if x == y)
        if eqn >= max(1, int(0.7 * min(len(norm_cells), len(exp)))):
            return True

    # 3) fallback: normalized match vs DB columns
    db_set = set(db_column_names)
    hit = sum(1 for x in norm_cells if x in db_set)
    if hit >= max(1, int(0.7 * len(norm_cells))):
        return True

    return False


# =========================
# UPLOAD REFACTOR HELPERS
# =========================
@dataclass(slots=True)
class TableShape:
    want_from: str
    want_to: str
    want_type: str
    value_col: str | None
    time_col: str | None
    has_long_shape: bool


@dataclass(slots=True)
class HeaderContext:
    has_multi: bool = False
    skip_count: int = 0
    header_from: list[str] | None = None
    header_to: list[str] | None = None
    header_type: list[str] | None = None
    header_rows_skipped: bool = False


def _get_tabular_resources(
    schema: str, table: str, resources_override: list[Resource] | None
) -> list[Resource]:
    """
    Source priority:
      1) explicit resources_override,
      2) global RESOURCES_BY_TABLE,
      3) /meta/ discovery.
    """
    bare_key = normalize_table_key(table)

    tabulars: list[Resource] = resources_override or []
    if not tabulars and RESOURCES_BY_TABLE:
        tabulars = RESOURCES_BY_TABLE.get(bare_key, [])
    if not tabulars:
        meta = _TABLES.get_table_meta(schema, table)
        tabulars = find_tabulars_in_meta(meta)

    if not tabulars:
        raise RuntimeError(f"No local tabular paths found for {table}")

    return tabulars


def _get_table_columns(
    schema: str, table: str
) -> tuple[list[str], set[str], bool, dict[str, dict]]:
    info = _TABLES.get_table_info(schema, table)
    columns: dict[str, dict] = info["columns"]
    column_names = list(columns.keys())

    required_nonnull: set[str] = {
        c for c, d in columns.items() if not d.get("is_nullable", True)
    }

    serial_pk = False
    if "id" in columns:
        coldef = columns["id"] or {}
        col_default = (coldef.get("column_default") or "").lower()
        if "nextval(" in col_default:
            serial_pk = True
            required_nonnull.discard("id")

    return column_names, required_nonnull, serial_pk, columns


def _infer_shape(column_names: list[str]) -> TableShape:
    db_cols_set = set(column_names)
    norm = oem2orm.normalizer.COLUMN_NORMALIZER

    want_from = norm("from")
    want_to = norm("to")
    want_type = norm("type")

    cand_values = [norm("value"), norm("amount"), norm("val")]
    value_col = next((c for c in cand_values if c in db_cols_set), None)

    has_long_shape = (
        want_from in db_cols_set and want_to in db_cols_set and value_col is not None
    )

    cand_time = [norm("time"), norm("timestamp"), norm("datetime"), norm("ts")]
    time_col = next((c for c in cand_time if c in db_cols_set), None)

    return TableShape(
        want_from=want_from,
        want_to=want_to,
        want_type=want_type,
        value_col=value_col,
        time_col=time_col,
        has_long_shape=has_long_shape,
    )


def _precompute_norm_by_index(csv_fields: list[str] | None) -> list[str]:
    if not csv_fields:
        return []
    orig_to_norm = oem2orm.normalizer.build_unique_column_map(
        csv_fields,
        normalizer=oem2orm.normalizer.COLUMN_NORMALIZER,
    )
    return [orig_to_norm[o] for o in csv_fields]


def _detect_multirow_header(raw_batch: list[dict[str, Any]]) -> HeaderContext:
    ctx = HeaderContext()
    if not raw_batch:
        return ctx

    header_candidates = raw_batch[:3]

    def _cell(row: dict[str, Any], key: str) -> str:
        v = row.get(key)
        return v if isinstance(v, str) else ""

    if (
        len(header_candidates) >= 2
        and _cell(header_candidates[0], "f0").strip().lower() == "from"
        and _cell(header_candidates[1], "f0").strip().lower() == "to"
    ):
        ctx.has_multi = True
        ctx.header_from = []
        ctx.header_to = []
        ctx.header_type = []

        ncols = len(header_candidates[0].keys())
        for j in range(1, ncols):
            fj = f"f{j}"
            ctx.header_from.append(_cell(header_candidates[0], fj))
            ctx.header_to.append(_cell(header_candidates[1], fj))
            if (
                len(header_candidates) >= 3
                and _cell(header_candidates[2], "f0").strip().lower() == "type"
            ):
                ctx.header_type.append(_cell(header_candidates[2], fj))
            else:
                ctx.header_type.append("")

        ctx.skip_count = 2
        if (
            len(header_candidates) >= 3
            and _cell(header_candidates[2], "f0").strip().lower() == "type"
        ):
            ctx.skip_count = 3

    return ctx


def _build_keymap_for_wide(
    column_names: list[str],
    norm_by_index: list[str],
    *,
    data_offset: int,
) -> dict[str, str]:
    """
    Map DB col -> f{i} by position.

    data_offset:
      - 0 for classic wide files where first data column is f0
      - 1 for multi-row header wide fallback where first data column is f1
    """
    keymap: dict[str, str] = {}

    if norm_by_index:
        for i, norm_name in enumerate(norm_by_index, start=data_offset):
            if norm_name in column_names:
                keymap[norm_name] = f"f{i}"
    else:
        for i, col in enumerate(column_names, start=data_offset):
            keymap[col] = f"f{i}"

    for col in column_names:
        keymap.setdefault(col, col)

    return keymap


def _skip_headers_if_needed(
    raw_batch: list[dict[str, Any]],
    ctx: HeaderContext,
    *,
    csv_fields: list[str] | None,
    norm_by_index: list[str],
    column_names: list[str],
) -> list[dict[str, Any]]:
    if ctx.header_rows_skipped or not raw_batch:
        return raw_batch

    if ctx.has_multi:
        raw_batch = raw_batch[ctx.skip_count :]
        ctx.header_rows_skipped = True
        return raw_batch

    # classic 1-row header
    if looks_like_classic_header_row(
        raw_batch[0],
        csv_fields=csv_fields if csv_fields else None,
        norm_by_index=norm_by_index if norm_by_index else None,
        db_column_names=column_names,
    ):
        raw_batch = raw_batch[1:]

    ctx.header_rows_skipped = True
    return raw_batch


def _emit_long_rows(
    raw_batch: list[dict[str, Any]],
    ctx: HeaderContext,
    shape: TableShape,
    *,
    db_cols_set: set[str],
) -> list[dict[str, Any]]:
    if (
        not ctx.has_multi
        or not shape.has_long_shape
        or not ctx.header_from
        or not shape.value_col
    ):
        return []

    out_rows: list[dict[str, Any]] = []
    header_from = ctx.header_from
    header_to = ctx.header_to or [""] * len(header_from)
    header_type = ctx.header_type or [""] * len(header_from)

    for r in raw_batch:
        ts_val = r.get("f0")
        if ts_val in (None, ""):
            continue

        for j in range(1, 1 + len(header_from)):
            fj = f"f{j}"
            val = r.get(fj)

            newrow: dict[str, Any] = {}
            if shape.time_col:
                newrow[shape.time_col] = ts_val

            newrow[shape.want_from] = (
                header_from[j - 1] if j - 1 < len(header_from) else None
            )
            newrow[shape.want_to] = header_to[j - 1] if j - 1 < len(header_to) else None
            if shape.want_type in db_cols_set:
                newrow[shape.want_type] = (
                    header_type[j - 1] if j - 1 < len(header_type) else None
                )

            newrow[shape.value_col] = val
            out_rows.append(newrow)

    return out_rows


def _map_wide_rows(
    raw_batch: list[dict[str, Any]],
    column_names: list[str],
    required_nonnull: set[str],
    serial_pk: bool,
    keymap: dict[str, str],
    csv_path: Path,
) -> list[dict[str, Any]]:
    batch: list[dict[str, Any]] = []
    for raw_row in raw_batch:
        try:
            mapped = convert_row_passthrough(
                raw_row, column_names, required_nonnull, serial_pk, keymap
            )
        except Exception as e:
            snippet = {k: raw_row.get(k) for k in list(raw_row.keys())[:10]}
            raise RuntimeError(
                f"Row conversion error in {csv_path.name}: {e}\n"
                f"Row head: {json.dumps(snippet, ensure_ascii=False)[:300]}..."
            ) from e
        batch.append(mapped)
    return batch


_TEXTISH_PG_TYPES = ("char", "text", "string", "json", "uuid", "bytea", "name", "enum")


def _pg_type_of(coldef: dict | None) -> str:
    if not coldef:
        return ""
    t = coldef.get("udt_name") or coldef.get("data_type") or coldef.get("type") or ""
    return str(t).strip().lower()


def _is_textish_type(pg_type: str) -> bool:
    return (not pg_type) or any(t in pg_type for t in _TEXTISH_PG_TYPES)


def _looks_like_header_row(row: dict[str, Any], coldefs: dict[str, dict]) -> bool:
    """Type-aware check: does this mapped row look like a leaked CSV header?

    A header row places column *names* into every column. The tell-tale sign
    that survives the other heuristics is a **non-text** column (timestamp,
    numeric, boolean, ...) holding a string equal to a column name — real data
    never does that. Designed for near-zero false positives.
    """
    col_name_set = {c.lower() for c in coldefs}
    for col, val in row.items():
        if not isinstance(val, str):
            continue
        s = val.strip()
        if not s:
            continue
        if _is_textish_type(_pg_type_of(coldefs.get(col))):
            continue
        # A typed (non-text) column holding text that names a column → header.
        if s.lower() in col_name_set or s.lower() == col.lower():
            return True
    return False


def _post_rows(schema: str, table: str, rows: list[dict[str, Any]]) -> tuple[bool, str]:
    """POST a batch. Returns (ok, detail); detail is a short error on failure."""
    if DRY_RUN:
        print(f"DRY_RUN: would POST {len(rows)} rows")
        return True, ""

    t0 = time.perf_counter()
    status, payload = _TABLES.post_rows(schema, table, rows)
    dt = time.perf_counter() - t0
    if status in (200, 201, 202):
        rate = (len(rows) / dt) if dt > 0 else 0.0
        loggi.info(
            "Uploaded %d rows -> status %s in %.1fs (%.0f rows/s)",
            len(rows),
            status,
            dt,
            rate,
        )
        return True, ""

    # Surface a sample of the offending row so the cause (e.g. a leaked header
    # row, a type mismatch) is visible. The OEP rejects the whole batch on one
    # bad row, so these rows were NOT uploaded.
    sample = rows[0] if rows else {}
    snippet = {k: sample.get(k) for k in list(sample.keys())[:8]}
    loggi.error(
        "POST FAILED for %s.%s: status %s — %d rows in this batch were NOT uploaded.\n"
        "  reason: %s\n  first row of batch: %s",
        schema,
        table,
        status,
        len(rows),
        payload,
        json.dumps(snippet, ensure_ascii=False, default=str)[:400],
    )
    return False, f"{status}: {str(payload)[:200]}"


# =========================
# BATCH PRODUCER + POSTERS
# =========================
def _id_missing_message(
    table: str,
    serial_pk: bool,
    column_names: list[str],
    sample_row: dict[str, Any] | None,
) -> str | None:
    """Warning text if the table auto-generates `id` but the data has none.

    `convert_row_passthrough` drops a missing/empty `id` for a serial primary
    key, so an absent `id` in a mapped row means the OEP will assign a new one.
    Returns None when there is nothing to warn about.
    """
    if not (serial_pk and "id" in column_names):
        return None
    if not sample_row or "id" in sample_row:
        return None
    extra = (
        " (and their order is non-deterministic with concurrency > 1)"
        if UPLOAD_CONCURRENCY > 1
        else ""
    )
    return (
        f"{table}: the data provides no 'id' values but the table has an "
        f"auto-generated 'id' (serial primary key) — the OEP will assign NEW ids, "
        f"so your local ids are NOT preserved{extra}."
    )


def _iter_ready_batches(
    table: str,
    tabulars: list[Resource],
    *,
    column_names: list[str],
    required_nonnull: set[str],
    serial_pk: bool,
    shape: "TableShape",
    coldefs: dict[str, dict],
    db_cols_set: set[str],
) -> Iterable[list[dict[str, Any]]]:
    """Yield batches of rows ready to POST (header detection + pivot/wide mapping).

    This is the CPU/producer side; posting happens in the caller so it can run
    sequentially or in parallel without changing this logic.
    """
    id_warned = False

    def _warn_id(rows: list[dict[str, Any]]) -> None:
        nonlocal id_warned
        if id_warned:
            return
        msg = _id_missing_message(table, serial_pk, column_names, rows[0] if rows else None)
        if msg:
            loggi.warning(msg)
        id_warned = True

    for res in tabulars:
        csv_path = resolve_csv_path(res.path)
        csv_fields = res.csv_fields or []

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found: {csv_path}")

        print(
            f"\nProcessing {table} from {csv_path} "
            f"(delimiter='{res.delimiter or ','}', encoding='{res.encoding or 'utf-8'}')"
        )

        norm_by_index = _precompute_norm_by_index(csv_fields)

        keymap: dict[str, str] | None = None
        header_ctx = HeaderContext()
        wide_header_checked = False

        for raw_batch in stream_csv_batches(
            csv_path,
            res.delimiter,
            res.encoding,
            column_names,
            csv_fields,
            BATCH_SIZE,
        ):
            if keymap is None:
                header_ctx = _detect_multirow_header(raw_batch)

                # wide mapping: multi-header fallback usually starts at f1
                if header_ctx.has_multi and not shape.has_long_shape:
                    keymap = _build_keymap_for_wide(
                        column_names, norm_by_index, data_offset=1
                    )
                elif not header_ctx.has_multi:
                    keymap = _build_keymap_for_wide(
                        column_names, norm_by_index, data_offset=0
                    )
                else:
                    # pivot mode: keymap unused, but keep a value
                    keymap = {}

            raw_batch = _skip_headers_if_needed(
                raw_batch,
                header_ctx,
                csv_fields=csv_fields or None,
                norm_by_index=norm_by_index,
                column_names=column_names,
            )
            if not raw_batch:
                continue

            # LONG PIVOT
            if header_ctx.has_multi and shape.has_long_shape:
                out_rows = _emit_long_rows(
                    raw_batch, header_ctx, shape, db_cols_set=db_cols_set
                )
                if out_rows:
                    _warn_id(out_rows)
                    yield out_rows
                continue

            # WIDE
            assert keymap is not None
            batch = _map_wide_rows(
                raw_batch, column_names, required_nonnull, serial_pk, keymap, csv_path
            )
            if not batch:
                continue

            # Safety net: if the upstream header heuristics missed a leading
            # header row, drop it here (type-aware) before it poisons the whole
            # batch — the OEP rejects the entire batch on a single bad row.
            if not wide_header_checked:
                wide_header_checked = True
                if _looks_like_header_row(batch[0], coldefs):
                    loggi.warning(
                        "Dropping a leading header-like row in %s "
                        "(a column name appeared in a non-text column).",
                        csv_path.name,
                    )
                    batch = batch[1:]

            if batch:
                _warn_id(batch)
                yield batch


def _post_batches_sequential(
    schema: str, table: str, batches: Iterable[list[dict[str, Any]]]
) -> tuple[int, int, int, str]:
    uploaded = failed_rows = failed_batches = 0
    first_error = ""
    for batch in batches:
        ok, detail = _post_rows(schema, table, batch)
        if ok:
            uploaded += len(batch)
        else:
            failed_rows += len(batch)
            failed_batches += 1
            first_error = first_error or detail
    return uploaded, failed_rows, failed_batches, first_error


def _post_batches_concurrent(
    schema: str,
    table: str,
    batches: Iterable[list[dict[str, Any]]],
    concurrency: int,
) -> tuple[int, int, int, str]:
    """POST batches with up to `concurrency` requests in flight at once.

    The producer (mapping) runs in this thread; only the HTTP POST runs in
    workers, so preparing the next batch overlaps waiting on the previous one.
    """
    uploaded = failed_rows = failed_batches = 0
    first_error = ""
    inflight: set = set()

    def _do(rows: list[dict[str, Any]]):
        ok, detail = _post_rows(schema, table, rows)
        return ok, detail, len(rows)

    def _consume(futs):
        nonlocal uploaded, failed_rows, failed_batches, first_error
        for fut in futs:
            ok, detail, n = fut.result()
            if ok:
                uploaded += n
            else:
                failed_rows += n
                failed_batches += 1
                first_error = first_error or detail

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        for batch in batches:
            inflight.add(ex.submit(_do, batch))
            if len(inflight) >= concurrency:
                done, inflight = wait(inflight, return_when=FIRST_COMPLETED)
                _consume(done)
        if inflight:
            done, _ = wait(inflight)
            _consume(done)

    return uploaded, failed_rows, failed_batches, first_error


# =========================
# UPLOAD (per table) - REFACTORED
# =========================
def upload_table(
    schema: str,
    table: str,
    resources_override: list[Resource] | None = None,
    *,
    strategy: str | None = None,
) -> TableUploadResult:
    effective_strategy = strategy or UPLOAD_STRATEGY
    result = TableUploadResult(schema=schema, table=table)

    tabulars = _get_tabular_resources(schema, table, resources_override)
    result.csv_paths = [t.path for t in tabulars]
    column_names, required_nonnull, serial_pk, coldefs = _get_table_columns(
        schema, table
    )
    shape = _infer_shape(column_names)
    db_cols_set = set(column_names)

    # "replace" strategy: clear existing rows once before uploading, so the
    # result is a fresh upload. Abort the table if clearing fails, rather than
    # silently appending onto stale data.
    if effective_strategy == "replace":
        if DRY_RUN:
            print(f"DRY_RUN: would clear all rows of {schema}.{table} (replace)")
        else:
            loggi.info("Strategy 'replace': clearing existing rows of %s ...", table)
            status, payload = _TABLES.delete_all_rows(schema, table)
            if status not in (200, 201, 202, 204):
                raise RuntimeError(
                    f"Could not clear table '{table}' for a replace upload: "
                    f"{status} {payload}"
                )
            loggi.info("Cleared existing rows of %s (status %s).", table, status)

    if len(tabulars) > 1:
        print(
            f"Note: multiple tabular paths found for {table}: {[t.path for t in tabulars]}"
        )

    batches = _iter_ready_batches(
        table,
        tabulars,
        column_names=column_names,
        required_nonnull=required_nonnull,
        serial_pk=serial_pk,
        shape=shape,
        coldefs=coldefs,
        db_cols_set=db_cols_set,
    )

    t0 = time.perf_counter()
    if UPLOAD_CONCURRENCY > 1 and not DRY_RUN:
        uploaded_rows, failed_rows, failed_batches, first_error = (
            _post_batches_concurrent(schema, table, batches, UPLOAD_CONCURRENCY)
        )
    else:
        uploaded_rows, failed_rows, failed_batches, first_error = (
            _post_batches_sequential(schema, table, batches)
        )
    elapsed = time.perf_counter() - t0
    rate = (uploaded_rows / elapsed) if elapsed > 0 else 0.0

    result.uploaded_rows = uploaded_rows
    result.failed_rows = failed_rows
    result.failed_batches = failed_batches
    result.first_error = first_error

    if failed_batches:
        loggi.error(
            "Done: %s — %d rows uploaded in %.1fs (%.0f rows/s, concurrency=%d), "
            "but %d batch(es) (%d rows) FAILED. See the 'POST FAILED' messages above.",
            table,
            uploaded_rows,
            elapsed,
            rate,
            UPLOAD_CONCURRENCY,
            failed_batches,
            failed_rows,
        )
    else:
        loggi.info(
            "Done: %s uploaded %d rows in %.1fs (%.0f rows/s, concurrency=%d).",
            table,
            uploaded_rows,
            elapsed,
            rate,
            UPLOAD_CONCURRENCY,
        )

    return result


# =========================
# FK RESOLUTION (parents-first)
# =========================
FK_DEF_RE = re.compile(
    r"FOREIGN KEY \((?P<local_cols>[^)]+)\)\s+REFERENCES\s+(?P<ref_schema>\w+)\.(?P<ref_table>\w+)\s*\((?P<ref_cols>[^)]+)\)",
    re.IGNORECASE,
)


def fk_parents_for_table(schema: str, table: str) -> set[str]:
    info = _TABLES.get_table_info(schema, table)
    parents: set[str] = set()
    for _, c in (info.get("constraints") or {}).items():
        if (c.get("constraint_type") or "").upper() == "FOREIGN KEY":
            m = FK_DEF_RE.search(c.get("definition") or "")
            if not m:
                continue
            ref_schema = m.group("ref_schema")
            ref_table = m.group("ref_table")
            parents.add(f"{ref_schema}.{ref_table}")
    return parents


def topo_sort_tables(idents: list[str], default_schema: str) -> list[str]:
    nodes_fq = []
    for ident in idents:
        s, t = split_ident(ident, default_schema)
        nodes_fq.append(f"{s}.{t}")

    parents_map = {n: fk_parents_for_table(*n.split(".", 1)) for n in nodes_fq}

    node_set = set(nodes_fq)
    edges = {
        n: {p for p in parents if p in node_set} for n, parents in parents_map.items()
    }

    indeg = {n: 0 for n in nodes_fq}
    for n in nodes_fq:
        indeg[n] = len(edges[n])

    q = deque([n for n, d in indeg.items() if d == 0])
    order_fq: list[str] = []
    while q:
        n = q.popleft()
        order_fq.append(n)
        for m in nodes_fq:
            if n in edges[m]:
                indeg[m] -= 1
                if indeg[m] == 0:
                    q.append(m)

    if len(order_fq) != len(nodes_fq):
        cycle = [n for n in nodes_fq if indeg[n] > 0]
        raise RuntimeError(f"FK cycle or missing external parents among: {cycle}")

    return [n.split(".", 1)[1] for n in order_fq]


def upload_tables_in_fk_order(
    idents: list[str],
    default_schema: str,
    resources_by_table: dict[str, list[Resource]] | None = None,
    *,
    strategy: str | None = None,
) -> list[TableUploadResult]:
    ordered_tables = topo_sort_tables(idents, default_schema)
    print("Upload order (parents -> children):", " -> ".join(ordered_tables))

    global RESOURCES_BY_TABLE
    raw_map = resources_by_table or {}
    RESOURCES_BY_TABLE = {normalize_table_key(k): v for k, v in raw_map.items()}

    if RESOURCES_BY_TABLE:
        print(f"[override keys] {sorted(RESOURCES_BY_TABLE.keys())}")

    results: list[TableUploadResult] = []
    for t in ordered_tables:
        schema, table = split_ident(t, default_schema)
        override = RESOURCES_BY_TABLE.get(normalize_table_key(table), [])
        print(
            f"[upload] table='{table}' schema='{schema}' override_rows={len(override)}"
        )
        try:
            results.append(
                upload_table(
                    schema, table, resources_override=override, strategy=strategy
                )
            )
        except Exception as e:  # noqa: BLE001 - record and keep going to the next table
            loggi.exception("Upload aborted for %s.%s: %s", schema, table, e)
            results.append(
                TableUploadResult(
                    schema=schema,
                    table=table,
                    failed_batches=1,
                    first_error=str(e)[:200],
                )
            )
    return results


# =========================
# Failure journal (for retry)
# =========================
def _journal_path() -> Path:
    raw = FAILURE_LOG or ".oep-upload/last-run.json"
    p = Path(raw).expanduser()
    return p if p.is_absolute() else (Path.cwd() / p)


def _write_failure_journal(results: list[TableUploadResult]) -> None:
    """Record failed tables so `retry` can re-upload only those.

    On a fully successful run, remove any stale journal so retry has nothing
    to do.
    """
    path = _journal_path()
    failed = [r for r in results if not r.ok]
    if not failed:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError as e:  # noqa: BLE001
            loggi.debug("Could not remove stale failure journal %s: %s", path, e)
        return

    payload = {
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "target": getattr(_s.api, "target", None),
        "failed": [
            {
                "schema": r.schema,
                "table": r.table,
                "csv": r.csv_paths,
                "failed_batches": r.failed_batches,
                "failed_rows": r.failed_rows,
                "first_error": r.first_error,
            }
            for r in failed
        ],
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        loggi.warning(
            "Recorded %d failed table(s) in %s — run 'oep-upload retry' to re-upload them.",
            len(failed),
            path,
        )
    except OSError as e:  # noqa: BLE001
        loggi.warning("Could not write failure journal %s: %s", path, e)


def _read_failure_journal() -> dict | None:
    path = _journal_path()
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        loggi.warning("Could not read failure journal %s: %s", path, e)
        return None


# =========================
# Entrypoint helpers
# =========================
def upload_tabular_data() -> list[TableUploadResult]:
    """
    Convenience entry: read datapackage from config/env,
    derive tables, and upload in FK order. Records any failures for `retry`.
    """
    oem_path = find_datapackage()
    if oem_path and oem_path.exists():
        resources_by_table = load_oem_resources(oem_path)
        tables_input = list(resources_by_table.keys())
        print(f"Found {len(tables_input)} tables in OEM file: {oem_path}")
    else:
        raise SystemExit(
            "No datapackage found. Set 'paths.datapackage_file' or OEP_OEM_FILE."
        )

    if not tables_input:
        raise SystemExit("No tables to upload. Check your OEM file.")

    results = upload_tables_in_fk_order(
        tables_input, DEFAULT_SCHEMA, resources_by_table
    )
    _write_failure_journal(results)
    return results


def retry_failed_uploads(*, strategy: str = "replace") -> list[TableUploadResult]:
    """Re-upload only the tables recorded as failed in the failure journal.

    Defaults to the ``replace`` strategy (clear each failed table first) so the
    retry is a clean re-upload rather than appending onto a partial one. The
    journal is rewritten afterwards with whatever still fails, so you can
    iterate: fix -> retry -> fix.
    """
    journal = _read_failure_journal()
    if not journal or not journal.get("failed"):
        loggi.info("Nothing to retry — no failed tables recorded in %s.", _journal_path())
        return []

    failed_tables = [f.get("table") for f in journal["failed"] if f.get("table")]
    loggi.info(
        "Retrying %d failed table(s) with strategy='%s': %s",
        len(failed_tables),
        strategy,
        ", ".join(failed_tables),
    )

    oem_path = find_datapackage()
    if not (oem_path and oem_path.exists()):
        raise SystemExit(
            "No datapackage found. Set 'paths.datapackage_file' or OEP_OEM_FILE."
        )
    resources_by_table = load_oem_resources(oem_path)

    wanted = {normalize_table_key(t) for t in failed_tables}
    subset = {
        k: v
        for k, v in resources_by_table.items()
        if normalize_table_key(k) in wanted
    }
    if not subset:
        loggi.warning(
            "None of the failed tables were found in the datapackage; nothing to retry."
        )
        return []

    results = upload_tables_in_fk_order(
        list(subset.keys()), DEFAULT_SCHEMA, subset, strategy=strategy
    )
    _write_failure_journal(results)  # rewrite with whatever still failed
    return results
