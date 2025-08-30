#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
import re
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import requests


import ast


# =========================
# CONFIG
# =========================
LOCAL_OEP_URL = "http://127.0.0.1:8000"
PROD_OEP_URL = "https://openenergyplatform.org"
BASE_URL = LOCAL_OEP_URL  # switch to PROD_OEP_URL when you want
REST_API = "/api/v0"
TOKEN = ""  # put your token here
HEADER = {"Authorization": f"Token {TOKEN}"}
DEFAULT_SCHEMA = "model_draft"

# All CSV paths are resolved relative to this folder
BASE_ROOT = Path("./SLE_data_publication/data/preprocessed/")
DATA_ROOT = BASE_ROOT / "data"
OEM_FILE = BASE_ROOT / "datapackage.json"

BATCH_SIZE = 500
DRY_RUN = False
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.5

NULL_TOKENS: set[str] = {"", "null", "none", "na", "nan", "n/a"}

# Global override map (filled in main)
RESOURCES_BY_TABLE: dict[str, list["Resource"]] = {}


# =========================
# MODELS
# =========================
@dataclass(slots=True, frozen=True)
class Resource:
    path: str
    delimiter: str | None = None
    encoding: str | None = None


# =========================
# SMALL UTILITIES
# =========================
def normalize_table_key(name: str) -> str:
    """
    Normalize any table identifier to a matching key:
    - strip whitespace
    - drop schema (keep bare)
    - lowercase
    """
    name = (name or "").strip()
    bare = name.split(".", 1)[-1]
    return bare.lower()


def split_ident(ident: str, default_schema: str) -> tuple[str, str]:
    """Return (schema, table) from 'schema.table' or 'table' (uses default_schema)."""
    ident = ident.strip()
    if "." in ident:
        s, t = ident.split(".", 1)
        return s, t
    return default_schema, ident


def is_url(s: str | None) -> bool:
    return bool(s) and (s.startswith("http://") or s.startswith("https://"))


def looks_tabular_path(path: str | None) -> bool:
    """Accept local CSV/TSV purely by extension; ignore 'format' fields."""
    if not path or is_url(path):
        return False
    p = path.lower()
    return p.endswith(".csv") or p.endswith(".tsv")


def guess_delimiter_from_path(path: str | None) -> str:
    if not path:
        return ","
    p = path.lower()
    if p.endswith(".tsv"):
        return "\t"
    if p.endswith(".csv"):
        return ","
    return ","


def resolve_csv_path(raw: str) -> Path:
    """
    Robust path resolution:
    - If absolute and exists -> use it.
    - If absolute but missing -> try DATA_ROOT/<raw.lstrip('/')>.
    - If relative -> DATA_ROOT/<raw>.
    """
    raw_path = Path(raw)
    if raw_path.is_absolute():
        return (
            raw_path
            if raw_path.exists()
            else (DATA_ROOT / raw_path.name if raw_path.name else DATA_ROOT)
        )
    return (DATA_ROOT / raw_path).resolve()


# =========================
# API
# =========================
def api_get(url: str) -> dict:
    r = requests.get(url, headers=HEADER, timeout=60)
    r.raise_for_status()
    return r.json()


def post_rows(schema: str, table: str, rows: list[dict]) -> tuple[int, dict]:
    url = f"{BASE_URL}{REST_API}/schema/{schema}/tables/{table}/rows/new"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, headers=HEADER, json={"query": rows}, timeout=120)
            status = resp.status_code
            try:
                payload = resp.json()
            except Exception:
                payload = {"raw": resp.text}
            if status >= 500:
                raise RuntimeError(f"Server {status}: {payload}")
            return status, payload
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            delay = RETRY_BASE_DELAY**attempt
            print(f"POST retry {attempt}/{MAX_RETRIES} in {delay:.1f}s due to: {e}")
            time.sleep(delay)


def get_table_info(schema: str, table: str) -> dict:
    return api_get(f"{BASE_URL}{REST_API}/schema/{schema}/tables/{table}/")


def get_table_meta(schema: str, table: str) -> dict:
    return api_get(f"{BASE_URL}{REST_API}/schema/{schema}/tables/{table}/meta/")


# =========================
# OEMetadata / datapackage
# =========================
def load_oem_resources(oem_path: Path) -> dict[str, list[Resource]]:
    """
    Parse datapackage/OEM file and return:
      { <normalized bare table>: [Resource(...), ...] }
    Only local CSV/TSV (by extension) are included. Delimiter falls back to extension.
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

        dialect = res.get("dialect") if isinstance(res.get("dialect"), dict) else {}
        delimiter = dialect.get("delimiter") or guess_delimiter_from_path(path)
        encoding = res.get("encoding") or dialect.get("encoding")

        key = normalize_table_key(table_name)
        out[key].append(Resource(path=path, delimiter=delimiter, encoding=encoding))

    return dict(out)


def find_datapackage() -> Path | None:
    """
    Discover datapackage.json via:
      1) env OEP_OEM_FILE
      2) CWD/datapackage.json
      3) script_dir/datapackage.json
      4) script_dir.parent/datapackage.json
      5) DATA_ROOT/datapackage.json (legacy default)
    Returns the first that exists, else None.
    """

    # candidates: list[Path] = []
    # cwd = Path.cwd()
    # script_dir = Path(__file__).resolve().parent

    # candidates.append(cwd / "datapackage.json")
    # candidates.append(script_dir / "datapackage.json")
    # candidates.append(script_dir.parent / "datapackage.json")
    # candidates.append(DATA_ROOT / "datapackage.json")

    if OEM_FILE.exists():
        return OEM_FILE
    return None


def find_tabulars_in_meta(meta_dict: dict) -> list[Resource]:
    """
    Extract tabular resources from a table's /meta/:
      returns [Resource(...), ...]
    Prefers meta['resources']; falls back to deep-scan for any local *.csv/*.tsv path.
    """
    results: list[Resource] = []

    # Prefer explicit resources list
    res_list = meta_dict["resources"]
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

    # Deep-scan if nothing found
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

    # Dedup by path (preserve order)
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
def stream_csv_rows(
    csv_path: Path, delimiter: str | None, encoding: str | None
) -> Iterable[dict[str, Any]]:
    enc = encoding or "utf-8-sig"
    delim = delimiter if (isinstance(delimiter, str) and len(delimiter) == 1) else ","
    with csv_path.open("r", newline="", encoding=enc) as f:
        reader = csv.DictReader(f, delimiter=delim)
        for i, row in enumerate(reader, 1):
            yield row  # we don't need the line number anymore


# =========================
# PASS-THROUGH MAPPER
# =========================

# e.g. NULL_TOKENS = {"", "null", "none", "na", "n/a", "nan"}
# make sure it's defined somewhere above


def _parse_composite_string(s: str) -> Any:
    """
    If s looks like a list or dict written as a string, parse it.
    Tries JSON first, then ast.literal_eval. Returns original string on failure.
    """
    txt = s.strip()
    if not txt:
        return s
    if (txt[0], txt[-1]) not in {("[", "]"), ("{", "}")}:
        return s
    # Try strict JSON first
    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        pass
    # Fall back to safe Python literal evaluation (handles single quotes)
    try:
        val = ast.literal_eval(txt)
        # Only accept list/dict results; otherwise leave it as-is
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
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col in column_names:
        v = row.get(col)

        # normalize explicit null-like strings
        if isinstance(v, str):
            s = v.strip()
            if s.lower() in NULL_TOKENS:
                v = None
            else:
                # convert string-wrapped lists/dicts
                v = _parse_composite_string(s)

        out[col] = v

    # enforce NOT NULL (skip 'id' if serial/identity)
    for col in required_nonnull:
        if out.get(col) is None:
            raise ValueError(f"column {col} is NOT NULL but value is missing/NULL")

    if serial_pk and (out.get("id") is None):
        out.pop("id", None)

    return out


# =========================
# UPLOAD (per table)
# =========================
def upload_table(
    schema: str, table: str, resources_override: list[Resource] | None = None
) -> None:
    """
    Source priority (as requested):
      1) explicit resources_override (from local datapackage.json),
      2) global RESOURCES_BY_TABLE lookup (also from datapackage.json),
      3) fallback to /meta/ discovery.
    """
    bare_key = normalize_table_key(table)

    # 1) explicit override wins if non-empty
    tabulars: list[Resource] = resources_override or []

    # 2) global mapping (if explicit was empty)
    if not tabulars and RESOURCES_BY_TABLE:
        tabulars = RESOURCES_BY_TABLE.get(bare_key, [])

    # 3) fallback to /meta/ only if we still have nothing
    if not tabulars:
        meta = get_table_meta(schema, table)
        tabulars = find_tabulars_in_meta(meta)

    if not tabulars:
        raise RuntimeError(f"No local tabular paths found for {table}")

    info = get_table_info(schema, table)
    columns: dict[str, dict] = info["columns"]
    column_names = list(columns.keys())
    required_nonnull: set[str] = {
        c for c, d in columns.items() if not d.get("is_nullable", True)
    }

    # serial/identity 'id' detection (omit when None)
    serial_pk = False
    if "id" in columns:
        coldef = columns["id"] or {}
        col_default = (coldef.get("column_default") or "").lower()
        if "nextval(" in col_default:
            serial_pk = True
            required_nonnull.discard("id")

    total_rows = 0
    if len(tabulars) > 1:
        print(
            f"Note: multiple tabular paths found for {table}: {[t.path for t in tabulars]}"
        )

    for res in tabulars:
        csv_path = resolve_csv_path(res.path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found: {csv_path}")

        print(
            f"\nProcessing {table} from {csv_path} (delimiter='{res.delimiter or ','}', encoding='{res.encoding or 'utf-8-sig'}')"
        )

        batch: list[dict] = []
        for raw_row in stream_csv_rows(csv_path, res.delimiter, res.encoding):
            try:
                mapped = convert_row_passthrough(
                    raw_row, column_names, required_nonnull, serial_pk
                )
            except Exception as e:
                # Show head of row for easier debugging
                snippet = {k: raw_row.get(k) for k in column_names[:10]}
                raise RuntimeError(
                    f"Row conversion error in {csv_path.name}: {e}\n"
                    f"Row head: {json.dumps(snippet, ensure_ascii=False)[:300]}..."
                ) from e

            batch.append(mapped)
            if len(batch) >= BATCH_SIZE:
                if DRY_RUN:
                    print(f"DRY_RUN: would POST batch of {len(batch)} rows")
                else:
                    status, payload = post_rows(schema, table, batch)
                    if status not in (200, 201, 202):
                        raise RuntimeError(f"POST failed: {status} {payload}")
                    print(f"Uploaded {len(batch)} rows -> status {status}")
                total_rows += len(batch)
                batch = []

        if batch:
            if DRY_RUN:
                print(f"DRY_RUN: would POST final batch of {len(batch)} rows")
            else:
                status, payload = post_rows(schema, table, batch)
                if status not in (200, 201, 202):
                    raise RuntimeError(f"POST failed: {status} {payload}")
                print(f"Uploaded {len(batch)} rows -> status {status}")
            total_rows += len(batch)

    print(f"Done: {table} uploaded {total_rows} rows total.")


# =========================
# FK RESOLUTION (parents-first)
# =========================
FK_DEF_RE = re.compile(
    r"FOREIGN KEY \((?P<local_cols>[^)]+)\)\s+REFERENCES\s+(?P<ref_schema>\w+)\.(?P<ref_table>\w+)\s*\((?P<ref_cols>[^)]+)\)",
    re.IGNORECASE,
)


def fk_parents_for_table(schema: str, table: str) -> set[str]:
    info = get_table_info(schema, table)
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
    """
    Accept a list of 'table' or 'schema.table'. Return **bare table names** in parents-first order.
    """
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
        for p in edges[n]:
            indeg[n] += 1

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

    # Return bare table names
    return [n.split(".", 1)[1] for n in order_fq]


def upload_tables_in_fk_order(
    idents: list[str],
    default_schema: str,
    resources_by_table: dict[str, list[Resource]] | None = None,
) -> None:
    ordered_tables = topo_sort_tables(idents, default_schema)
    print("Upload order (parents -> children):", " -> ".join(ordered_tables))

    # Normalize overrides to bare, lowercase keys for matching
    global RESOURCES_BY_TABLE
    raw_map = resources_by_table or {}
    RESOURCES_BY_TABLE = {normalize_table_key(k): v for k, v in raw_map.items()}

    # helpful visibility
    if RESOURCES_BY_TABLE:
        print(f"[override keys] {sorted(RESOURCES_BY_TABLE.keys())}")

    for t in ordered_tables:
        schema, table = split_ident(
            t, default_schema
        )  # t is bare; schema becomes default
        override = RESOURCES_BY_TABLE.get(normalize_table_key(table), [])
        print(
            f"[upload] table='{table}' schema='{schema}' override_rows={len(override)}"
        )
        upload_table(schema, table, resources_override=override)


# =========================
# Main
# =========================
# TODO Streamline csv reading -> once function, easy to use, good path reading
if __name__ == "__main__":
    resources_by_table: dict[str, list[Resource]] | None = None
    tables_input: list[str] = []
    resources_by_table = load_oem_resources(OEM_FILE)
    if OEM_FILE.exists():
        resources_by_table = load_oem_resources(OEM_FILE)
        tables_input = list(resources_by_table.keys())  # already bare+normalized keys
        print(f"Found {len(tables_input)} tables in OEM file: {OEM_FILE}")
    else:
        # Manual list (can mix 'table' and 'schema.table')
        tables_input = [
            "open_modex_bsf_scalar",
        ]

    if not tables_input:
        raise SystemExit(
            "No tables to upload. Check your OEM file or specify tables manually."
        )

    upload_tables_in_fk_order(tables_input, DEFAULT_SCHEMA, resources_by_table)
