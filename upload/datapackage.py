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

from config import get_settings, export_env_vars


# =========================
# SETTINGS / CONSTANTS
# =========================

_s = get_settings()
export_env_vars(_s)  # keep legacy env-var consumers happy

# Effective endpoint and token / headers
_API_BASE: str = str(_s.endpoint.api_base_url).rstrip("/") + "/"
_TIMEOUT: int = int(_s.api.timeout_s)
_TOKEN: str = _s.effective_api_token or ""
_HEADER = {"Authorization": f"Token {_TOKEN}"} if _TOKEN else {}

# Upload behavior
BATCH_SIZE: int = int(_s.upload.batch_size)
DRY_RUN: bool = bool(_s.upload.dry_run)
MAX_RETRIES: int = int(_s.upload.max_retries)
RETRY_BASE_DELAY: float = float(_s.upload.retry_base_delay)
DEFAULT_SCHEMA: str = _s.upload.default_schema

# Null tokens
NULL_TOKENS: set[str] = set(map(lambda s: s.lower(), _s.upload.null_tokens))

# Paths
ROOT = Path(_s.paths.root).resolve()
DATA_ROOT = (ROOT / _s.paths.data_dir).resolve()
OEM_FILE = (
    (ROOT / _s.paths.datapackage_file).resolve() if _s.paths.datapackage_file else None
)

# Global override map (filled later)
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
    Resolution:
    - absolute & exists -> use it
    - relative -> DATA_ROOT/<raw>
    """
    raw_path = Path(raw)
    if raw_path.is_absolute():
        return raw_path
    return (DATA_ROOT / raw_path).resolve()


def _join_api(*parts: str) -> str:
    # Build URLs like <api_base>/schema/<schema>/tables/<table>/...
    suffix = "/".join(p.strip("/") for p in parts if p is not None)
    return f"{_API_BASE}{suffix}"


# =========================
# API
# =========================
def api_get(url: str) -> dict:
    r = requests.get(url, headers=_HEADER, timeout=_TIMEOUT)
    r.raise_for_status()
    return r.json()


def post_rows(schema: str, table: str, rows: list[dict]) -> tuple[int, dict]:
    url = _join_api("schema", schema, "tables", table, "rows", "new")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                url, headers=_HEADER, json={"query": rows}, timeout=max(120, _TIMEOUT)
            )
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
    return api_get(_join_api("schema", schema, "tables", table))


def get_table_meta(schema: str, table: str) -> dict:
    return api_get(_join_api("schema", schema, "tables", table, "meta"))


# =========================
# OEMetadata / datapackage
# =========================
def load_oem_resources(oem_path: Path) -> dict[str, list[Resource]]:
    """
    Parse datapackage/OEM file and return:
      { <normalized bare table>: [Resource(...), ...] }
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

        dialect = res.get("dialect") if isinstance(res.get("dialect"), dict) else {}
        delimiter = dialect.get("delimiter") or guess_delimiter_from_path(path)
        encoding = res.get("encoding") or dialect.get("encoding")

        key = normalize_table_key(table_name)
        out[key].append(Resource(path=path, delimiter=delimiter, encoding=encoding))

    return dict(out)


def find_datapackage() -> Path | None:
    """
    Discovery priority:
      1) env OEP_OEM_FILE (if present)
      2) config.paths.datapackage_file (if provided)
      3) CWD/datapackage.json
      4) ROOT/datapackage.json
      5) DATA_ROOT/datapackage.json
    """
    env_hint = (
        (Path.cwd() / (os.environ.get("OEP_OEM_FILE") or "")).resolve()
        if "OEP_OEM_FILE" in os.environ
        else None
    )
    candidates: list[Path] = []
    if env_hint and env_hint.name:
        candidates.append(env_hint)
    if OEM_FILE:
        candidates.append(OEM_FILE)
    candidates.append(Path.cwd() / "datapackage.json")
    candidates.append(ROOT / "datapackage.json")
    candidates.append(DATA_ROOT / "datapackage.json")

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
def stream_csv_rows(
    csv_path: Path, delimiter: str | None, encoding: str | None
) -> Iterable[dict[str, Any]]:
    enc = encoding or "utf-8-sig"
    delim = delimiter if (isinstance(delimiter, str) and len(delimiter) == 1) else ","
    with csv_path.open("r", newline="", encoding=enc) as f:
        reader = csv.DictReader(f, delimiter=delim)
        for row in reader:
            yield row


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
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col in column_names:
        v = row.get(col)

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
# UPLOAD (per table)
# =========================
def upload_table(
    schema: str, table: str, resources_override: list[Resource] | None = None
) -> None:
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
            f"\nProcessing {table} from {csv_path} "
            f"(delimiter='{res.delimiter or ','}', encoding='{res.encoding or 'utf-8-sig'}')"
        )

        batch: list[dict] = []
        for raw_row in stream_csv_rows(csv_path, res.delimiter, res.encoding):
            try:
                mapped = convert_row_passthrough(
                    raw_row, column_names, required_nonnull, serial_pk
                )
            except Exception as e:
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

    return [n.split(".", 1)[1] for n in order_fq]


def upload_tables_in_fk_order(
    idents: list[str],
    default_schema: str,
    resources_by_table: dict[str, list[Resource]] | None = None,
) -> None:
    ordered_tables = topo_sort_tables(idents, default_schema)
    print("Upload order (parents -> children):", " -> ".join(ordered_tables))

    global RESOURCES_BY_TABLE
    raw_map = resources_by_table or {}
    RESOURCES_BY_TABLE = {normalize_table_key(k): v for k, v in raw_map.items()}

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
# Entrypoint helper (optional)
# =========================
def run_from_oem() -> None:
    """
    Convenience entry: read datapackage from config/env,
    derive tables, and upload in FK order.
    """
    resources_by_table: dict[str, list[Resource]] | None = None
    tables_input: list[str] = []

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

    upload_tables_in_fk_order(tables_input, DEFAULT_SCHEMA, resources_by_table)
