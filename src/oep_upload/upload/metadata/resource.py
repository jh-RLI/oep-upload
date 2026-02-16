# oep_upload/upload/resource.py

from __future__ import annotations

from oemetadata.v2.v20.example import OEMETADATA_V20_EXAMPLE
from oemetadata.v2.v20.template import OEMETADATA_V20_TEMPLATE

import json
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Any, Iterable

from oep_upload.config import get_settings, export_env_vars
from oep_upload.config.logging import setup_logging
from oep_upload.utils import is_blank, slugify

# Resolve selected datapackage dir
from oep_upload.describe.csv_file import _resolve_package_root_from_hint

# OEP API helper
from oem2orm import oep_oedialect_oem2orm as oem2orm

import oem2orm.normalizer as norm

log = setup_logging()
settings = get_settings()
export_env_vars(settings)


@dataclass(frozen=True)
class UploadResult:
    resource_name: str
    status: str  # "uploaded" | "skipped" | "error"
    detail: str = ""


def _choose_datapackage_file(package_root: Path) -> Path:
    """
    Prefer an explicit datapackage.json; otherwise fall back to the generated one.
    """
    dp = package_root / "datapackage.json"
    if dp.is_file():
        return dp
    raise FileNotFoundError(
        f"No datapackage.json found in {package_root}. "
        "If you created one via the inspector, rename or keep the generated filename."
    )


def _load_datapackage(package_root: Path) -> dict:
    path = _choose_datapackage_file(package_root)
    log.info("Using datapackage file: %s", path)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise ValueError(f"Failed to load datapackage at {path}: {e}") from e


# --------------------
# Schema normalization helpers (fields + PK/FK)
# --------------------


def _as_list_of_str(v) -> list[str]:
    """
    Coerce a single string or list-like into list[str].
    - None -> []
    - "id" -> ["id"]
    - ["id","version"] -> ["id","version"]
    - other -> [str(other)]
    """
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v]
    return [str(v)]


def _schema_field_names(resource: dict) -> set[str]:
    """
    Return set of declared column names from resource.schema.fields[*].name
    """
    schema = resource.get("schema")
    if not isinstance(schema, dict):
        return set()
    fields = schema.get("fields")
    if not isinstance(fields, list):
        return set()
    out = set()
    for f in fields:
        if isinstance(f, dict):
            n = f.get("name")
            if isinstance(n, str) and n.strip():
                out.add(n.strip())
    return out


def _normalize_schema(resource: dict, *, fk_schema: Optional[str] = None) -> dict:
    """
    Return a NEW resource dict where:
      - fields[*].nullable is always set (default True)
      - schema.primaryKey always exists and is a list[str] (empty if missing)
      - foreignKeys[*].fields/reference.fields are lists
      - missing sides are filled from the other
      - reference.resource is qualified if fk_schema is provided (not used here)
    """
    res = deepcopy(resource)
    schema = res.get("schema")
    if not isinstance(schema, dict):
        return res

    # ---- fields: ensure "nullable"
    fields = schema.get("fields")
    if isinstance(fields, list):
        out_fields = []
        for f in fields:
            if isinstance(f, dict):
                g = dict(f)
                if "nullable" not in g:
                    g["nullable"] = True
                out_fields.append(g)
            else:
                out_fields.append(f)
        schema["fields"] = out_fields

    # ---- primaryKey -> ensure exists, always list[str]
    if "primaryKey" in schema:
        schema["primaryKey"] = _as_list_of_str(schema.get("primaryKey"))
    else:
        schema["primaryKey"] = []

    # ---- foreignKeys
    fks = schema.get("foreignKeys")
    if fks is None:
        res["schema"] = schema
        return res
    if isinstance(fks, dict):
        fks = [fks]
    elif not isinstance(fks, list):
        fks = [fks]

    declared_cols = _schema_field_names(res)
    out_fks: list[dict] = []

    for idx, fk in enumerate(fks, start=1):
        if not isinstance(fk, dict):
            log.warning("Skipping non-dict foreignKey at index %d", idx)
            continue
        fk = dict(fk)

        local_fields = _as_list_of_str(fk.get("fields"))
        ref = fk.get("reference")
        ref = dict(ref) if isinstance(ref, dict) else {}
        ref_fields = _as_list_of_str(ref.get("fields"))
        ref_res = ref.get("resource")

        if not ref_fields and local_fields:
            ref_fields = list(local_fields)
        if not local_fields and ref_fields:
            local_fields = list(ref_fields)

        if isinstance(ref_res, str) and fk_schema and "." not in ref_res:
            ref["resource"] = f"{fk_schema}.{ref_res}"

        fk["fields"] = local_fields
        ref["fields"] = ref_fields
        fk["reference"] = ref

        for lf in local_fields:
            if lf not in declared_cols:
                log.warning(
                    "FK refers to missing local column '%s' in resource '%s'",
                    lf,
                    res.get("name") or "<unnamed>",
                )
        if len(local_fields) != len(ref_fields):
            log.warning(
                "FK field count mismatch local=%s vs reference=%s in resource '%s'",
                local_fields,
                ref_fields,
                res.get("name") or "<unnamed>",
            )

        out_fks.append(fk)

    schema["foreignKeys"] = out_fks
    res["schema"] = schema
    return res


# --------------------
# Keywords helpers
# --------------------


def _merge_keywords(
    existing: Optional[Iterable[str]], extras: Optional[Iterable[str]]
) -> list[str]:
    """
    Merge two keyword iterables into a de-duplicated list (case-insensitive).
    Preserves original casing from the first occurrence.
    """
    out: list[str] = []
    seen: set[str] = set()
    for source in (existing or []), (extras or []):
        for k in source:
            if not isinstance(k, str):
                continue
            kk = k.strip()
            if not kk:
                continue
            lk = kk.lower()
            if lk not in seen:
                seen.add(lk)
                out.append(kk)
    return out


# --------------------
# Metadata assembly
# --------------------


def _assemble_resource_metadata(
    pkg: dict,
    resource: dict,
    package_root: Path,
    *,
    extra_keywords: Optional[list[str]] = None,
) -> dict:
    """
    Build a single-resource OEMetadata v2.0 document from a frictionless datapackage.
    - Uses OEMETADATA_V20_TEMPLATE + example @context
    - Normalizes fields (nullable) + PK/FK shapes
    - Applies SAME normalizers used during table creation
    - Adds resource-level keywords if provided
    - Returns a document with exactly one resource in `resources`
    """
    md: dict[str, Any] = deepcopy(OEMETADATA_V20_TEMPLATE)
    md["@context"] = OEMETADATA_V20_EXAMPLE["@context"]

    # dataset-like top-levels
    pkg_name = pkg.get("name")
    pkg_title = pkg.get("title") or pkg_name or package_root.name

    md["@id"] = pkg.get("id") or pkg.get("@id")  # optional
    md["name"] = pkg_name or slugify(package_root.name, fallback="_empty_")
    md["title"] = pkg_title
    md["description"] = pkg.get("description")

    # carry over useful package-level facets
    if "keywords" in pkg:
        md["keywords"] = pkg["keywords"]
    if "license" in pkg:
        md["license"] = pkg["license"]
    if "licenses" in pkg and isinstance(pkg["licenses"], list):
        md["licenses"] = pkg["licenses"]

    # --- prepare resource (work on a copy)
    r = deepcopy(resource)

    # prefill name/title before normalization
    if is_blank(r.get("name")) and isinstance(r.get("path"), str):
        r["name"] = slugify(Path(r["path"]).stem, fallback="_empty_")
    if is_blank(r.get("title")):
        r["title"] = r.get("name") or "resource"

    # 1) Normalize schema (fields + PK/FK)
    r = _normalize_schema(r, fk_schema=None)  # no schema prefixing

    # 2) Normalize identifiers/columns; remap PK/FK to normalized names
    norm_table_name, col_map = norm.normalize_resource_inplace(
        r,
        table_norm=norm.TABLE_NORMALIZER,
        col_norm=norm.COLUMN_NORMALIZER,
    )

    # 3) Re-run schema normalization in case shapes were rewritten
    r = _normalize_schema(r, fk_schema=None)

    # 4) Add resource-level keywords (de-duplicated)
    if extra_keywords:
        r["keywords"] = _merge_keywords(r.get("keywords"), extra_keywords)

    # record normalization info
    extras = r.get("extras", {})
    extras["oep:normalized_table_name"] = norm_table_name
    if col_map:
        extras["oep:normalized_columns"] = col_map
    r["extras"] = extras

    md["resources"] = [r]
    return md


def _extract_table_identifier(resource: dict) -> Optional[str]:
    """
    Return the *normalized* table identifier (matches created tables).
    """
    name = resource.get("name")
    if is_blank(name):
        return None
    return norm.TABLE_NORMALIZER(str(name))


def upload_resource_metadata_for_package(
    package_hint: Optional[str | Path] = None,
    *,
    extra_keywords: Optional[list[str]] = None,  # e.g. ["oep_upload"]
) -> list[UploadResult]:
    """
    Upload per-resource OEMetadata (single-resource docs) for the selected datapackage.
    Assumes tables already exist and were created using the same normalizers.

    Params:
        extra_keywords: list of strings to add to each resource's 'keywords'.
    """
    package_root = _resolve_package_root_from_hint(package_hint)
    log.info("Uploading resource metadata for datapackage: %s", package_root)

    pkg = _load_datapackage(package_root)

    resources = pkg.get("resources") or []
    if not isinstance(resources, list) or not resources:
        log.warning("No resources found in datapackage at %s", package_root)
        return []

    results: list[UploadResult] = []

    for idx, resource in enumerate(resources, start=1):
        try:
            md_single = _assemble_resource_metadata(
                pkg, resource, package_root, extra_keywords=extra_keywords
            )

            # IMPORTANT: use normalized name for addressing the table
            table_name = _extract_table_identifier(md_single["resources"][0])
            if not table_name:
                log.warning("Resource #%d has no usable name; skipping.", idx)
                results.append(
                    UploadResult(
                        resource_name=f"#{idx}",
                        status="skipped",
                        detail="missing resource.name",
                    )
                )
                continue

            log.info("Uploading metadata for table '%s'...", table_name)

            # Upload the normalized, single-resource metadata
            oem2orm.api_updateMdOnTable(
                md_single,
                table_name,
                settings.effective_api_token,
            )

            results.append(UploadResult(resource_name=table_name, status="uploaded"))
            log.info("Uploaded metadata for '%s'.", table_name)

        except Exception as e:
            detail = str(e)
            res_name = resource.get("name") or f"#{idx}"
            log.error("Error uploading metadata for '%s': %s", res_name, detail)
            results.append(
                UploadResult(resource_name=res_name, status="error", detail=detail)
            )

    log.info(
        "Resource metadata upload complete: %d uploaded, %d skipped, %d errors",
        sum(1 for r in results if r.status == "uploaded"),
        sum(1 for r in results if r.status == "skipped"),
        sum(1 for r in results if r.status == "error"),
    )
    return results
