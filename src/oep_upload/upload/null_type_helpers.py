from __future__ import annotations

from typing import Any

# ---- missing token helpers (self-contained) ----

MISSING_TOKENS = {"", "na", "n/a", "nan", "null", "none", ".", "-", "—"}


def is_missing(val: str) -> bool:
    v = (val or "").strip().lower()
    return v in MISSING_TOKENS


# ---- postgres coercion helpers ----

BOOL_TRUE = {"1", "1.0", "true", "t", "yes", "y"}
BOOL_FALSE = {"0", "0.0", "false", "f", "no", "n"}

PG_BOOL_TYPES = {"bool", "boolean"}


def _pg_type_name(coldef: dict | None) -> str:
    if not coldef:
        return ""
    t = coldef.get("udt_name") or coldef.get("data_type") or coldef.get("type") or ""
    return str(t).strip().lower()


def to_db_null_if_missing(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return None if is_missing(s) else s
    return v


def coerce_value_for_pg(v: Any, pg_type: str) -> Any:
    v2 = to_db_null_if_missing(v)
    if v2 is None:
        return None

    t = (pg_type or "").strip().lower()

    if t in PG_BOOL_TYPES or t.endswith("bool") or t.endswith("boolean"):
        if isinstance(v2, bool):
            return v2

        # accept numeric 0/1 from pyarrow, etc.
        if isinstance(v2, (int, float)) and v2 in (0, 1, 0.0, 1.0):
            return bool(int(v2))

        s = str(v2).strip().lower()
        if s in BOOL_TRUE:
            return True
        if s in BOOL_FALSE:
            return False
        raise ValueError(f"Invalid boolean token: {v2!r}")

    return v2


def coerce_row_for_pg(
    row: dict[str, Any],
    coldefs: dict[str, dict],
    required_nonnull: set[str],
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col, v in row.items():
        pg_t = _pg_type_name(coldefs.get(col))
        try:
            out[col] = coerce_value_for_pg(v, pg_t)
        except ValueError as e:
            # include column + detected pg type so we can debug mapping/schema issues
            raise ValueError(
                f"{e} (column={col!r}, pg_type={pg_t!r}, value={v!r})"
            ) from e

    missing_required = [c for c in required_nonnull if out.get(c) is None]
    if missing_required:
        raise ValueError(
            f"Missing required values (NULL after coercion): {missing_required}"
        )

    return out
