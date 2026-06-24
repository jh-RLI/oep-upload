"""Verify an upload by comparing local CSV row counts with the OEP table counts.

This is a sanity check (counts, not values). It is pivot-aware: wide time-series
CSVs with a ``from`` / ``to`` / ``type`` header are uploaded as one row per
(timestamp x series), so the expected count for those is an estimate and a
mismatch is reported as ``review`` rather than a hard failure.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# statuses that mean "something is wrong"
_FAILURE_STATUSES = {"mismatch", "empty", "error"}


@dataclass
class TableVerification:
    table: str
    kind: str  # "long" | "wide/pivot" | "empty"
    expected: int  # expected rows derived from the local CSV(s)
    actual: Optional[int]  # row count reported by the OEP (None on error)
    status: str  # "ok" | "review" | "mismatch" | "empty" | "error"
    detail: str = ""


@dataclass
class VerificationReport:
    results: list[TableVerification] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(r.status in _FAILURE_STATUSES for r in self.results)

    def format_table(self) -> str:
        lines = [
            f"{'table':40} {'kind':12} {'local~':>10} {'oep':>10}  result",
            "-" * 86,
        ]
        for r in self.results:
            actual = "n/a" if r.actual is None else str(r.actual)
            extra = f"  ({r.detail})" if r.detail else ""
            lines.append(
                f"{r.table:40.40} {r.kind:12} {r.expected:>10} {actual:>10}"
                f"  {r.status}{extra}"
            )
        return "\n".join(lines)


def expected_db_rows(
    csv_path: Path, delimiter: str | None = None, encoding: str | None = None
) -> tuple[str, int]:
    """Estimate how many rows a CSV should produce in its OEP table.

    Returns ``(kind, expected_rows)`` where kind is:
      - ``"long"``       normal CSV with one header row -> rows = lines - 1
      - ``"wide/pivot"`` a from/to[/type] header -> rows = data_rows * n_series
      - ``"empty"``      no rows
    """
    first3: list[list[str]] = []
    total = 0
    with open(
        csv_path, "r", encoding=encoding or "utf-8", errors="replace", newline=""
    ) as fh:
        reader = csv.reader(fh, delimiter=delimiter or ";")
        for i, row in enumerate(reader):
            if i < 3:
                first3.append(row)
            total += 1

    if not first3:
        return ("empty", 0)

    first_cell = (first3[0][0] if first3[0] else "").strip().lower()
    if first_cell == "from":
        header_rows = 2
        if len(first3) >= 3 and (
            first3[2][0] if first3[2] else ""
        ).strip().lower() == "type":
            header_rows = 3
        n_series = max(0, len(first3[0]) - 1)  # every column except the time column
        data_rows = max(0, total - header_rows)
        return ("wide/pivot", data_rows * n_series)

    return ("long", max(0, total - 1))


def verify_uploaded_data(
    package_hint: Optional[str | Path] = None,
    *,
    precise_below: int = 10**9,
) -> VerificationReport:
    """Compare the configured datapackage's local CSVs with the OEP row counts."""
    # Imported here so configuration is applied before these read settings.
    from oep_upload.api.oep import OEPApiClient, TablesService
    from oep_upload.upload.datapackage import (
        DEFAULT_SCHEMA,
        find_datapackage,
        load_oem_resources,
        resolve_csv_path,
        split_ident,
    )

    oem = find_datapackage()
    if not oem:
        raise SystemExit(
            "No datapackage found. Set paths.datapackage_file or OEP_OEM_FILE."
        )

    by_table = load_oem_resources(oem)
    service = TablesService(OEPApiClient.from_settings())

    report = VerificationReport()
    for table, resources in sorted(by_table.items()):
        kind = "long"
        expected = 0
        for r in resources:
            k, n = expected_db_rows(resolve_csv_path(r.path), r.delimiter, r.encoding)
            kind = k
            expected += n

        schema, tbl = split_ident(table, DEFAULT_SCHEMA)
        try:
            actual = service.get_row_count(schema, tbl, precise_below=precise_below)
        except Exception as e:  # noqa: BLE001 - record and continue
            report.results.append(
                TableVerification(table, kind, expected, None, "error", str(e)[:120])
            )
            continue

        if actual == 0 and expected > 0:
            status = "empty"
        elif actual == expected:
            status = "ok"
        else:
            # wide/pivot expectations are estimates -> don't hard-fail on those
            status = "mismatch" if kind == "long" else "review"
        report.results.append(
            TableVerification(table, kind, expected, actual, status)
        )

    return report