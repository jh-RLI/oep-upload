#!/usr/bin/env python3
import argparse
from pathlib import Path
import re
import pandas as pd


def _sanitize_sheet_name(name: str, max_len: int = 80) -> str:
    # Keep letters, digits, dash, underscore; replace others with "_"
    safe = re.sub(r"[^A-Za-z0-9\-_]", "_", name or "Sheet")
    return safe[:max_len].rstrip("_") or "Sheet"


def _unique_path(base: Path) -> Path:
    if not base.exists():
        return base
    stem, suffix = base.stem, base.suffix
    for i in range(1, 10_000):
        candidate = base.with_name(f"{stem}__{i}{suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Could not create unique filename for {base}")


def split_xlsx_to_csvs(
    xlsx_path,
    output_dir=None,
    sep=";",
    encoding="utf-8",
    na_rep="",
    date_format=None,
    skip_empty=True,
):
    """
    Split all sheets of an Excel file into separate CSVs.

    Parameters
    ----------
    xlsx_path : str | Path
        Path to the input .xlsx file.
    output_dir : str | Path | None
        Directory to save CSV files. Defaults to same dir as input file.
    sep : str
        CSV delimiter.
    encoding : str
        File encoding for output CSVs.
    na_rep : str
        String representation for NaN/NaT.
    date_format : str | None
        Optional Pandas date_format for datetime columns (e.g., "%Y-%m-%d").
    skip_empty : bool
        If True, skip completely empty sheets.

    Returns
    -------
    list[Path]
        List of output CSV paths.
    """
    xlsx_path = Path(xlsx_path)
    output_dir = Path(output_dir) if output_dir else xlsx_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Read all sheets at once (dict of {sheet_name: DataFrame})
    xls = pd.read_excel(xlsx_path, sheet_name=None, engine="openpyxl")
    output_paths = []

    for sheet_name, df in xls.items():
        # Optionally skip empty sheets (no rows or no columns or all NaN)
        if skip_empty and (df.empty or df.dropna(how="all").empty):
            continue

        safe_name = _sanitize_sheet_name(sheet_name)
        out_path = output_dir / f"{xlsx_path.stem}_{safe_name}.csv"
        out_path = _unique_path(out_path)  # avoid overwriting

        # Write CSV
        df.to_csv(
            out_path,
            index=False,
            sep=sep,
            encoding=encoding,
            na_rep=na_rep,
            date_format=date_format,
        )
        output_paths.append(out_path)

    return output_paths


def main():
    p = argparse.ArgumentParser(
        description="Split all sheets in an Excel file into CSV files."
    )
    p.add_argument("xlsx_path", help="Path to the .xlsx file")
    p.add_argument(
        "-o", "--output-dir", help="Directory to write CSVs (default: same as input)"
    )
    p.add_argument("--sep", default=";", help="CSV delimiter (default: ;) ")
    p.add_argument("--encoding", default="utf-8", help="CSV encoding (default: utf-8)")
    p.add_argument(
        "--na-rep", default="", help="String for NaN values (default: empty)"
    )
    p.add_argument(
        "--date-format", default=None, help='Format datetimes (e.g. "%Y-%m-%d")'
    )
    p.add_argument(
        "--no-skip-empty", action="store_true", help="Do not skip empty sheets"
    )
    args = p.parse_args()

    paths = split_xlsx_to_csvs(
        args.xlsx_path,
        output_dir=args.output_dir,
        sep=args.sep,
        encoding=args.encoding,
        na_rep=args.na_rep,
        date_format=args.date_format,
        skip_empty=not args.no_skip_empty,
    )
    for pth in paths:
        print(pth)


if __name__ == "__main__":
    main()
