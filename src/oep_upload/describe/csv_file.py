# oep_upload/inspect_and_generate.py

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from oep_upload.config import get_settings, export_env_vars
from oep_upload.config.logging import setup_logging
from oep_upload.utils import is_blank, slugify

from omi.inspection import infer_metadata, InspectionError

log = setup_logging()
settings = get_settings()
export_env_vars(settings)


@dataclass(frozen=True)
class GenerationResult:
    csv_path: Path
    datapackage_path: Path | None
    status: str  # "generated" | "skipped-exists" | "skipped-no-change" | "error"
    detail: str = ""


class PackageSelectError(Exception):
    """Raised when a datapackage root cannot be uniquely resolved."""


# -----------------------
# Utilities
# -----------------------


def _to_posix(p: Path, base: Path) -> str:
    return str(p.relative_to(base).as_posix())


def _safe_is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
        return True
    except Exception:
        return False


def _hash_obj(obj: dict) -> str:
    payload = json.dumps(obj, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:12]


def _load_json_if_exists(p: Path) -> dict | None:
    if p.exists() and p.is_file():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            log.warning(
                "Existing JSON at %s could not be parsed; will overwrite safely.", p
            )
    return None


def _write_json_pretty(p: Path, data: dict) -> None:
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def _validate_and_resolve_roots() -> tuple[Path, Path]:
    ROOT = Path(settings.paths.root).expanduser().resolve()
    DATA_ROOT = (ROOT / settings.paths.data_dir).expanduser().resolve()

    log.info("Configured ROOT: %s", ROOT)
    log.info("Configured DATA_ROOT: %s", DATA_ROOT)

    if not ROOT.exists() or not ROOT.is_dir():
        raise FileNotFoundError(
            f"Configured ROOT does not exist or is not a directory: {ROOT}"
        )
    if not DATA_ROOT.exists():
        raise FileNotFoundError(
            f"Configured data directory does not exist: {DATA_ROOT}"
        )
    if not DATA_ROOT.is_dir():
        raise NotADirectoryError(
            f"Configured data directory is not a directory: {DATA_ROOT}"
        )
    if not _safe_is_relative_to(DATA_ROOT, ROOT):
        raise PermissionError(
            f"DATA_ROOT must be inside ROOT. Got DATA_ROOT={DATA_ROOT}, ROOT={ROOT}"
        )
    return ROOT, DATA_ROOT


def _has_marker_file(p: Path) -> bool:
    return (p / "datapackage.json").is_file() or (
        p / "datapackage.generated.json"
    ).is_file()


def _contains_csvs(data_dir: Path) -> bool:
    if not data_dir.is_dir():
        return False
    try:
        next(
            d for d in data_dir.rglob("*") if d.is_file() and d.suffix.lower() == ".csv"
        )
        return True
    except StopIteration:
        return False


def _looks_like_package(p: Path) -> bool:
    """A directory looks like a package if it has datapackage markers OR a data/ folder with CSVs."""
    return _has_marker_file(p) or _contains_csvs(p / "data")


def _resolve_package_root_from_hint(hint: Optional[str | Path]) -> Path:
    """
    Resolve a single datapackage root directory.

    Accepts:
      - Absolute paths
      - Paths relative to DATA_ROOT
      - Paths relative to project ROOT
      - Bare names (matched under DATA_ROOT or ROOT)

    A directory qualifies as a package if it either has datapackage markers
    OR contains a 'data/' subfolder with at least one CSV.

    If hint is None, tries settings.paths.data_dir; if still None,
    auto-detects when exactly one such directory exists under DATA_ROOT.
    """
    ROOT, DATA_ROOT = _validate_and_resolve_roots()

    # 1) Initial hint
    if hint is None:
        hint = getattr(settings.paths, "data_dir", None)

    # 2) Auto-detect if no hint
    if hint is None:
        candidates = [
            d for d in DATA_ROOT.iterdir() if d.is_dir() and _looks_like_package(d)
        ]
        if len(candidates) == 1:
            pkg_root = candidates[0].resolve()
            log.info("Auto-selected datapackage: %s", pkg_root)
            return pkg_root
        elif len(candidates) == 0:
            raise PackageSelectError(
                "No datapackage-like directory found under DATA_ROOT. "
                "Provide --package or set settings.paths.data_dir to a folder with 'data/' and CSVs."
            )
        else:
            names = ", ".join(c.name for c in candidates)
            raise PackageSelectError(
                f"Multiple datapackage-like directories under {DATA_ROOT}: {names}. "
                "Provide --package or set settings.paths.data_dir."
            )

    hint = Path(hint).expanduser()

    # 3) Build resolution candidates
    resolution_order: list[Path] = []
    if hint.is_absolute():
        resolution_order.append(hint)
    else:
        resolution_order.append((DATA_ROOT / hint).resolve())
        resolution_order.append((ROOT / hint).resolve())

        # Bare name search under DATA_ROOT then ROOT
        if len(hint.parts) == 1:
            by_name_dr = [
                d for d in DATA_ROOT.iterdir() if d.is_dir() and d.name == hint.name
            ]
            by_name_root = [
                d for d in ROOT.iterdir() if d.is_dir() and d.name == hint.name
            ]
            resolution_order.extend(by_name_dr + by_name_root)

    # 4) For each existing candidate, accept it or walk up until ROOT looking for a package-like dir
    for base in resolution_order:
        if not base.exists():
            continue
        probe = base if base.is_dir() else base.parent

        # If the probe itself looks like a package, accept it
        if _looks_like_package(probe):
            if not _safe_is_relative_to(probe, ROOT):
                raise PackageSelectError(
                    f"Package root {probe} must be inside project ROOT {ROOT}"
                )
            return probe.resolve()

        # Walk up to ROOT boundary
        for ancestor in [probe, *probe.parents]:
            if _looks_like_package(ancestor):
                if not _safe_is_relative_to(ancestor, ROOT):
                    raise PackageSelectError(
                        f"Package root {ancestor} must be inside project ROOT {ROOT}"
                    )
                return ancestor.resolve()
            # stop walking once we leave ROOT
            try:
                ancestor.relative_to(ROOT)
            except Exception:
                break

    raise PackageSelectError(f"Could not resolve datapackage name/path: {hint}")


def _iter_csvs_in_datapackage(package_root: Path) -> Iterable[Path]:
    """
    Yield CSV files only from <package_root>/data/** (case-insensitive .csv).
    """
    data_dir = package_root / "data"
    for p in data_dir.rglob("*"):
        if p.is_file() and p.suffix.lower() == ".csv":
            yield p


def _ensure_resource_name(meta: dict, csv_path: Path, package_root: Path) -> None:
    """
    Ensure resource has a non-empty name and a POSIX-relative path.
    """
    try:
        if (
            "resources" in meta
            and isinstance(meta["resources"], list)
            and meta["resources"]
        ):
            res = meta["resources"][0]

            # If name missing or blank, set from filename stem (slugified)
            if is_blank(res.get("name")):
                res["name"] = slugify(csv_path.stem)

            # Always ensure path is relative POSIX ("data/.../file.csv")
            res["path"] = _to_posix(csv_path, package_root)

            # Helpful default that doesn't hurt if already set
            res.setdefault("profile", "tabular-data-resource")
    except Exception:
        pass


# -----------------------
# Inference
# -----------------------


def generate_datapackage_for_csv(
    csv_path: Path, package_root: Path, metadata_format: str = "OEP"
) -> dict:
    """
    Use OMI + frictionless to infer metadata for a single CSV.
    Assumes semicolon-delimited data per your inspector.
    """
    log.debug("Inferring metadata for %s", csv_path)
    try:
        inferred = infer_metadata(str(csv_path), metadata_format=metadata_format)
        _ensure_resource_name(inferred, csv_path, package_root)
        return inferred
    except InspectionError:
        raise
    except Exception as e:
        raise InspectionError(f"Inference failed for {csv_path}: {e}") from e


def _maybe_backup_existing(original: Path) -> None:
    """
    If overwriting datapackage.json, optionally back it up first (opt-in via settings.backups.enable_datapackage_backup).
    """
    try:
        enable_backup = getattr(settings, "backups", {}).get(
            "enable_datapackage_backup", False
        )
        if original.exists() and enable_backup:
            backup = original.with_suffix(original.suffix + ".bak")
            original.replace(backup)
            log.info("Backed up existing %s to %s", original.name, backup.name)
    except Exception as e:
        log.warning("Could not backup existing %s: %s", original, e)


# -----------------------
# Main process (single package)
# -----------------------


def process_single_datapackage(
    package_hint: Optional[str | Path] = None,
    overwrite_legacy: bool = False,
    stop_on_error: bool = False,
) -> list[GenerationResult]:
    """
    Generate one datapackage file for exactly one package root.
    The output is written to <package_root>/datapackage.generated.json (and optionally datapackage.json).
    """
    package_root = _resolve_package_root_from_hint(package_hint)
    log.info("Selected datapackage: %s", package_root)

    csv_files = list(_iter_csvs_in_datapackage(package_root))
    if not csv_files:
        log.warning("No CSV files found in %s/data", package_root)
        return []

    log.info("Found %d CSV file(s) under %s/data", len(csv_files), package_root)

    # Infer metadata per CSV and collect resources
    resources = []
    results: list[GenerationResult] = []
    first_meta_template: dict | None = None

    for csv_path in csv_files:
        rel = csv_path.relative_to(package_root)
        log.info("Processing CSV: %s", rel)
        try:
            meta = generate_datapackage_for_csv(
                csv_path, package_root, metadata_format="OEP"
            )
            if first_meta_template is None:
                first_meta_template = (
                    meta  # use first inferred as base for package-level fields
                )
            # Always use exactly one resource per CSV
            if (
                "resources" in meta
                and isinstance(meta["resources"], list)
                and meta["resources"]
            ):
                resources.append(meta["resources"][0])
            else:
                raise InspectionError(f"No 'resources' produced for {rel}")
            results.append(
                GenerationResult(
                    csv_path, package_root / "datapackage.generated.json", "generated"
                )
            )
        except Exception as e:
            log.error("Error processing %s: %s", csv_path, e)
            results.append(GenerationResult(csv_path, None, "error", str(e)))
            if stop_on_error:
                raise

    # If at least one resource succeeded, build the package file
    ok_resources = [r for r in resources if isinstance(r, dict)]
    if ok_resources:
        # Build package dict: start from first meta, then replace resources with the collected list
        package_meta = (first_meta_template or {}).copy()
        package_meta["resources"] = ok_resources

        target_generated = package_root / "datapackage.generated.json"
        existing = _load_json_if_exists(target_generated)
        new_hash = _hash_obj(package_meta)
        old_hash = _hash_obj(existing) if isinstance(existing, dict) else None

        if old_hash == new_hash:
            log.info("No change for %s (already up-to-date).", target_generated)
            # Update statuses that were "generated" to "skipped-no-change"
            results = [
                (
                    r
                    if r.status != "generated"
                    else GenerationResult(
                        r.csv_path, target_generated, "skipped-no-change"
                    )
                )
                for r in results
            ]
        else:
            _write_json_pretty(target_generated, package_meta)
            log.info(
                "Wrote %s with %d resource(s).", target_generated, len(ok_resources)
            )

        if overwrite_legacy:
            target_legacy = package_root / "datapackage.json"
            _maybe_backup_existing(target_legacy)
            _write_json_pretty(target_legacy, package_meta)
            log.info("Wrote %s", target_legacy)
    else:
        log.warning("No valid resources could be inferred. Package file not written.")

    # Summary
    log.info(
        "Metadata generation: %d succeeded, %d skipped, %d errors",
        sum(1 for r in results if r.status == "generated"),
        sum(1 for r in results if r.status.startswith("skipped")),
        sum(1 for r in results if r.status == "error"),
    )
    return results


# -----------------------
# CLI
# -----------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Infer OEP datapackage metadata for a single datapackage."
    )
    parser.add_argument(
        "--package",
        "-p",
        help=(
            "Datapackage name or path. May point to a folder with 'data/' containing CSVs "
            "(even without a datapackage.json). If omitted, uses settings.paths.data_dir "
            "or auto-detects a single package under DATA_ROOT."
        ),
        default=None,
    )
    parser.add_argument(
        "--overwrite-legacy",
        action="store_true",
        help="Also write datapackage.json (backup optional via settings.backups.enable_datapackage_backup).",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop at first error instead of continuing.",
    )
    return parser


def main():
    parser = _build_arg_parser()
    args = parser.parse_args()
    process_single_datapackage(
        package_hint=args.package,
        overwrite_legacy=bool(args.overwrite_legacy),
        stop_on_error=bool(args.stop_on_error),
    )


if __name__ == "__main__":
    main()
