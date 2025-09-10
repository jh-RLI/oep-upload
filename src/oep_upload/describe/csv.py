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


def _has_datapackage_markers(p: Path) -> bool:
    return (p / "datapackage.json").is_file() or (
        p / "datapackage.generated.json"
    ).is_file()


def _resolve_package_root_from_hint(hint: Optional[str | Path]) -> Path:
    """
    Resolve a single datapackage root directory:

      Accepts hints relative to DATA_ROOT or to the repo ROOT, and also absolute paths.
      Walks upward until a directory with datapackage markers is found.
      Requires the package root to contain a 'data/' subdirectory.

      If hint is None, tries settings.paths.datapackage_dir; if that is also None,
      auto-detects if exactly one package exists under DATA_ROOT.
    """
    ROOT, DATA_ROOT = _validate_and_resolve_roots()

    # 1) Determine the initial "candidate" path from the hint (or settings)
    if hint is None:
        hint = getattr(settings.paths, "datapackage_dir", None)

    if hint is None:
        # Attempt auto-detection: look for exactly one directory under DATA_ROOT with datapackage markers
        candidates = [
            d for d in DATA_ROOT.iterdir() if d.is_dir() and _has_datapackage_markers(d)
        ]
        if len(candidates) == 1:
            pkg_root = candidates[0]
            log.info("Auto-selected datapackage: %s", pkg_root)
            data_dir = pkg_root / "data"
            if not data_dir.is_dir():
                raise PackageSelectError(
                    f"Found datapackage at {pkg_root} but missing 'data/' subfolder."
                )
            return pkg_root
        elif len(candidates) == 0:
            raise PackageSelectError(
                "No datapackage found under DATA_ROOT. "
                "Set settings.paths.datapackage_dir or create a datapackage with datapackage.json."
            )
        else:
            names = ", ".join(c.name for c in candidates)
            raise PackageSelectError(
                f"Multiple datapackages under {DATA_ROOT}: {names}. "
                "Set settings.paths.datapackage_dir to choose one."
            )

    hint = Path(hint).expanduser()

    # 2) Try a series of resolution strategies, in this order:
    resolution_order = []

    # Absolute path as-is
    if hint.is_absolute():
        resolution_order.append(hint)
    else:
        # a) Under DATA_ROOT
        resolution_order.append((DATA_ROOT / hint).resolve())
        # b) Under ROOT
        resolution_order.append((ROOT / hint).resolve())

    # c) If hint is a bare name, try child directories by name under DATA_ROOT then ROOT
    if not hint.is_absolute() and len(hint.parts) == 1:
        by_name_dr = [
            d for d in DATA_ROOT.iterdir() if d.is_dir() and d.name == hint.name
        ]
        by_name_root = [d for d in ROOT.iterdir() if d.is_dir() and d.name == hint.name]
        resolution_order.extend(by_name_dr + by_name_root)

    # 3) Pick the first existing path and walk up to find the package root
    for base in resolution_order:
        if not base.exists():
            continue
        probe = base if base.is_dir() else base.parent

        # Walk up to ROOT boundary
        for ancestor in [probe, *probe.parents]:
            if _has_datapackage_markers(ancestor):
                if not _safe_is_relative_to(ancestor, ROOT):
                    raise PackageSelectError(
                        f"Package root {ancestor} must be inside project ROOT {ROOT}"
                    )
                data_dir = ancestor / "data"
                if not data_dir.is_dir():
                    raise PackageSelectError(
                        f"Found datapackage at {ancestor} but missing 'data/' subfolder."
                    )
                return ancestor
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
    Ensure the inferred metadata has a useful resource name and a path relative to package root.
    """
    try:
        if (
            "resources" in meta
            and isinstance(meta["resources"], list)
            and meta["resources"]
        ):
            res = meta["resources"][0]
            res.setdefault("name", csv_path.stem)
            # Relative path like "data/.../file.csv"
            res["path"] = str(csv_path.relative_to(package_root))
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
        help="Datapackage name or path. If omitted, uses settings.paths.data_dir.",
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
