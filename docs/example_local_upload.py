"""Example: upload the preprocessed datapackage to a LOCAL (Docker) OEP instance.

Shows the oep-upload library API used as a script or as a step in a data
pipeline: configure -> run -> verify. The verification is the built-in
``oep_upload.verify()`` feature (also available as ``oep-upload verify``).

Usage:
    python docs/example_local_upload.py                 # append (continue) + verify
    python docs/example_local_upload.py --replace       # clear tables, fresh upload
    python docs/example_local_upload.py --verify-only    # don't upload, just check

Prerequisites:
    * A local OEP running (Docker) at http://localhost:8000
    * A LOCAL OEP API token (from your local instance's profile settings)
    * The token in your .env as OEP_API_TOKEN_LOCAL (or set API_TOKEN below)
"""

from __future__ import annotations

import argparse

import oep_upload

# --- adjust these to your setup -------------------------------------------
DATA_ROOT = "data/postprocessed"  # folder containing datapackage.json + data/
DATAPACKAGE_FILE = "datapackage.json"  # relative to DATA_ROOT (or absolute)
API_TOKEN = None  # None -> read OEP_API_TOKEN_LOCAL / OEP_API_TOKEN from .env

# shared configuration for the local OEP — passed to every call below
LOCAL = dict(
    target="local",  # -> http://localhost:8000/api/v0/
    data_root=DATA_ROOT,
    datapackage_file=DATAPACKAGE_FILE,
    api_token=API_TOKEN,  # None falls back to the .env token
    log_file="logs/",  # one timestamped log file per run
)
# --------------------------------------------------------------------------


def upload(strategy: str) -> int:
    """Run the full pipeline against the local OEP."""
    print(f"Uploading (strategy={strategy}) ...")
    return oep_upload.run(strategy=strategy, **LOCAL)


def verify() -> int:
    """Verify the upload using the built-in feature, then print the report."""
    report = oep_upload.verify(**LOCAL)
    print(report.format_table())
    print("\nResult:", "OK" if report.ok else "PROBLEMS FOUND")
    return 0 if report.ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Local OEP upload example.")
    parser.add_argument(
        "--replace", action="store_true", help="Clear each table first (fresh upload)."
    )
    parser.add_argument(
        "--verify-only", action="store_true", help="Only verify; do not upload."
    )
    parser.add_argument(
        "--no-verify", action="store_true", help="Skip the verification step."
    )
    args = parser.parse_args()

    if args.verify_only:
        return verify()

    rc = upload("replace" if args.replace else "append")
    if rc != 0:
        return rc
    if not args.no_verify:
        rc = verify()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())