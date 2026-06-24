"""Example: run the oep-upload pipeline STEP BY STEP (instead of run()).

Use this when you want to drive the phases yourself — e.g. inspect or gate
between steps, or re-run only a later step (just configure, then call that one
function). The full pipeline is:

    describe -> create tables -> upload rows -> upload metadata  (-> verify)

Usage:
    python docs/example_step_by_step.py

Prerequisites: a local OEP at http://localhost:8000 and a token in .env
(OEP_API_TOKEN_LOCAL). Adjust the paths below to your dataset.
"""

from __future__ import annotations

import oep_upload

# Shared configuration for the local OEP.
LOCAL = dict(
    target="local",  # -> http://localhost:8000/api/v0/
    data_root="data/postprocessed",  # folder with datapackage.json + data/
    datapackage_file="datapackage.json",
    log_file="logs/",  # one timestamped log file per run
)


def main() -> int:
    # 1) Configure ONCE. The step helpers below then reuse this configuration
    #    (only pass overrides again if you want to change something).
    settings = oep_upload.configure(**LOCAL)
    print("Target:", settings.endpoint.api_base_url)

    # 2) Describe — infer/refresh datapackage metadata from the CSVs.
    #    Skip this if you maintain datapackage.json by hand.
    results = oep_upload.describe_datapackage()
    print(f"describe: {len(results)} resource(s) processed")

    # --- you could pause here to review the generated datapackage ---

    # 3) Create — create the tables on the OEP (model_draft schema).
    oep_upload.create_tables()

    # 4) Upload rows — stream the CSVs and POST them in batches.
    #    Pass strategy="replace" here (or in configure) for a fresh upload.
    oep_upload.upload_rows()  # e.g. oep_upload.upload_rows(strategy="replace")

    # 5) Upload metadata — one document per table.
    oep_upload.upload_metadata(extra_keywords=["my_project"])

    # 6) Verify — compare local row counts with the OEP (optional gate).
    report = oep_upload.verify()
    print(report.format_table())
    return 0 if report.ok else 1


# Re-running just one step later is the same idea — configure, then call it:
#
#     import oep_upload
#     oep_upload.configure(target="local", data_root="data/postprocessed")
#     oep_upload.upload_rows(strategy="replace")   # only re-upload the rows


if __name__ == "__main__":
    raise SystemExit(main())