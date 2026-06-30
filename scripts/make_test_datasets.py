#!/usr/bin/env python3
"""Generate realistic CSV test datapackages for oep-upload.

Creates semicolon-delimited CSV files plus an OEMetadata-style ``datapackage.json``
that the oep-upload tool can create + upload directly.

Two datasets:

  simple_all_types/   ~100 rows, one table exercising the common column types
                      (bigint, text, varchar, integer, float, numeric, boolean,
                      date, timestamp, interval, json object, json array).

  energy_model/       research-style modelling data, ~N GB (default ~2 GB):
                        - locations           (~5k rows)
                        - plants              (~20k rows, FK -> locations, json)
                        - load_profiles       (bulk time series -> ~target size,
                                                FK -> plants, json)
                      Foreign keys link the tables (tests FK upload ordering) and
                      every table has an explicit ``id`` (tests id preservation).

All values are CSV; JSON objects/arrays live as quoted cells inside the CSV.

Usage:
  python scripts/make_test_datasets.py                  # both, large ~2 GB
  python scripts/make_test_datasets.py --which simple
  python scripts/make_test_datasets.py --target-gb 0.2  # smaller "large" set
  python scripts/make_test_datasets.py --out /path/to/dir
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

DEFAULT_OUT = "/mnt/d/Arbeit/OEP/test-data/api-data-upload"

META_METADATA = {
    "metadataVersion": "OEMetadata-2.0.4",
    "metadataLicense": {
        "name": "CC0-1.0",
        "title": "Creative Commons Zero v1.0 Universal",
        "path": "https://creativecommons.org/publicdomain/zero/1.0",
    },
}


# --------------------------------------------------------------------------
# datapackage.json helpers
# --------------------------------------------------------------------------
def _field(name: str, type_: str, nullable: bool = True, description: str = "") -> dict:
    return {"name": name, "type": type_, "nullable": nullable, "description": description}


def _fk(local: str, ref_resource: str, ref_field: str = "id") -> dict:
    return {
        "fields": [local],
        "reference": {"resource": ref_resource, "fields": [ref_field]},
    }


def _resource(name: str, fields: list[dict], foreign_keys: list[dict] | None = None) -> dict:
    return {
        "name": name,
        "path": f"data/{name}.csv",
        "type": "table",
        "format": "csv",
        "encoding": "utf-8",
        "dialect": {"delimiter": ";"},
        "profile": "tabular-data-resource",
        "schema": {
            "fields": fields,
            "primaryKey": ["id"],
            "foreignKeys": foreign_keys or [],
        },
    }


def _write_datapackage(pkg_dir: Path, name: str, title: str, resources: list[dict]) -> None:
    pkg_dir.mkdir(parents=True, exist_ok=True)
    (pkg_dir / "data").mkdir(parents=True, exist_ok=True)
    dp = {
        "@context": "",
        "name": name,
        "title": title,
        "description": f"Synthetic test dataset '{name}' for oep-upload.",
        "@id": None,
        "resources": resources,
        "metaMetadata": META_METADATA,
    }
    (pkg_dir / "datapackage.json").write_text(
        json.dumps(dp, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def _writer(path: Path):
    fh = open(path, "w", encoding="utf-8", newline="")
    return fh, csv.writer(fh, delimiter=";", quoting=csv.QUOTE_MINIMAL)


# --------------------------------------------------------------------------
# random data helpers
# --------------------------------------------------------------------------
COUNTRIES = ["DE", "FR", "ES", "IT", "PL", "NL", "DK", "SE", "AT", "CZ"]
PLANT_TYPES = ["wind_onshore", "wind_offshore", "pv_open", "pv_rooftop", "hydro", "biomass"]
FUELS = ["wind", "solar", "water", "biomass", "gas"]
QUALITY = ["measured", "estimated", "interpolated", "forecast"]
WORDS = ["alpha", "beta", "gamma", "delta", "nord", "sued", "ost", "west", "park", "feld"]


def _name(prefix: str, i: int) -> str:
    return f"{prefix}_{random.choice(WORDS)}_{i}"


# --------------------------------------------------------------------------
# dataset 1: simple, all types, ~100 rows
# --------------------------------------------------------------------------
def gen_simple(out: Path, rows: int = 100) -> None:
    pkg = out / "simple_all_types"
    fields = [
        _field("id", "bigint", nullable=False, description="primary key (provided)"),
        _field("name", "text"),
        _field("category", "varchar"),
        _field("count_small", "integer"),
        _field("count_big", "bigint"),
        _field("amount", "float"),
        _field("ratio", "numeric"),
        _field("is_active", "boolean"),
        _field("event_date", "date"),
        _field("created_at", "timestamp"),
        _field("duration", "interval"),
        _field("payload", "json", description="a JSON object"),
        _field("tags", "json", description="a JSON array"),
    ]
    _write_datapackage(
        pkg,
        "simple_all_types",
        "Simple all-types demo",
        [_resource("demo", fields)],
    )

    fh, w = _writer(pkg / "data" / "demo.csv")
    with fh:
        w.writerow([f["name"] for f in fields])
        base = datetime(2024, 1, 1, 0, 0, 0)
        for i in range(1, rows + 1):
            payload = {"unit": "MW", "valid": bool(i % 2), "score": round(random.random(), 3)}
            tags = random.sample(WORDS, k=3)
            w.writerow(
                [
                    i,
                    _name("item", i),
                    random.choice(PLANT_TYPES),
                    random.randint(0, 1000),
                    random.randint(10**6, 10**12),
                    round(random.uniform(-1000, 1000), 4),
                    round(random.uniform(0, 1), 6),
                    bool(i % 3),
                    (date(2024, 1, 1) + timedelta(days=i)).isoformat(),
                    (base + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S"),
                    f"{i % 24:02d}:00:00",
                    json.dumps(payload, ensure_ascii=False),
                    json.dumps(tags, ensure_ascii=False),
                ]
            )
    print(f"  wrote {pkg/'data'/'demo.csv'} ({rows} rows)")


# --------------------------------------------------------------------------
# dataset 2: energy model, large
# --------------------------------------------------------------------------
def gen_complex(out: Path, target_gb: float, n_locations: int, n_plants: int) -> None:
    pkg = out / "energy_model"

    loc_fields = [
        _field("id", "bigint", nullable=False),
        _field("name", "text"),
        _field("country", "varchar"),
        _field("latitude", "double precision"),
        _field("longitude", "double precision"),
        _field("elevation_m", "float"),
        _field("metadata", "json"),
    ]
    plant_fields = [
        _field("id", "bigint", nullable=False),
        _field("name", "text"),
        _field("plant_type", "varchar"),
        _field("fuel", "text"),
        _field("capacity_mw", "float"),
        _field("efficiency", "numeric"),
        _field("commissioned", "date"),
        _field("is_active", "boolean"),
        _field("location_id", "bigint"),
        _field("attributes", "json"),
    ]
    profile_fields = [
        _field("id", "bigint", nullable=False),
        _field("plant_id", "bigint"),
        _field("timestamp", "timestamp"),
        _field("value_mw", "double precision"),
        _field("quality", "varchar"),
        _field("meta", "json"),
    ]
    _write_datapackage(
        pkg,
        "energy_model",
        "Energy model research dataset",
        [
            _resource("locations", loc_fields),
            _resource("plants", plant_fields, foreign_keys=[_fk("location_id", "locations")]),
            _resource("load_profiles", profile_fields, foreign_keys=[_fk("plant_id", "plants")]),
        ],
    )

    # locations
    fh, w = _writer(pkg / "data" / "locations.csv")
    with fh:
        w.writerow([f["name"] for f in loc_fields])
        for i in range(1, n_locations + 1):
            meta = {"grid_node": f"N{i:06d}", "region": random.choice(COUNTRIES)}
            w.writerow(
                [
                    i,
                    _name("loc", i),
                    random.choice(COUNTRIES),
                    round(random.uniform(35.0, 70.0), 6),
                    round(random.uniform(-10.0, 30.0), 6),
                    round(random.uniform(0, 2500), 2),
                    json.dumps(meta, ensure_ascii=False),
                ]
            )
    print(f"  wrote locations.csv ({n_locations} rows)")

    # plants
    fh, w = _writer(pkg / "data" / "plants.csv")
    with fh:
        w.writerow([f["name"] for f in plant_fields])
        for i in range(1, n_plants + 1):
            attrs = {
                "operator": _name("op", i % 500),
                "turbines": random.randint(1, 80),
                "tags": random.sample(WORDS, k=2),
            }
            w.writerow(
                [
                    i,
                    _name("plant", i),
                    random.choice(PLANT_TYPES),
                    random.choice(FUELS),
                    round(random.uniform(0.5, 800.0), 3),
                    round(random.uniform(0.1, 0.6), 5),
                    (date(2000, 1, 1) + timedelta(days=random.randint(0, 9000))).isoformat(),
                    bool(random.getrandbits(1)),
                    random.randint(1, n_locations),
                    json.dumps(attrs, ensure_ascii=False),
                ]
            )
    print(f"  wrote plants.csv ({n_plants} rows)")

    # load_profiles — the bulk; stream until the file reaches target size.
    # Hand-format rows (only the json cell needs CSV-quoting) for speed.
    target_bytes = int(target_gb * (1024**3))
    path = pkg / "data" / "load_profiles.csv"
    header = "id;plant_id;timestamp;value_mw;quality;meta\n"
    # a small pool of pre-serialized json cells, CSV-quoted, rotated per row
    meta_pool = []
    for src in ("sim", "scada", "model"):
        for v in (1, 2, 3):
            s = json.dumps({"src": src, "rev": v}, ensure_ascii=False)
            meta_pool.append('"' + s.replace('"', '""') + '"')

    base = datetime(2018, 1, 1, 0, 0, 0)
    written = 0
    rid = 0
    flush_every = 200_000
    buf = []
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(header)
        size = len(header)
        while size < target_bytes:
            for _ in range(flush_every):
                rid += 1
                plant_id = (rid % n_plants) + 1
                ts = (base + timedelta(hours=rid % 8760)).strftime("%Y-%m-%d %H:%M:%S")
                val = round(random.uniform(0, 900), 4)
                meta = meta_pool[rid % len(meta_pool)]
                buf.append(
                    f"{rid};{plant_id};{ts};{val};{QUALITY[rid % 4]};{meta}\n"
                )
            chunk = "".join(buf)
            fh.write(chunk)
            buf.clear()
            size += len(chunk.encode("utf-8"))
            written = rid
            print(f"    load_profiles: {written:,} rows, {size/1024**3:.2f} GB", flush=True)
    print(f"  wrote load_profiles.csv ({written:,} rows, ~{size/1024**3:.2f} GB)")


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate oep-upload test datapackages.")
    ap.add_argument("--out", default=DEFAULT_OUT, help=f"output dir (default: {DEFAULT_OUT})")
    ap.add_argument("--which", choices=["both", "simple", "complex"], default="both")
    ap.add_argument("--target-gb", type=float, default=2.0, help="approx size of the load_profiles file")
    ap.add_argument("--locations", type=int, default=5000)
    ap.add_argument("--plants", type=int, default=20000)
    ap.add_argument("--simple-rows", type=int, default=100)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    random.seed(args.seed)
    out = Path(args.out).expanduser()
    out.mkdir(parents=True, exist_ok=True)
    print(f"Output dir: {out}")

    if args.which in ("both", "simple"):
        print("Generating simple_all_types ...")
        gen_simple(out, rows=args.simple_rows)

    if args.which in ("both", "complex"):
        print(f"Generating energy_model (~{args.target_gb} GB) ...")
        gen_complex(out, args.target_gb, args.locations, args.plants)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())