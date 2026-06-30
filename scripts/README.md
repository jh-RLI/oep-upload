# scripts

Developer helper scripts (not part of the installed package).

## make_test_datasets.py

Generates realistic CSV **datapackages** for testing oep-upload — semicolon CSV
plus an OEMetadata-style `datapackage.json` the tool can create + upload directly.

It produces two datasets:

- **`simple_all_types/`** — ~100 rows, one table covering the common column types
  (`bigint`, `text`, `varchar`, `integer`, `float`, `numeric`, `boolean`, `date`,
  `timestamp`, `interval`, plus a JSON object and a JSON array column).
- **`energy_model/`** — research-style modelling data (~2 GB by default):
  `locations` → `plants` → `load_profiles` (a bulk time series). The tables are
  linked by **foreign keys** (exercises FK upload ordering) and every table has an
  explicit `id` (exercises id preservation). JSON objects live as quoted cells.

```bash
# both datasets, large set ~2 GB (default output dir is the mounted drive)
python scripts/make_test_datasets.py

# just the small one
python scripts/make_test_datasets.py --which simple

# a smaller "large" set, custom location
python scripts/make_test_datasets.py --which complex --target-gb 0.2 --out /tmp/td

# tune sizes
python scripts/make_test_datasets.py --locations 5000 --plants 20000 --seed 42

# prefix table (resource) names — recommended before uploading to a shared/prod
# OEP so the tables are clearly marked test data and don't collide
python scripts/make_test_datasets.py --prefix test_
```

`--prefix` is applied to every resource (table) name, its CSV file name, and the
foreign-key references — e.g. `test_locations`, `test_plants`, `test_load_profiles`.

Default output: `D:\Arbeit\OEP\test-data\api-data-upload`
(`--out` to change). Uses only the Python standard library; deterministic via
`--seed`.

> The field `type` names come from `oem2orm`'s supported set
> (`bigint`, `text`, `varchar`, `integer`, `float`, `numeric`, `boolean`, `date`,
> `timestamp`, `interval`, `json`, …). If a future oem2orm version drops one,
> adjust the affected field in the generator.
