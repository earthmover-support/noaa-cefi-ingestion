# NOAA CEFI regional MOM6 → Arraylake / Icechunk

Scripts and notes for re-ingesting the NOAA CEFI regional MOM6 archive
(`s3://noaa-oar-cefi-regional-mom6-pds`, anonymous access) as Icechunk repos in the Arraylake org `noaa-cefi`.
Almost all data is **virtual**: the repos hold chunk references into NOAA's original NetCDF files, which are never
modified or copied. A small amount is stored **natively** where virtual references were not possible.

## Result

11 repos, written and validated against the source files.

| Repo | Product (release) |
|---|---|
| `cefi-nwa-decadal-forecast-yearly` | NWA decadal forecast, yearly (r20250819) |
| `cefi-nwa-decadal-forecast-monthly` | NWA decadal forecast, monthly (r20250925) |
| `cefi-nwa-seasonal-forecast-monthly` | NWA seasonal forecast (r20250710) |
| `cefi-nwa-seasonal-reforecast-monthly` | NWA seasonal reforecast (r20250413) |
| `cefi-nwa-hindcast-daily` | NWA hindcast, daily (r20250715) |
| `cefi-nwa-hindcast-monthly` | NWA hindcast, monthly (r20250715) |
| `cefi-nwa-multi-decadal-outlook-yearly` | NWA multi-decadal outlook (r20260331) |
| `cefi-pci-hindcast-daily` | Pacific Islands hindcast, daily (r20260427) |
| `cefi-pci-hindcast-monthly` | Pacific Islands hindcast, monthly (r20260427) |
| `cefi-nep-hindcast-daily` | NEP hindcast, daily (r20260701) |
| `cefi-nep-hindcast-monthly` | NEP hindcast, monthly (r20260701) |

Volume: about 9.9 TB referenced (24.5 TB uncompressed) in the virtual repos, plus about 4.2 TB stored natively
(14.1 TB uncompressed), almost all of it the NEP daily 3-D copy. Forecast products use a forecast-model-run-collection
layout (`time` = init, `member`, integer `step` plus a 2-D `valid_time`); each repo has `raw` and `regrid` groups.

## What is not virtual or not included

- **NEP daily per-year 3-D variables** (20 arrays) are a native copy. Their per-year files use a time chunk of 100 that
  does not divide 365/366, which needs variable-length chunks to reference virtually. See
  `ingest/ingest_nep_hindcast_daily_3d_native.py`.
- NWA seasonal forecast init 202510 and multi-decadal `T_adx` SSP585 are native (small).
- **Not ingested:** six NWA hindcast-monthly arrays and one decadal-monthly file whose source files have an empty
  chunk index; duplicate `volcello`/`vollcello` series; older releases.

## Layout

| Path | Contents |
|---|---|
| `INGESTION_REQUIREMENTS.md` | requirements and the decisions made, per issue |
| `INGESTION-PROGRESS.md` | full working log: scans, schemas, validation results, commit IDs |
| `SOURCE-FIXES.md` | what would need fixing in the original files, with suggestions for NOAA/GFDL |
| `ingest/ingest_<region>_<product>.py` | one script per product |
| `ingest/fix_nwa_hindcast_daily_statics.py` | replaces a wrong land/sea mask in one repo |
| `ingest/scan_packing_attrs.py`, `static_scan.py`, `repo_packing_scan.py` | check for CF `scale_factor`/`add_offset` (none found) |
| `RESTRUCTURING.md` | how to rename, move, re-coordinate and combine the repos, and change their descriptions |
| `utilities.py`, `test_utilities.py` | the functions `RESTRUCTURING.md` uses, and their tests |

## Running the scripts

Each script is a self-contained [uv](https://docs.astral.sh/uv/) script with inline dependencies:

```bash
./ingest/ingest_nwa_hindcast_daily.py --help
```

Typical phases are `--parse` (read the source headers into virtual references, run on a Dask cluster), `--dry-run`
(in-memory repo plus validation), `--write` (commit to Arraylake) and `--validate` (compare committed chunks with
the source files by hash). Parse results are cached under `ingest/.cache/` (not kept).

The scripts assume an Arraylake login with access to the `noaa-cefi` org, and a Coiled workspace and software
environment (`earthmover-devs`, `cefi-ingest-2`) that you will need to replace with your own.

## Validation

Chunk windows were hashed from the source files and compared with the committed stores, element classes were checked
for fill values, and each repo was opened with xarray as a user would. Details per product are in
`INGESTION-PROGRESS.md`.
