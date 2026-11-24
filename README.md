# NOAA CEFI regional MOM6 → Arraylake / Icechunk

Scripts and notes for re-ingesting the NOAA CEFI regional MOM6 archive
(`s3://noaa-oar-cefi-regional-mom6-pds`, anonymous access) as Icechunk repos in the Arraylake org `noaa-cefi`.
Almost all data is **virtual**: the repos hold chunk references into NOAA's original NetCDF files, which are never
modified or copied. A small amount is stored **natively** where virtual references were not possible.

## Result

7 repos, restructured in November 2026 after NOAA's review (`REVISIONS.md`, `LOG.md`). Each repo is one model domain + experiment, with the
frequencies as groups: `native/{daily,monthly,yearly}`, `native/static` (the full static set, stored once), and `regrid/{daily,monthly,yearly}`
(only the frequencies a product has). The original 11-repo layout (`raw`/`regrid` per frequency) no longer exists.

| Repo | Contents (release) | Groups |
|---|---|---|
| `nwa12-hindcast` | NWA hindcast, daily + monthly (r20250715) | `native/{daily,monthly,static}`, `regrid/{daily,monthly}`; sea ice nested in `native/monthly/ice`, `native/static/ice`, `regrid/monthly/ice` |
| `nwa12-decadal-forecast` | NWA decadal forecast, monthly (r20250925) + yearly (r20250819) | `native/{monthly,yearly,static}`, `regrid/{monthly,yearly}` |
| `nwa12-seasonal-forecast` | NWA seasonal forecast (r20250710) | `native/{monthly,static}`, `regrid/monthly` |
| `nwa12-seasonal-reforecast` | NWA seasonal reforecast (r20250413) | `native/{monthly,static}`, `regrid/monthly` |
| `nwa12-multi-decadal-outlook` | NWA multi-decadal outlook (r20260331) | `native/{yearly,static}`, `regrid/yearly` |
| `hic6k-hindcast` | Pacific Islands hindcast, daily + monthly (r20260427) | `native/{daily,monthly,static}`, `regrid/{daily,monthly}` |
| `nep10k-hindcast` | NEP hindcast, daily + monthly (r20260701) | `native/{daily,monthly,static}`, `regrid/{daily,monthly}` |

Volume: about 9.9 TB referenced (24.5 TB uncompressed) in the virtual data, plus about 4.2 TB stored natively (14.1 TB uncompressed), almost all
of it the NEP daily 3-D copy. Forecast products keep a forecast-model-run layout (`time` = init, `member`, integer `step` plus a 2-D `valid_time`).
Within a group all cell-position grids (h, q, u, v) share the group; coordinate variable names differ between domains (for example `yh`/`xh` for
the NWA and Pacific Islands, `jh`/`ih` for the NEP) and were not unified.

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
| `ingest/ingest_<region>_<product>.py` | one script per product: the original ingestion that produced the first 11 repos |
| `ingest/fix_nwa_hindcast_daily_statics.py` | replaces a wrong land/sea mask in one repo |
| `ingest/scan_packing_attrs.py`, `static_scan.py`, `repo_packing_scan.py` | check for CF `scale_factor`/`add_offset` (none found) |
| `REVISIONS.md`, `LOG.md` | NOAA's requested revisions with decisions and progress; running log of the restructuring and the code used |
| `ingest/restructure_*.py`, `ingest/rename_repo_by_import.py`, `ingest/delete_old_repos.py` | rebuild the old repos in the new layout, rename a repo without copying data, retire the old repos |
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
