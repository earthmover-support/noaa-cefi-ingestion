# Running log: restructuring the NOAA CEFI repos

Newest entries last. Each entry says what was done, what was found and which code was used. Decisions and the task list are in
`REVISIONS.md`; the original ingestion log is `INGESTION-PROGRESS.md`. Snapshot IDs are the commit IDs of `main` in Arraylake.

## 2026-11-09 to 11-22: checks and housekeeping after the first ingestion

- **CF packing attributes (`scale_factor`, `add_offset`).** Scanned the HDF5 headers of all 7,576 source files (60,971 datasets), plus the 10
  netCDF3 static files. None carries packing attributes, `valid_*` attributes or the HDF5 scale-offset filter. Recorded as section 6c of
  `INGESTION-PROGRESS.md`. Code: `ingest/scan_packing_attrs.py` (Coiled, header reads only), `ingest/static_scan.py`, `ingest/repo_packing_scan.py`.
- **Icechunk suggestion to NOAA softened.** `SOURCE-FIXES.md` now lists the seven silently empty source arrays first, cheaper fixes next, and
  Icechunk as an option to consider, with the cause stated as unconfirmed.
- Notes, scripts and a README were put in this repository.

## 2026-11-23: restructuring utilities

`RESTRUCTURING.md`, `utilities.py` and `test_utilities.py` were added (commit `1a20fd8`): rename variables, set coordinates, move arrays and
groups, and `copy_node`, which copies a group or array between repos (virtual chunks stay virtual references, native chunks are copied).

## 2026-11-24: NOAA's requested revisions

NOAA reviewed the first ingestion. The distinct tasks, decisions and open questions are in `REVISIONS.md`. Main decisions: one repo per model
domain + experiment (11 repos become 7, named `<domain>-<experiment>` with `nwa12`, `nep10k`, `hic6k`); layout `native`/`regrid`, each with
daily / monthly / yearly groups; all cell-position grids in one group; a `static` group holding the full static set, stored once per repo;
older releases deferred.

**Why only a few statics were ingested the first time.** The NEP plan listed "core 5 vs everything" as an open choice and the confirmed
schema took the core five plus `geolon_c`/`geolat_c`. It was a scoping choice carried over from the pilot, not a source limitation: all of
NOAA's listed variables are in the `ocean_static` / `ice_static` files in the bucket.

### Inventory of the first three repos (read-only)

Code: `ingest/inventory_repos.py REPO [REPO ...]` lists per group the arrays, dimension signatures, static-like names and `coordinates`
attributes. For the multi-decadal outlook, the names shared by `raw/main`, `raw/u_grid` and `raw/v_grid` (`time`, `scenario`, `z_l`, `xh`,
`xq`, `yh`, `yq`) have identical values; only attributes differ (the `raw/main` copies of `xh`, `yh`, `xq`, `yq` had no `units`,
`long_name`, `axis`; `time` had `bounds` only in `raw/main`).

### Rebuilt: `nwa12-seasonal-forecast`, `nwa12-seasonal-reforecast`, `nwa12-multi-decadal-outlook`

Code: `ingest/restructure_nwa12_seasonal.py --product forecast|reforecast [--build] [--validate] [--promote]` and
`ingest/restructure_nwa12_multidecadal.py [--build] [--validate] [--promote]`. They need an Arraylake login and read only the small static
files from the NOAA bucket; no data files are read. Each build creates the repo, works on a branch `restructure`, copies the old repo's groups
with `utilities.copy_node`, writes `native/static` from the product's static file, validates against the old repo, and only `--promote`
moves `main` to the branch. Builds are resumable. The old repos are not touched.

| New repo | `main` snapshot | Layout |
|---|---|---|
| `nwa12-seasonal-forecast` | `5F1XGAP9A4WR7SEQ0SCG` | `native/monthly`, `native/static`, `regrid/monthly` |
| `nwa12-seasonal-reforecast` | `XA8CSJD92HKR89BF5T6G` | same |
| `nwa12-multi-decadal-outlook` | `SSXF75836KA3YX3HK71G` | `native/yearly` (raw main + u_grid + v_grid merged), `native/static`, `regrid/yearly` |

Checks, per repo against the old one: every array's metadata and chunk references identical (seasonal forecast 4,320 virtual references and
1,216 native chunks; reforecast 96,960 and 28; multi-decadal 72,684 and 1,155), `native/static` equals the source static file, every group opens
with xarray, and sampled values read through xarray (first and last init, SSP126 / SSP585) are identical in old and new. A first version of
the validation compared every native chunk byte for byte; it used too many resources on the laptop and was replaced by these lighter checks.

Findings:
- **The old seasonal forecast and reforecast repos had land stored as the literal `1e20` in `areacello`, `deptho` and `wet`**, while their
  `_FillValue` attribute said NaN, so xarray returned `1e20` as data. The build asserts the old arrays equal the source on every valid cell
  and are exactly `1e20` on the fill cells, then writes NaN. The multi-decadal repo already had NaN.
- NOAA ships no static file for the regridded grid, so there is no `regrid/static`.
- `geolon`/`geolat` (and `_u`/`_v` where the group has u and v variables) are in the data group as well as in `native/static`, because the data
  variables name them in their `coordinates` attribute. The multi-decadal u-grid and v-grid variables now list `geolon_u`/`geolat_u` and
  `geolon_v`/`geolat_v` (they had no 2-D coordinates).
- Static sources: seasonal products use their own `ocean_static.nc` (land 1e20 to NaN); the geolon/geolat family comes from the NWA hindcast
  monthly r20250715 static (no land gaps; asserted equal wherever the product's own file is unmasked). The multi-decadal release ships no
  static file: fields from the NWA decadal-forecast yearly r20250819 static (its mask matches the data), coordinates from the hindcast static.
- Repo descriptions are limited to 255 characters (Arraylake).

### Rebuilt: `nwa12-decadal-forecast` (2026-11-24)

Merges `cefi-nwa-decadal-forecast-monthly` (r20250925) and `cefi-nwa-decadal-forecast-yearly` (r20250819). Code:
`ingest/restructure_nwa12_decadal.py [--build] [--validate] [--promote]` (same branch / validate / promote flow as above; it reuses the
static-file readers of the multi-decadal script). `main` snapshot `Y8CSZS53EXDM8HQDRP90`. Layout: `native/monthly`, `native/yearly`,
`native/static`, `regrid/monthly`, `regrid/yearly`.

Checks: 90 arrays compared against the two old repos (591,136 virtual references identical, 70 native chunks present with identical lengths),
`native/static` equal to the source files, all five groups open with xarray, and sampled values (first and last init, monthly and yearly)
identical in old and new.

Findings:
- **The monthly and yearly static files are not identical.** `deptho` is identical. `areacello` and `wet` agree on every cell valid in both,
  but differ on land cells: 2,665 cells are valid only in the yearly file and 9,356 only in the monthly file. Both products' ocean mask
  (`wet == 1`, `deptho > 0`, 391,722 cells at the surface) equals the data mask exactly with either static. One `native/static` serves both
  frequencies, built from the yearly r20250819 static as in the other NWA repos; the 9,356 land-cell values that only the monthly file had are
  therefore NaN in the new repo.
- **Audit of the 1e20 fill problem across all repos** (`ingest/audit_native_fill.py`, read-only: natively stored float arrays containing
  1e20). It is a bug only where `_FillValue` says NaN but the values are 1e20, which was the case in `cefi-nwa-decadal-forecast-monthly`
  (`areacello`, `deptho`, `wet`) and the seasonal forecast and reforecast repos. `cefi-nwa-decadal-forecast-yearly` and both Pacific Islands repos
  also hold 1e20 on land in those three arrays, but declare 1e20 as `_FillValue`, so xarray masks them correctly. The new repos have NaN everywhere; all other repos have no such arrays.

### Rebuilt: `hic6k-hindcast` (2026-11-24)

Merges `cefi-pci-hindcast-daily` and `cefi-pci-hindcast-monthly` (both r20260427). Code: `ingest/restructure_hic6k.py [--build] [--validate]
[--promote]`, with the subgroup merge and static helpers in `ingest/restructure_common.py` (`merge_subgroups`, `check_moved`,
`add_uv_coordinates`). `main` snapshot `3ZT9VVNNWG7071NVCK80`. Layout: `native/daily`, `native/monthly`, `native/static`, `regrid/daily`,
`regrid/monthly`.

Checks: 821 arrays compared against the two old repos (3,834,432 virtual references identical, 33 native chunks present with identical
lengths), `native/static` equal to the source file, all seven groups open with xarray, and sampled values for a tracer, a u-grid and a v-grid
variable (first and last step, daily and monthly) identical in old and new.

Findings:
- The Pacific Islands `ocean_static.nc` files of the daily and monthly releases are identical (asserted), so one `native/static` serves both.
  Its 1-D axes: `yq` has 539 points and `xq` 726 (the q grid has no extra row here), so the (539, 726) shape is shared by the q-grid
  (`yq`, `xq`) and u-grid (`yh`, `xq`) fields; the build assigns dimensions by field name, not by shape.
- The static file also has three int8 masks (`mask`, `mask_u`, `mask_v`) and a length-1 `time` axis; the masks are kept, `time` is not a
  static field and is left out.
- The old repos' `areacello`/`deptho`/`wet` held 1e20 on land but declared 1e20 as `_FillValue` (so xarray masked them); the new ones use NaN
  like the other repos. `write_static` now adds the NaN `_FillValue` attribute to float arrays only.

Next: `nwa12-hindcast` (daily + monthly) needs a decision on the sea-ice grids first (see REVISIONS.md); then `nep10k-hindcast` (4.2 TB native).
