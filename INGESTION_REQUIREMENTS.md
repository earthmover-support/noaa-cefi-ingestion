# Ingestion requirements: NOAA CEFI MOM6 -> `noaa-cefi` Arraylake org

(Legend: "user" = the project owner making the decisions; "Claude" = the AI assistant doing the scanning/drafting. Companion files: `INGESTION-PROGRESS.md` (status table, findings, schemas), `SOURCE-FIXES.md`. Source files are never modified.)

Source: `s3://noaa-oar-cefi-regional-mom6-pds` (anonymous, us-east-1; Kerchunk JSONs + NetCDFs).
Target: Arraylake org `noaa-cefi`, bucket config `noaa-oar-cefi-regional-mom6-pds`
(`s3://noaa-oar-cefi-regional-mom6-pds/arraylake/`, i.e. the dedicated prefix already exists).

- Documentation: NOAA's feedback document "CEFI Data in Earthmover Implementation" (Eli Holmes' audit of the pilot archive, plus decision notes on problems P1-P9; held by the project owner) and the existing pilot (`cefi/README.md`, `cefi/scripts/`).
- Ingestion type: **virtual** (chunking inherited from source; no rechunking).
- Re-ingest the 8 NOAA-PMEL pilot products plus the products added since (NWA multi-decadal outlook, Pacific Islands daily and monthly): 11 repos in `noaa-cefi`. NOAA-PMEL is left untouched.
- Layout: FMRC for forecast products where appropriate.
- Fixes/tweaks per the feedback doc: see the decisions below; per-product handling is in `INGESTION-PROGRESS.md`.
- VCAP: create as needed in `noaa-cefi` (none exists yet; org has no repos or VCAPs).
- Compute: local machine + Coiled account (confirm before launching any cluster).

## Decisions (user; dates in brackets where later)

- **Source files are never modified.** All fixes happen in the Icechunk store only.
- **Source:** parse the NetCDFs directly (not the Kerchunk JSONs); netCDF3 statics need a netCDF3-capable parser.
- **Scope:** all products, but start with a couple, recording progress and findings as we go.
- **P1 chunking:** ignore (virtual inherits source chunks).
- **P2 (NEP daily 4-D, one file/year, time chunk 100 not dividing 365/366):** do NOT split into per-file groups.
  Leave those variables out and flag as a TODO to revisit once variable-length chunks are supported.
  **SUPERSEDED 2026-10-27 (user): ingest them as NATIVE chunks via Xarray on Coiled; DONE, see the decision below.**
- **P3 volcello/vollcello:** rename `vollcello` -> `volcello` in the store (no source edits). NEP daily has BOTH series as separate per-year files (33 each, per grid); both are per-year 4-D, so P2 excludes them for now, and the name clash must be resolved (check whether the two series hold identical data) when P2 is solved. The bundled `volcello` copies inside other variables' files are left out (decision of 2026-10-22 below).
- **P4 statics:** fold geolon/geolat/deptho/areacello/wet into each raw group. Small coordinates/statics are
  copied as native chunks (not virtual), so netCDF3 and the masked-coordinate problem are not blockers.
- **P5 NWA hindcast monthly regrid (two longitude grids):** keep all variables; the 37 sea-ice variables go in a separate `regrid/ice` group (decision of 2026-10-22 below). **P6 multi-decadal (one scenario missing a year):** `T_adx` SSP585 written as native chunks with 2045 = NaN (decision of 2026-10-22 below).
- **P7 decadal forecasts:** FMRC layout (user: "we will use FMRC this time where appropriate"); integer lead step + 2-D valid_time.
- **P8 seasonal forecast/reforecast (user-confirmed 2026-10-20):** FMRC cube per grid; KEEP the bundled `<var>_anom` fields as ordinary variables
  (36 vars forecast, 8 reforecast); `step` = integer months 0..11 with 2-D `valid_time` (not timedelta64); normalize both products to
  (member, step, y, x) (forecast via the Zarr `transpose` codec on every file); fill unified to NaN (verified safe on primaries);
  per-file `valid_time`/`init` replaced by the derived `time`/`valid_time`.
- **Volume bookkeeping:** for every product/group record uncompressed and compressed data volume (and file
  counts), in case we later choose native ingestion.
- **Fix-list bookkeeping:** for every product record what would need fixing in the ORIGINAL files for clean
  virtual ingestion (e.g. P1 chunk shapes, P2 per-year time chunks, P3 bundled vars, P4 netCDF3 statics,
  P8 fill values/dim order/missing member, P9 JSONs). Keep it in `SOURCE-FIXES.md`.

## Decision (2026-10-20, user): NWA seasonal forecast init 202510
Init 202510 (9 members, member 6 missing; different chunk shapes) is written as NATIVE chunks into the same Icechunk arrays, rather than
excluded or split into its own group. Both grids. Written with `xarray.Dataset.to_zarr(region=...)` (user's suggestion). The other inits stay virtual.
Source files untouched; this is the one deliberate exception to "all data virtual" and is noted in repo metadata and SOURCE-FIXES.md.

## Decision (2026-10-20, user): when to run step-5 checks
Do NOT build/run the step-5 assumption checks (pandera.xarray schema validation per product, coordinate agreement across all files,
`_anom` fill check, `ocean_static.nc` grid check, `valid_time` == init + lead months, `month` variable inspection, decadal lead-vector check,
final codec-chain assertion) as standalone work now. Do them only as part of the real ingestion of each product. Header-level scans and
planning continue in the meantime.

## NOTE: NEP hindcast daily 4-D variables (P2) - cannot be done virtually today
The per-year 4-D variables of `northeast_pacific/.../hindcast/daily` (raw: dissic no3 o2 po4 si so talk thetao uo vo volcello vollcello; regrid: same minus uo, vo)
are stored as 33 yearly files with time chunk 100 and 365/366 steps per file. A Zarr array needs a regular chunk grid, so the years cannot be joined into one
virtual array (chunk boundaries do not line up with file boundaries).
- **Virtual ingestion requires variable-length chunks** (not supported yet); until then these variables are left out. TODO: revisit when available.
- **Native ingestion would require rewriting ~6.5 TB of compressed data** (raw 5,185 GB + regrid 1,278 GB on disk, upper bound incl. bundled
  volcello/vollcello copies; ~15 TB uncompressed: raw 8.4 TB excl. bundled copies, regrid 7.0 TB).
- Alternative (declined): one group per yearly file. Upstream fix: NOAA rewrites files with a time chunk dividing the series.
- Effect: the NEP daily repo holds only the 16 full-period 2-D variables per grid (~3% of the data); the 3-D ocean state (temperature, salinity, nutrients, velocities) is missing.

## Decision (2026-10-22, user): NWA hindcast monthly regrid (P5)
Ingest ALL regrid variables. The 37 sea-ice variables (own longitude grid, start -98.0 / step 0.0801) go in a separate `regrid/ice` group; the 427 ocean variables in `regrid/main`.
This replaces the earlier unconfirmed proposal to skip NWA monthly regrid. raw/ice lon/lat: use the ocean static geolon/geolat only if verified equal at ingestion time.

## Decision (2026-10-22, user): bundled volcello copies
LEAVE OUT the bundled `volcello` arrays that sit inside other variables' files (NEP monthly raw 62 files, NWA monthly raw 62 files; NEP daily bundled copies are per-year and excluded anyway).
The standalone `volcello` file (one per grid) IS ingested as a normal variable. Nothing is deleted from the source; the store just does not reference the bundled copies.
This confirms the earlier Claude proposal (the unverified-duplicate caveat above no longer gates it: the user's decision is unconditional).

## Process rule (user, 2026-10-22)
ALWAYS write out (draft) the schema for a dataset before moving on to scan the next dataset. Order per product: scan -> record findings -> draft schema -> user confirms -> next product.

## Decision (2026-10-22, user): nwa decadal forecast monthly
FMRC cube per grid as drafted. Time fields (average_T1/T2/DT, time_bnds) stored once per (time, step) after verifying member rows are identical; raw geolon/geolat/statics taken from the NWA hindcast monthly ocean_static.nc (verified against this release at ingestion). ice_static of this release is the wrong region and is not used.

## Decision (2026-10-22, user): NWA multi-decadal outlook T_adx SSP585 (P6)
T_adx SSP585 (raw; missing year 2045) is written as native chunks with 2045 = NaN into the same scenario-stacked array; the other scenarios stay virtual. See INGESTION-PROGRESS.md.

## Note (2026-10-22, user): pacific_islands monthly raw chunking
~1 MB 4-D chunks (10,10,50,50) and ~3 million virtual chunk references are acceptable.

## Decision (2026-10-26, user): NWA statics must match the data's land/sea mask
**Finding.** The NWA hindcast release (r20250715) `ocean_static.nc` (daily and monthly share the same file) marks 288 cells as land (`wet` = 0, `deptho` masked) where the hindcast data itself has values: the data has 391,722 ocean cells, the static 391,434. The NWA decadal-forecast (r20250819) static matches the data exactly (391,722) and so do the seasonal releases' own statics. The multi-decadal outlook ships no static and has the same 391,722-cell mask. So all NWA model output uses one mask; the hindcast static is the odd one out.
**Rule for NWA repos.** `deptho`, `areacello` and `wet` must reproduce where the data has values. Every ingest script asserts this against the first month of a 2-D variable (0 mismatches in both directions). Sources: coordinates (`geolon`, `geolat`, `geolon_c`, `geolat_c`) from a gap-free static of the same grid (the release's own, else the NWA hindcast monthly static); `deptho`/`areacello`/`wet` from the NWA decadal-yearly r20250819 static (netCDF4) whenever the release's own mask does not match its data. 1e20 "no data" is stored as NaN; each field's attrs record its source.
**Applied to:** decadal yearly and monthly (own static for the mask, coordinates from the hindcast static), seasonal forecast and reforecast (own statics: match), multi-decadal outlook (user decision 2026-10-26), NWA hindcast monthly (this decision, user 2026-10-26). **NWA hindcast daily was written BEFORE the check existed with the mismatching hindcast static; user decided (2026-10-26) to replace its raw `deptho`/`areacello`/`wet` in place: DONE, commit `4FHQ979N4KNE8K3QEJ80` (`ingest/fix_nwa_hindcast_daily_statics.py`).**
Consequence to note: the decadal static's `areacello` is masked (NaN) over land (235,107 cells) whereas the hindcast static's `areacello` is defined everywhere; following the decision, `areacello` comes from the decadal static in every NWA repo, so land cells carry no cell area.

## Decision (2026-10-26, Claude, reversible; user informed): six unreadable arrays in NWA hindcast monthly are excluded
Raw `vmo_rotate` and regrid `dissic`, `talk`, `volcello`, `psm`, `q_p_2_n_Sm` have an empty chunk index in their source files (HDF5: 0 chunks, 0 bytes of storage; reads return fill) although the files are 3-14 GB, so there is nothing for a virtual reference to point at. They are left out of the repo (same treatment as the empty Pacific Islands files), listed in the repo tag `excluded` and in SOURCE-FIXES.md. Effect: regrid has no `volcello`, `dissic` or `talk` (raw has them). Possible recovery (NOT attempted): scan the files for orphaned index nodes. They can be added later if NOAA regenerates the files. User note (2026-10-26): corrupted HDF5 writes like these are an issue that would be avoided if NOAA wrote directly to Icechunk instead (atomic commits); recorded as a recommendation in SOURCE-FIXES.md.

## Decision (2026-10-27, user): NEP daily per-year 3-D variables are ingested as NATIVE chunks (supersedes the P2 exclusion)
"I have decided that for now the data that we were unable to ingest due to variable-length chunks should just be ingested as native chunks instead. This should be done via Xarray on Coiled."
- Done in `noaa-cefi/cefi-nep-hindcast-daily`, groups `raw` (11 arrays: dissic no3 o2 po4 si so talk thetao uo vo volcello) and `regrid` (9 arrays: same minus uo, vo), next to the virtual 2-D variables. Script: `ingest/ingest_nep_hindcast_daily_3d_native.py`.
- `vollcello` is NOT stored: it is identical to `volcello` in 462 of 462 sampled source chunk windows per grid (Claude decision, reversible: the script's `vollcello_decision` drops it only when identical). Bundled `volcello` copies inside other variables' files are not read (decision of 2026-10-22).
- Layout: time = the repo's existing 12,053-day axis (each yearly file placed by date and checked against it); zarr chunks (30 days, 10 levels, 200, 200) float32, Blosc(zstd, clevel 3, shuffle); zarr `fill_value` = the stored sentinel (raw 1e20, regrid NaN); native `z_l` coordinate added to both groups.
- This is the temporary alternative to variable-length chunks: the native copy duplicates ~14 TB uncompressed (4.15 TB stored) that the virtual stores only reference. When variable-length chunks (or the NOAA fix of writing time chunks that divide the series) exist, the virtual version would replace it.
