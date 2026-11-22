# NOAA CEFI MOM6 -> Arraylake (`noaa-cefi` org): ingestion progress

**What this is.** Working record for re-ingesting the NOAA CEFI regional MOM6 data (`s3://noaa-oar-cefi-regional-mom6-pds`, anonymous) as VIRTUAL Icechunk repos in the Arraylake org `noaa-cefi`
(repos under the bucket config `noaa-oar-cefi-regional-mom6-pds`, prefix `arraylake/`), replacing the earlier pilot in the `NOAA-PMEL` org, with tweaks from NOAA's feedback document ("CEFI Data in Earthmover Implementation": audit findings P1-P9).
Companion files: `INGESTION_REQUIREMENTS.md` (decisions and rules) and `SOURCE-FIXES.md` (what NOAA would need to fix in the source files).
**Legend:** "user" = the project owner who makes the decisions (Earthmover); "Claude" = the AI assistant that scanned the bucket and drafted the schemas. Source files are never modified.

**Status (2026-10-23): scanning and planning are DONE for all products. Ingestion is in progress, one product at a time: ALL 11 written and validated (the last, `cefi-nwa-hindcast-monthly`, on 2026-10-26); see the "Ingestion log" at the bottom.** Steps 5-7 of the ingestion checklist (assumption checks, execution, validation) are deliberately deferred to the real ingestion of each product (user decision); the per-product step-5 checks are listed in each plan section below.
Sizes: "uncompressed" is what a native ingest would hold before compression; "on disk" is the source NetCDF size. Both for newest release, raw / regrid.

| Product (release) | Proposed repo | Layout (confirmed) | Left out / special handling | Uncompressed (raw / regrid) | On disk (raw / regrid) |
|---|---|---|---|---|---|
| NEP hindcast monthly (r20260701) | cefi-nep-hindcast-monthly **(DONE 2026-10-26)** | raw: main, u_grid, v_grid, b_x, b_y; regrid (flat) | 62 bundled `volcello` copies (1,425 GB) | 1,797 GB / 1,642 GB | 1,183 / 332 GB |
| NEP hindcast daily (r20260701) | cefi-nep-hindcast-daily **(DONE 2026-10-26)** | raw, regrid (flat; see group-naming rule); 16 two-D vars | ALL per-year 4-D variables (P2): need variable-length chunks (virtual) or ~6.5 TB rewrite (native) | 215 GB / 214 GB (2-D subset) | 98.7 / 58.0 GB (2-D subset) |
| NWA hindcast monthly (r20250715) | cefi-nwa-hindcast-monthly **(DONE 2026-10-26)** | raw: main, u_grid, v_grid, ice, ice_xB, ice_yB; regrid: main + ice (P5: 37 ice vars on a different lon grid) | 62 bundled `volcello` copies (3,142 GB) | 4,280 GB / 3,949 GB | 2,177 / 1,250 GB |
| NWA hindcast daily (r20250715) | cefi-nwa-hindcast-daily **(DONE 2026-10-23)** | raw, regrid (flat; see group-naming rule), 11 vars | none | 326 GB / 325 GB | 131.7 / 133.4 GB |
| NWA seasonal forecast (r20250710) | cefi-nwa-seasonal-forecast-monthly **(DONE 2026-10-23, groups `raw`/`regrid`)** | FMRC (time=init, member, step=months, valid_time); 36 vars incl. `_anom` | init 202510 (member 6 missing, different chunks) written as NATIVE chunks | 44 GB / 44 GB | 16.5 / 18.9 GB |
| NWA seasonal reforecast (r20250413) | cefi-nwa-seasonal-reforecast-monthly **(DONE 2026-10-23, groups `raw`/`regrid`)** | FMRC, 120 inits; 8 vars incl. `_anom` | fill unified to NaN | 302 GB / 301 GB | 105.9 / 127.1 GB |
| NWA decadal forecast yearly (r20250819) | cefi-nwa-decadal-forecast-yearly **(DONE 2026-10-22)** | FMRC (61 inits, step=lead year, valid_time); built earlier on the pilot `fmrc` branch | none | 96 GB / 96 GB | 37 / 38 GB |
| NWA decadal forecast monthly (r20250925) | cefi-nwa-decadal-forecast-monthly **(DONE 2026-10-23, groups `raw`/`regrid`)** | FMRC (61 inits, member 10, step 120 months) | raw coords from the NWA hindcast monthly ocean_static (this release's own is masked / wrong region) | 3,451 GB / 3,443 GB | 1,400 / 1,418 GB |
| NWA multi-decadal outlook (r20260331) | cefi-nwa-multi-decadal-outlook-yearly **(DONE 2026-10-26)** | scenario-stacked (SSP126/245/370/585 x time 131) | `T_adx` SSP585 (missing year 2045) written as NATIVE chunks with 2045 = NaN | 727 GB / 151 GB | 287.5 / 41.2 GB |
| Pacific Islands hindcast daily (r20260427) | cefi-pci-hindcast-daily **(DONE 2026-10-23)** | raw: main, u_grid, v_grid; regrid (flat) | none | 339 GB / 300 GB | 271.5 / 161.8 GB |
| Pacific Islands hindcast monthly (r20260427) | cefi-pci-hindcast-monthly **(DONE 2026-10-26)** | raw: main, u_grid, v_grid; regrid (flat) | 2 EMPTY variables (`T_adx_2d`, `ffedet_btm`); 66 bundled `volcello` copies (2,124 GB) | 2,610 GB / 239 GB | 2,987 / 123 GB |

(Repo names are proposals. Pacific Islands is a draft product outside NOAA's audit.)

**Scan method (the helper scripts were session-local and are NOT preserved).** (1) Inventory: list every object in the bucket with obstore (26,707 objects, 36.95 TB) and group by region / experiment / frequency / grid / release.
(2) Header scan: read HDF5 metadata only (shape, chunks, dtype, compression level, shuffle, fill value, units) with h5py over an obstore reader (256 KB buffer), 6 worker processes, ~1.5 files/s. VirtualiZarr manifest parsing was far too slow for multi-GB files (~80 min for 900 files) and threaded h5py serializes on its global lock.
(3) Small targeted reads: coordinate arrays (lon/lat per regrid file, `lead`, `member`, `time`), and the netCDF3 static files via scipy.
(4) Per-variable codec/chunk comparison across files, because virtual concatenation needs one identical codec chain per variable.

---

## Earlier work (2026-09-28): NOAA-PMEL decadal-yearly FMRC branch
Target: branch `fmrc` of `NOAA-PMEL/cefi-nwa-decadal-forecast-yearly` (branched from `main`).

Target: branch `fmrc` of `NOAA-PMEL/cefi-nwa-decadal-forecast-yearly` (branched from `main`).
Script: `cefi/scripts/build_decadal_fmrc.py` (reuses helpers from `cefi/scripts/build_cefi.py`).

### Checklist (earlier work)

- [x] 1. Requirements: one FMRC cube per grid (`raw`, `regrid`) replacing the 61 per-init
      `i<YYYY>01` groups; integer lead-year `step`; virtual refs into NODD; new branch, main untouched.
- [x] 2. Read access: NODD `s3://noaa-oar-cefi-regional-mom6-pds` anonymous via obstore; Arraylake repo readable.
- [x] 3. Scan: `northwest_atlantic/full_domain/decadal_forecast/yearly/{raw,regrid}/r20250819/`,
      one Kerchunk JSON per (var, init): 6 vars x 61 inits (i196501..i202501) per grid.
- [x] 4. Plan (shape confirmed with user in conversation):
      - regrid: (time: 61, member: 10, step: 10, lat: 844, lon: 774), chunks (1, 4, 4, 422, 387)
      - raw:    (time: 61, member: 10, step: 10, yh: 845, xh: 775), chunks (1, 10, 10, 200, 200) + statics
- [x] 5. Assumptions checked on main: identical vars/shapes/chunks/codecs/fill across all 61 inits,
      identical member/lat/lon; only `lead` differs (float "days since <init>", 4 distinct vectors
      due to leap years; `lead // 365 == 0..9` for all inits).
- [x] 6. Execute: in-memory dry run (full 61 inits) passed; branch `fmrc` created at main
      `K5BRTR7JBF5582H2TVS0`, committed `MJTF0ZJVDDSQ2X9D58B0` (2026-09-28).
- [x] 7. Validate: committed branch vs `main` per-init groups: all 61 inits, every var (point per
      init + full slabs for i1965/i1968/i2025), valid_time == decoded lead, raw statics identical.

### Notes (earlier work)

- 3-init prototype round-trip (in-memory Icechunk) was bit-identical to main, incl. member/lead edge chunks.
- Source Kerchunks inline only `lead/0` and `member/0` (coordinates, loaded anyway).
- raw `average_T1/T2`, `time_bnds` are per-init "days since" floats -> decoded to datetimes, written natively.
- raw statics (geolon, geolat, deptho, areacello, wet) copied verbatim from `main:raw/i196501`.
- NODD occasionally drops connections mid-read (`connection closed before message completed`);
  validation retries those.

---

# Re-ingest into `noaa-cefi` org (started 2026-10-19)

Requirements: `INGESTION_REQUIREMENTS.md`. Source of tweaks: feedback PDF (P1-P9).

- [x] 1. Requirements collected (virtual; FMRC for forecasts; Coiled available).
- [x] 2. Read access: Arraylake bucket config `noaa-oar-cefi-regional-mom6-pds` in `noaa-cefi`
      (url `s3://noaa-oar-cefi-regional-mom6-pds/arraylake/`, read store lists empty = no repos yet);
      public bucket lists anonymously via obstore (`northeast_pacific`, `northwest_atlantic`, `pacific_islands`).
      Org has 0 repos and 0 VCAPs.
- Step status for the `noaa-cefi` re-ingest: see the status table at the top of this file.

## Scan notes (2026-10-19)

Scan scripts and outputs were session-local scratch files and are not preserved; the method is described at the top of this file.

- [x] 3a. Whole-bucket inventory: 26,707 objects, 36.95 TB. Newest releases (audit scope):
      NEP hindcast daily/monthly r20260701 (pilot used r20250912!), NWA hindcast r20250715, NWA seasonal fcst r20250710,
      reforecast r20250413, decadal monthly r20250925, decadal yearly r20250819, multi_decadal_outlook r20260331 (new, not in pilot),
      pacific_islands r20260427 (draft). Newest-release volumes (uncompressed ~ see header scans; on-disk GB):
      NEP daily raw 5284, regrid 1336; NEP monthly raw 1183, regrid 332; NWA decadal monthly raw 1400, regrid 1418;
      NWA hindcast monthly raw 2177, regrid 1250; daily raw 132, regrid 133; yearly decadal raw 37, regrid 38.
- [x] 3b. **nwa decadal yearly r20250819** header scan (all 733 .nc, 0 parse errors, ~5 min with 32 threads):
      - raw: 366 var-init files (6 vars x 61 inits) + `ocean_static.nc` (65 MB, HDF5-parsable, 845x775, carries
        geolon/geolat/deptho/areacello/wet/sftof, cell sizes, *_c/_u/_v grids). So statics are available in the same release
        directory; the pilot copied them from another repo instead.
      - Every variable has ONE shape/chunk/dtype/fill across all 61 inits; 4-D vars chunks (10,10,200,200) raw, (4,4,422,387) regrid.
      - Uncompressed volume: raw 95.9 GB, regrid 95.6 GB (each of 6 vars ~16 GB); source NC on-disk 37 GB each grid.
      - `xh`/`yh` stored with two chunkings (200 and full) between files and fill 9.97e36 vs NaN: harmless (coordinates are loaded natively).

- [x] 3c. **nep hindcast monthly r20260701** header scan (h5py over obstore; 900 of 900 .nc, 2 errors = the netCDF3 statics
      `ice_static.nc`, `ocean_static.nc`, as P4 predicts). NB: pilot used r20250912; r20260701 is newer.
      Method note: VirtualiZarr manifest parsing was ~80 min for these files, and h5py threads serialize on its global lock;
      a ProcessPool (6) of metadata-only h5py reads with a 256 KB obstore buffer took ~9 min.
      - Layout: ONE file per variable covering the whole period (396 monthly steps, 1993-01..2025-12, `days since 1993-01-01`).
        So there is no time concat: ingest = merge variables into groups. Time chunk 100 (396 = 3x100 + 96) is fine for a single file.
      - raw: 461 var files; regrid: 437 var files (all 437 regrid vars also in raw; raw has 24 extra e.g. uo, vo, umo, vmo, FA_X...).
      - Volume (uncompressed / on disk): raw 3,222 GB / 1,183 GB; regrid 1,642 GB / 332 GB.
        Raw excluding the 62 bundled `volcello` copies: 1,797 GB uncompressed.
      - Structural signatures -> groups needed (dims, size, #vars, uncompressed GB):
        raw: (jh816,ih342) 372 vars 164 GB | (z_l52,jh816,ih342) 63 vars 1,448 GB | (zi76,jh,ih) 1 | (ct5,jh,ih) 1 |
             u-stagger (jh816,iq343) 6 + (z_l,jh,iq) 3 | v-stagger (jq817,ih342) 6 + (z_l,jq,ih) 3 | xB343 3 | yB817 3.
        regrid: (lat815,lon341) 372 vars 164 GB | (z_l52,lat,lon) 63 vars 1,442 GB | (zi76) 1 | (ct5) 1.
      - Chunking (P1, inherited): 3-D raw [38,255,107] (netCDF-C default, 390 files); 3-D regrid [100,200,200];
        4-D [100,10,200,200] = 160 MB uncompressed (70 raw / 64 regrid files); odd ones [100,5,200,200], [99,1,204,86]. All gzip.
      - Fill: raw 9.96921e36, regrid NaN (consistent within each grid).
      - P3: exactly one standalone `volcello` file per grid; 62 raw files ALSO bundle a full 4-D `volcello` (1,425 GB uncompressed)
        -> drop the bundled copies (ingest only the standalone). No `vollcello` typo in NEP monthly (only NEP daily).
      - Every raw file also bundles geolon/geolat/geolon_c/geolat_c + time_bnds (small; take once, natively).
      - Dim naming: NEP raw uses CMIP-style ih/jh/iq/jq (+ xB/yB/ct/zi); statics use xh/yh/xq/yq (rename needed, as in the pilot).
- [x] 4 (nep hindcast monthly plan confirmed, see plan section). [ ] 5 checks pending.
- [x] 3e. **nwa seasonal forecast r20250710 + reforecast r20250413** header scan (1,106 files, 2 errors = netCDF3 `ocean_static.nc` x2, ~4 min).
      - Seasonal forecast: 4 inits (202510, 202601, 202604, 202607) x 18 vars, per grid. Dims (lead:12, member, yh/lat, xh/lon).
        Uncompressed raw 44.1 GB, regrid 44.0 GB (on disk 16.5 / 18.9 GB). Fill raw 9.97e36, regrid NaN.
        **Init 202510 has 9 members (member 6 missing) AND different chunks** from the other 3 inits:
        raw (12,9,200,200) vs (12,10,200,200); regrid (4,3,282,258) vs (12,10,200,200). VirtualiZarr concat along init needs identical
        chunk shapes on the other dims, so 202510 cannot join the FMRC array as published (needs rewrite, or its own group/exclusion).
        Each file bundles `<var>_anom` (same shape as the variable) and, in raw only, a per-file `valid_time` ("days since <init>").
        `lead` has units "months".
      - Seasonal reforecast: 120 inits x 4 vars (sos, sob, tos, tob), per grid. Dims (member:10, lead:12, yh/lat, xh/lon), i.e.
        member-first, OPPOSITE of the forecast (lead, member). Chunks raw (10,12,200,200), regrid (4,4,282,258), identical across inits.
        Uncompressed raw 301.8 GB, regrid 301.0 GB (on disk 105.9 / 127.1 GB). `lead` has no units. Each file also bundles `<var>_anom`.
        Raw fill: 9.97e36 in 390 files, NaN in 90 (matches audit: sos/tob/tos x 30 inits); regrid all NaN.
      - OPEN RISKS for step 5/6: (a) can a virtual array be transposed to (member, lead, ...)? ManifestArray transpose support in
        VirtualiZarr / zarr v3 transpose codec must be tested on a prototype; if not, the forecast and reforecast stay in their
        published order and we document it. (b) The fill-value override is only safe if the 30 NaN-fill files really hold NaN (not 9.97e36) on land;
        must be checked by reading data from one NaN-fill and one 9.97e36-fill file.

## Plan: nep hindcast monthly (r20260701) -> `noaa-cefi/cefi-nep-hindcast-monthly` (step 4, CONFIRMED by user: group by horizontal grid, descriptive group names)

Group by HORIZONTAL grid (not by full dim signature as the pilot did), so one group can hold surface (2-D), depth-resolved (z_l/zi) and
`ct` variables together; a Dataset may carry several vertical dims. Fewer groups than the pilot (raw 10 -> 5, regrid 4 -> 1).

```
regrid/main   time: 396, z_l: 52, zi: 76, ct: 5, lat: 815, lon: 341        437 data vars (372 2-D, 63 (z_l), 1 (zi), 1 (ct)), virtual
raw/main      time: 396, z_l: 52, zi: 76, ct: 5, jh: 816, ih: 342          437 data vars virtual (incl. standalone volcello) 
              + native: geolon, geolat (jh, ih), deptho, areacello, wet (+ _c/_u/_v grids if wanted)
raw/u_grid    time: 396, z_l: 52, jh: 816, iq: 343                         9 vars (6 2-D + 3 4-D)
raw/v_grid    time: 396, z_l: 52, jq: 817, ih: 342                         9 vars
raw/b_x       time: 396, jh: 816, xB: 343                                  3 vars   (hybrid stagger as published)
raw/b_y       time: 396, yB: 817, ih: 342                                  3 vars
```
- time: CF-decoded datetime64 (days since 1993-01-01), 396 monthly steps 1993-01 .. 2025-12, loaded as a native coordinate.
- 4-D variables keep source chunks (100,10,200,200); 3-D raw (38,255,107), regrid (100,200,200). No rechunking (virtual).
- Bundled `volcello` copies in 62 files are dropped; only the standalone `volcello.*.nc` is used.
- Statics: geolon/geolat come from the data files (they are bundled in every one); deptho/areacello/wet come from the netCDF3
  `ocean_static.nc` (read with a netCDF3 reader, rename xh/yh -> ih/jh) and are written as NATIVE chunks (small).
- Dim names for u/v/B groups as published (iq, jq, xB, yB); to be checked against CF/GeoZarr in step 5.
- Step-5 checks still TO DO: z_l / zi / ct / time coordinate values identical across all files; ih/jh/lat/lon identical; the 24 raw-only
  variables' stagger dims match their group; statics shape == (816, 342).
- Expected volume if later ingested natively: raw 1,797 GB, regrid 1,642 GB uncompressed (see 3c).

## De-risking results (2026-10-20)

Session-local test scripts (not preserved): a fill-value slab comparison, a stock-transpose attempt (fails), and the transpose-codec workaround (passes).

- **Fill values (P8, reforecast raw): SAFE to unify.** Read member 0 / lead 0 of tos, sos, tob from one NaN-fill file and one 9.97e36-fill
  file each: identical counts in all 6 (263,153 NaN, 391,722 finite, 0 values >= 1e30). Land is NaN in the data either way; 9.97e36 is
  only the declared `_FillValue`. -> override fill_value to NaN in the store metadata. Caveat: one member/lead per file sampled;
  step 7 must compare full arrays on a sample.
- **Dimension order (P8): NOT supported natively, but a workaround verified.**
  VirtualiZarr `ManifestArray` has no `.transpose` (`permute_dims` is an open TODO at `manifests/array_api.py:357`; our pinned commit
  de4e669 == upstream HEAD on 2026-10-20). Workaround: build a new `ManifestArray` with permuted shape/chunks, permuted chunk-key order,
  and a Zarr v3 `transpose` codec (order = inverse permutation) prepended to the existing codecs [transpose, bytes, shuffle, zlib].
  Verified on seasonal_forecast regrid `tos` i202601: reading back (member, lead, lat, lon) through Icechunk matched the source
  h5py slab (lead 1:4, member 2:7, 40x40 window; 24,000 finite values, 5 distinct member slices).
  Open caveats: (1) tested on one chunk of one file, not yet through an FMRC concat or across chunk boundaries (do in step 6 prototype);
  (2) readers must support the Zarr v3 `transpose` codec (zarr-python does; check other clients before promising it to users);
  (3) it is custom code on top of VirtualiZarr internals, so a candidate to raise upstream as a feature request (ManifestArray.permute_dims).

### Rule: one codec chain per variable (user, 2026-10-20)
Every file concatenated into one variable (here: every init) must have an IDENTICAL array metadata incl. codec chain
(+ dtype, chunks, fill_value). So the transpose workaround is applied by ONE helper to EVERY forecast file's arrays, same `order`,
never to a subset. Reforecast keeps its published (member, lead) order -> no transpose codec; chain [bytes, shuffle, zlib].
Step-5 check to add: for each variable, parse all files with VirtualiZarr (or h5py: compression_opts level, shuffle flag, itemsize)
and assert `metadata.codecs`, dtype, chunks and (post-override) fill_value are equal across all inits BEFORE writing. Not yet verified:
the h5py scan only recorded compression="gzip", not zlib level / shuffle, so per-file codec equality is still unchecked.

### Codec-chain check: nwa seasonal forecast r20250710 + reforecast r20250413 (2026-10-20, `codec_check.py` on `hdrc_nwa_seasonal.parquet`)
1,106 files, 2 errors (netCDF3 ocean_static.nc x2). Compared dtype, compression, zlib level, shuffle, fletcher32, scaleoffset, fill, chunks
for every variable across all files within (experiment, grid):
- Compression is gzip(zlib) level 2 + shuffle, no fletcher32/scaleoffset, for ALL data variables in ALL files. dtype consistent.
- **Only codec-level mismatch on data vars = `_FillValue`** (P8): reforecast raw sos/tob/tos + their `_anom`: 9.97e36 in 90 files vs NaN in 30
  per variable. Fixed by the planned NaN override (verified safe, see de-risking).
- Small coordinates only (loaded natively, harmless): reforecast raw `xh`/`yh` same 9.97e36-vs-NaN fill split (390/90 files);
  reforecast regrid `init` is int32 in 90 files, int64 in 390.
- **Only chunk-level mismatch = forecast init 202510** (member chunk 9; regrid chunks (4,3,282,258)) on all 36 variables (18 vars + 18 `_anom`) x 2 grids.
  Reforecast chunks are identical across all 120 inits.
- => Seasonal forecast (inits 202601/04/07) and reforecast can use one codec chain per variable once fill is overridden;
  202510 remains the open decision (exclude / own group / needs upstream regeneration).
- Still to check (scans running in order): nwa decadal yearly (`hdrc_nwa_decadal_yearly.parquet`), nep hindcast monthly (`hdrc_nep_hindcast_monthly.parquet`).

### Codec-chain check: nwa decadal yearly r20250819 (`hdrc_nwa_decadal_yearly.parquet`)
733 files, 0 errors. 52 (grid, var) groups: ALL data variables have identical dtype/gzip level 2/shuffle/fill/chunks across all 61 inits
(raw (10,10,200,200), regrid (4,4,422,387)). No chunk-only mismatches. Only difference: `xh`/`yh` in the single `ocean_static.nc`
are uncompressed vs gzip in the 366 data files (coordinates, loaded natively -> harmless). FMRC concat over init is safe as-is.

### Codec-chain check: nep hindcast monthly r20260701 (`hdrc_nep_hindcast_monthly.parquet`, `nep_groups.py`)
900 files, 2 errors (netCDF3 `ice_static.nc`, `ocean_static.nc`). One file per variable, so no cross-file concat of a variable; checked instead
(a) cross-file consistency of the bundled coordinates and (b) codec/chunks across variables within each planned group.
- 0 codec-chain mismatches across files. Every variable incl. coordinates, all 8,936 arrays: gzip level 2 + shuffle, no fletcher32/scaleoffset.
- Per planned group, ONE dtype+codec+fill: raw/* float32, fill 9.96921e36 (main 437 vars, u_grid 9, v_grid 9, b_x 3, b_y 3);
  regrid/main float32, fill NaN (437 vars). Group sizes match the confirmed plan exactly (437/9/9/3/3 and 437).
- Chunk shapes differ between variables, as expected (each is its own array, so no conflict): 3-D raw (38,255,107), 3-D regrid (100,200,200),
  4-D (100,10,200,200), plus the `ct` variable (100,5,200,200) raw / (99,1,204,86) regrid.
- Chunk-only differences exist only in bundled small coordinates (geolon/geolat/_c, ih): chunking varies with each variable's
  stagger (e.g. (200,200), (200,342), (816,200)). We take these once and write them NATIVELY, so no impact.
=> **Codec-chain verification complete for all three scanned products** (nwa seasonal fcst+reforecast, nwa decadal yearly, nep hindcast monthly).
   Only genuine blockers: seasonal forecast init 202510 (chunks), seasonal reforecast raw fill split (fixed by NaN override).
   Step-5 "one chain per variable" check is therefore satisfied on the header level; the final pre-write assertion (compare
   `ManifestArray.metadata` of every file, after the transpose/fill transforms) stays in the build script as a hard guard.

## Decision: seasonal forecast init 202510 -> native chunks via `to_zarr(region=...)` (user, 2026-10-20)
- Confirmed from source: 202510 `member = [1,2,3,4,5,7,8,9,10]` (member 6 absent), inits 202601/04/07 `member = [1..10]` (both grids).
- Plan: (1) read 202510 vars with xarray, keep `*_anom` (user 2026-10-20), drop only per-file `valid_time`/`init`, apply the SAME transforms as the virtual inits (dim order via the array's
  transpose codec, fill -> NaN), `reindex(member=[1..10])` so member 6 is NaN; (2) write into the existing array with
  `ds.to_zarr(store, group=..., region={"time": slice(k, k+1)})` so the array's own codecs/chunking (12,10,200,200) are used -> same codec chain.
- Mechanism to make room for the 202510 slot (NOT yet tested): virtual concat requires identical chunk shapes, so 202510 enters the virtual
  concat as a placeholder ManifestArray with the metadata of a normal init and an EMPTY chunk manifest; after the virtual commit, region-write
  the native data. (Append would put 202510 last -> non-monotonic `time`, so not used.) Verify: region write works on an array whose metadata
  carries the transpose codec; `time` order is 202510, 202601, 202604, 202607; unwritten/empty chunks read as NaN.
- Validation: nine real members == source exactly; member-6 slab all NaN; other three inits still read virtually (spot-check bytes vs source).
- Volume: ~4 GB on disk (~11 GB uncompressed incl. `_anom`) per grid; local run is enough.
- Upstream ask for NOAA: regenerate 202510 with all 10 members (SOURCE-FIXES.md).

## Plan: nwa seasonal forecast (r20250710) + reforecast (r20250413) -> `noaa-cefi/cefi-nwa-seasonal-{forecast,reforecast}-monthly` (step 4, CONFIRMED by user 2026-10-20: keep *_anom, integer-month step + valid_time, normalize to (member, step))

Facts used (from header scans + coordinate reads): forecast `lead` int32 0..11 units "months"; reforecast `lead` int32 0..11 no units; `member` 1..10;
per-file scalar `init` ("days since <init>", = 0); forecast raw has per-file `valid_time` (days since init: 0,31,59,90,... = month starts, i.e. init + lead months);
also a `month` variable (not yet inspected). Init dates come from the filename `.iYYYYMM`. Forecast: 202510, 202601, 202604, 202607. Reforecast: 120 inits,
quarterly (Jan/Apr/Jul/Oct) 1994-01 .. 2023-10. Seasonal raw files do NOT bundle geolon/geolat (statics come from `ocean_static.nc`, netCDF3).

```
cefi-nwa-seasonal-forecast-monthly           (18 vars; uncompressed 44 GB/grid incl. bundled anoms; on disk 16.5 / 18.9 GB raw / regrid)
  regrid/main  time: 4, member: 10, step: 12, lat: 844, lon: 774       chunks (1, 10, 12, 200, 200)
  raw/main     time: 4, member: 10, step: 12, yh: 845, xh: 775         chunks (1, 10, 12, 200, 200) + native statics (geolon, geolat, deptho, areacello, wet)
  time = forecast_reference_time (2025-10-01, 2026-01-01, 2026-04-01, 2026-07-01); inits 202601/04/07 virtual, 202510 NATIVE (decided)
cefi-nwa-seasonal-reforecast-monthly         (4 vars sos/sob/tos/tob; uncompressed 302 GB/grid incl. anoms; on disk 105.9 / 127.1 GB)
  regrid/main  time: 120, member: 10, step: 12, lat: 844, lon: 774     chunks (1, 4, 4, 282, 258)
  raw/main     time: 120, member: 10, step: 12, yh: 845, xh: 775       chunks (1, 10, 12, 200, 200) + native statics
both: valid_time (time, step) datetime64 = init + step months (native, tiny); step int months 0..11 attrs {standard_name: forecast_period, units: months};
      member 1..10; featureType forecast_model_run_collection; fill_value NaN (override); time chunk = 1 (one run per chunk)
```
- Dim order (member, step): forecast files are (lead, member) -> transposed through the Zarr `transpose` codec on EVERY forecast file's arrays (same order, verified
  on one chunk); reforecast files are already (member, lead) -> no transpose. Forecast and reforecast are separate repos, so their chains never need to match.
- `step` is an integer month count, not a timedelta64 (months are not a fixed duration), same approach as the decadal yearly FMRC (integer lead year);
  the real timestamps live in 2-D `valid_time`. Consequence: rolodex's timedelta-based indexing would need `valid_time` rather than `time + step`.
- Step-5 checks still TO DO: lat/lon (yh/xh) values identical across all inits within each product (the regrid lat/lon differ at ~1e-7 BETWEEN forecast and
  reforecast, fine since separate repos); member/lead coordinate values identical; inspect the `month` variable; verify the release's `ocean_static.nc`
  is the 845x775 NWA grid (pilot found region-swapped statics; decadal yearly's own static was correct); source `valid_time` == init + lead months for all 4 forecast inits.
- Open choices for the user: (a) keep or drop `*_anom` (real anomaly fields bundled in every file; keeping them doubles the variable count to 36 forecast / 8 reforecast,
  at no extra storage since virtual; dropping loses data); (b) `step` integer months + valid_time vs a timedelta approximation; (c) confirm the (member, step) normalization.

### Consequences of keeping `*_anom` (user, 2026-10-20)
- Forecast: 36 variables per grid (18 + 18 `_anom`); reforecast: 8 (4 + 4 `_anom`). Same codec chain/chunks as their base variables (verified: codec_check lists the `_anom` arrays with identical chains).
- The NaN fill override must also be verified for `_anom` arrays (the earlier fill check only read sos/tos/tob primaries); add to step 5: read one `sos_anom`/`tos_anom`/`tob_anom` slab from a NaN-fill and a 9.97e36-fill reforecast raw file.
- Variable names stay as published (`tos_anom` etc.); document in attrs that they are anomalies of the base variable.

## Note (user, 2026-10-20): step-5 timing
The step-5 checks (listed per product in the conversation and in the plan sections above: pandera schemas, coordinate agreement across all
files, `_anom` fill check, static-file grid check, seasonal `valid_time` check, `month` variable, decadal lead vectors, final codec-chain assertion)
are deferred: run them only when doing the real ingestion of each product, not as separate up-front work. Steps 5 for all planned products
therefore remain unchecked in this file until then.

- [x] 3f. **nep hindcast daily r20260701** header+codec scan (`hdrc_nep_hindcast_daily.parquet`, `an_daily.py`; 760 files, 2 errors = netCDF3 `ice_static.nc`, `ocean_static.nc`; ~8 min).
      Two kinds of files (P2 split), per grid:
      | | files | on disk | uncompressed | vars |
      |---|---|---|---|---|
      | raw full-period 3-D (time, jh, ih), 199301-202512 | 16 | 98.7 GB | 215 GB | 16 |
      | raw per-year 4-D (time 365/366, z_l 52, ...) 33 yr x 12 vars | 396 | 5,185 GB | 13,998 GB (8,400 GB excl. bundled volcello/vollcello copies) | 12 |
      | regrid full-period 3-D | 16 | 58.0 GB | 214 GB | 16 |
      | regrid per-year 4-D, 33 yr x 10 vars | 330 | 1,278 GB | 6,967 GB | 10 |
      - Full-period files: 12053 daily steps (1993-01-01 .. 2025-12-31, `days since 1993-01-01`), one file per variable, all 16 on (jh816, ih342) raw / (lat815, lon341) regrid
        -> a single group per grid. Chunks (P1): raw (213,109,45) = netCDF-C default; regrid (100,200,200). Time chunk 100 with a partial last chunk (53) is fine in one file.
        Fill raw 9.96921e36, regrid NaN. Time 12053 is a single-file axis, no concat.
      - Per-year 4-D files (P2): vars raw dissic no3 o2 po4 si so talk thetao uo vo volcello vollcello; regrid same minus uo, vo. Chunks (100,10,200,200) for all; shape (365|366, 52, ...).
        Raw dim signatures: z_l,jh,ih (10 vars); uo on (z_l,jh,iq); vo on (z_l,jq,ih). EXCLUDED from ingestion per the P2 decision (TODO: revisit with variable-length chunks).
      - P3: standalone `volcello` AND `vollcello` exist as separate per-year file series (33 files each per grid). Bundled copies only in RAW: `volcello` inside so/thetao files
        (66 files, 1,399 GB uncompressed), `vollcello` inside dissic/no3/o2/po4/si/talk files (198 files, 4,198 GB). Regrid has no bundled copies.
        All of it is per-year 4-D, so it is excluded with P2 today. OPEN for when P2 is solved: rename vollcello -> volcello would clash with the real volcello series; need to check
        whether the two series are identical (data) before deciding.
      - Codec check: 0 codec-chain mismatches across files per (grid, variable) (gzip level 2 + shuffle everywhere; per-year chunks all (100,10,200,200)).
      - => Ingestable now: 16 two-D variables per grid, full period. Raw group (time 12053, jh 816, ih 342) + native statics; regrid group (time 12053, lat 815, lon 341). ~215 GB uncompressed per grid if ever native.
        (Pilot/Earthmover store had 14 vars from the older release; this release has 16.)
- **!! NEP daily 4-D (P2): needs variable-length chunks to ingest VIRTUALLY, or rewriting ~6.5 TB (compressed; ~15 TB uncompressed) to ingest NATIVELY. Left out for now; see INGESTION_REQUIREMENTS.md note.**

## Plan: nep hindcast daily (r20260701), 2-D subset only -> `noaa-cefi/cefi-nep-hindcast-daily` (step 4, CONFIRMED by user 2026-10-20)

Facts: 16 full-period primary variables, identical list in raw and regrid: btm_co3_ion btm_co3_sol_arag btm_co3_sol_calc btm_htotal btm_o2 chlos mesozoo_200
no3os pco2surf phos phycos ssh sshmax sshmin tob tos. One file per variable, 12053 daily steps. Raw files also bundle: geolon/geolat (jh,ih) [full coords, chunks (200,200)],
geolon_c/geolat_c (jq,iq), time_bnds (time, nv=2), average_T1/T2/DT (time), plus 1-D index coords ih/jh/iq/jq/nv. Regrid files bundle only lat/lon/time (no time_bnds).
netCDF3 statics (read with scipy from bytes; not HDF5): `ocean_static.nc` (28 MB) dims ih342 iq343 jh816 jq817 z_l52 zi76 ..., fields geolon/geolat(+_c/_u/_v), deptho, areacello(+_bu/_cu/_cv),
wet(+_c/_u/_v), sftof, dxt/dyt/dxCu/..., Coriolis; fill 1e20; it uses the SAME dim names and shapes as the data (816x342 NEP grid), unlike the pilot's older release
(which needed xh->ih renames and had region-swapped files). `ice_static.nc` (5.7 MB): dims yT816 xT342 (+yTe,xTe,xB,yB,ct), fields CELL_AREA, COSROT, SINROT, geolon/geolat, fill -1e34.
(Not yet verified: that static geolon/geolat equal the data files' geolon/geolat, and which are masked over land - deferred to the real ingestion per user.)

```
cefi-nep-hindcast-daily
  regrid/main  time: 12053, lat: 815, lon: 341            16 vars float32, virtual, chunks (100, 200, 200), fill NaN
  raw/main     time: 12053, jh: 816, ih: 342              16 vars float32, virtual, chunks (213, 109, 45), fill 9.96921e36
               + native: geolon, geolat (jh, ih) from a data file; statics deptho, areacello, wet from ocean_static.nc
               + native (proposed): geolon_c/geolat_c (jq, iq), time_bnds (time, nv), average_T1/T2/DT (time)
  time: CF-decoded datetime64 daily, 1993-01-01 .. 2025-12-31, loaded natively (days since 1993-01-01)
```
- Excluded: all per-year 4-D variables (needs variable-length chunks virtually, or ~6.5 TB rewrite natively) - see the P2 note.
- Native data are small coordinates/statics only (time axis 12053 values, 2-D grids ~280k points each); everything else virtual.
- Access cost note (P1, inherited): raw chunks (213,109,45) are time-series friendly (one chunk = 213 days over 109x45 cells) but a single-day map touches ~64 chunks of 4 MiB each;
  regrid chunks (100,200,200) are better balanced. Only NOAA rewriting files can change this.
- Volume if later ingested natively: 215 GB (raw) / 214 GB (regrid) uncompressed; 98.7 / 58.0 GB on disk.
- Open choices: (a) keep time_bnds + average_T1/T2/DT natively in raw; (b) which statics to carry (core 5 as in the pilot vs. all of ocean_static incl. cell sizes, stagger-grid coords, sftof).

### Confirmed choices (user, 2026-10-20): nep hindcast daily
- raw/main carries NATIVE `time_bnds` (time, nv) and `average_T1/T2/DT` (time), decoded to datetimes where they are times.
- Statics in raw/main: core 5 (geolon, geolat from a data file; deptho, areacello, wet from `ocean_static.nc`) PLUS geolon_c/geolat_c (jq, iq). Not carried: sftof, cell sizes, Coriolis, _u/_v grids, ice_static.
- Step-5 checks for this product (deferred to the real ingestion): static geolon/geolat vs data-file geolon/geolat, land masking in statics, coordinate agreement across the 16 files, time axis identical across files, pandera schema.

- [x] 3g. **nwa hindcast monthly r20250715** header+codec scan (`hdrc_nwa_hindcast_monthly.parquet`, `an_nwam.py`, `lon_scan.py`, `nwa_static.py`; 954 files; 1 error = netCDF3 `ocean_static.nc`; ~9 min).
      - Layout: one file per variable, whole period, 372 monthly steps (1993-01 .. 2023-12, `days since 1993-01-01`); no time concat. raw: 488 var files + 2 statics; regrid: 464 var files.
      - Volume (uncompressed / on disk): raw 7,422 GB / 2,177 GB (4,280 GB uncompressed excl. the 62 bundled volcello copies = 3,142 GB); regrid 3,949 GB / 1,250 GB.
      - P1 chunks: 3-D (100,200,200) (raw 411 files, regrid 393); 4-D (100,10,200,200) (raw 76, regrid 70); ct var raw (100,5,200,200), regrid (75,1,169,155). All gzip 2 + shuffle.
        (NWA already uses sensible 100x200x200; no netCDF-C-default chunks seen, unlike NEP.)
      - Raw dims use MOM6-native names AND SIS2 ice names for the SAME 845x775 size: ocean (yh,xh): 357 2-D + 69 (z_l) + 1 (zi) = 427 vars; u-stagger (yh,xq) 6+3; v-stagger (yq,xh) 6+3;
        ice (yT,xT) 36 + 1 (ct,yT,xT); ice (yT,xB) 3; ice (yB,xT) 3. Regrid: (lat,lon) 393 + (z_l) 69 + (zi) 1 + (ct) 1 = 464 vars on two longitude grids (below).
        Whether yT/xT equal yh/xh numerically is UNVERIFIED (deferred step-5 check); they carry different dim names so ice vars are in separate groups unless renamed.
      - 24 raw-only variables (no regrid): FA_X FA_Y FI_X FI_Y IX_TRANS IY_TRANS S/T_adx/ady_2d tauuo tauvo udml/uml/vdml/vml_restrat uhml vhml umo umo_2d vmo vmo_2d uo vo; regrid has no variable absent from raw.
      - **P5 confirmed, sharper than the audit**: all 464 regrid files have lon n=774 and identical lat (5.2725..58.1608, n=844 from the scan), but TWO lon grids, both ending at -36.0800:
        427 ocean variables start at -98.4423, step 0.08068; 37 sea-ice variables (ALB BHEAT BMELT BSNK CN E2MELT EXT EXT_MAX EXT_MIN FRAZIL LSNK LSRC MIB RAIN SALTF SN2IC SNOWFL STRENGTH_hf T1-T4 TMELT TSN XPRT
        siconc simass sisnconc sisnmass sisnthick sispeed sitemptop sithick sitimefrac siu siv sivol) start at -98.0, step 0.0801. (My per-array "distinct arrays" counts were invalid: Python hash() differs per worker process. Exact lat equality across files is still unchecked.)
      - P3: exactly one standalone `volcello` per grid, no `vollcello`; 62 raw files bundle a full 4-D volcello (3,142 GB uncompressed; drop). Regrid has none.
      - 18 raw `*_rotate` vector variables (S/T_adx/ady_2d_rotate, tauuo/tauvo_rotate, umo/vmo(_2d)_rotate, uo/vo_rotate, uhml/vhml, u/v ml_restrat, u/v dml_restrat _rotate) have fill NaN (rest of raw: 9.96921e36) and time units spelled
        "days since 1993-01-01" (rest of raw: "... 00:00:00"). Raw data vars otherwise 9.96921e36, regrid NaN.
      - Codec check: codec chain gzip level 2 + shuffle everywhere, no fletcher32/scaleoffset. Only mismatches are small coordinates (raw time, xh, yh, z_l, xT, xTe, yT, yTe: fill NaN vs 9.97e36 between the 18 `_rotate` files and the rest) - native, harmless.
        One file per variable, so no cross-file concat of a variable.
      - Statics: raw dir has `ocean_static.nc` (66 MB, netCDF3 `CDF\x02`, dims yq846 xq776 yh845 xh775; read via scipy) and `ice_static.nc` (13 MB, netCDF4/HDF5; CELL_AREA, COSROT, SINROT, GEOLAT, GEOLON on (yT,xT) 845x775).
        ocean_static is the CORRECT NWA grid (yh/xh values identical to a data file's; geolon -98.442..-36.080, geolat 5.273..58.161), no masked geolon/geolat (0 fill cells) in this release (the pilot's region swap does not apply here).
        ice_static GEOLON/GEOLAT ARE masked with -1e34 (edges). Raw data files do NOT bundle geolon/geolat, so ocean_static is the only source of raw geolon/geolat.
      - ***NOT DOABLE / BLOCKED for this product***: (1) a single `regrid` group with one common `lon` axis for all 464 variables (P5): the 37 sea-ice variables are on a different longitude grid. Virtually they CAN be a second group
        (own `lon`), but a common grid needs NOAA to regrid (needs their weights). (2) nothing else is blocked: no per-year/variable-length-chunk problem (all whole-period files), no missing member/init.
- [x] 3h. **nwa hindcast daily r20250715** header+codec scan (`hdrc_nwa_hindcast_daily.parquet`, `an_nwad.py`, `lon_scan_daily.py`; 24 files, 1 error = netCDF3 `ocean_static.nc`; seconds).
      - 11 variables per grid, identical list in raw and regrid: btm_co3_ion btm_co3_sol_arag btm_co3_sol_calc btm_htotal btm_o2 chlos mesozoo_200 no3os pco2surf phos phycos. One file per variable, whole period.
      - Time: 11,322 daily steps (1993-01-01 .. 2023-12-31, `days since 1993-01-01`), chunk 100. All arrays 3-D, chunks (100,200,200), all gzip 2 + shuffle. No concat.
      - Volume (uncompressed / on disk): raw 326 GB / 131.7 GB; regrid 325 GB / 133.4 GB (each var ~30 GB uncompressed).
      - ONE dim signature per grid: raw (time, yh 845, xh 775) all 11; regrid (time, lat 844, lon 774) all 11. Fill raw 9.96921e36, regrid NaN. No bundled geolon/geolat in the data files (only time_bnds, average_T1/T2/DT, nv, xh/yh).
      - P5 does NOT apply: all 11 regrid files have the same lon start (-98.4423), step (0.08068), end (-36.0800) and lat range (5.2725..58.1608). (Exact array equality not verified: that run's hashes were disabled; step-5 item.)
      - P3: no volcello/vollcello in this product (no bundled copies, no standalone). P1: chunks already (100,200,200). Codec chain consistent (0 mismatches).
      - Statics: raw dir has `ocean_static.nc` (66 MB, netCDF3) and `ice_static.nc` (13 MB, netCDF4), the same byte sizes as NWA monthly's (65,657,556 / 13,139,663) - probably identical files, UNVERIFIED. `ice_stati.json`, `ocean_stati.json` (P9 misnamed JSONs) and `hlos...json`/`o3os...json` (first char dropped) exist but are irrelevant: we parse the NetCDFs.
      - NOT DOABLE/BLOCKED: nothing found. This is the cleanest product so far.
      - Not yet drafted: schema (expected: raw/main (time 11322, yh 845, xh 775) 11 vars + native statics; regrid/main (time 11322, lat 844, lon 774) 11 vars).
- [x] 3i. **nwa decadal forecast monthly r20250925** header+codec scan (`hdrc_nwa_decadal_monthly.parquet`, `an_decm.py`, `lead_facts.py`, `dec_static.py`; 2,198 files, 2 errors = netCDF3 `ocean_static.nc` + `ice_static.nc`; ~16 min).
      - 18 vars x 61 inits (1965-01 .. 2025-01, annual, all January) per grid: MLD_003 btm_co3_ion btm_co3_sol_arag btm_o2 epc100 jprod_ndi_new_100 jprod_nlgp_new_100 jprod_nmdp_new_100 jprod_nsmp_new_100 nlgz_100 nmdz_100 nsmz_100 phos sob sos tob tos wc_vert_int_npp.
        Files named <var>.nwa.full.dc_fcast.monthly.<grid>.r20250925.enss.i<YYYYMM>.nc. raw 1,098 var files + 2 statics; regrid 1,098.
      - Volume (uncompressed / on disk): raw 3,451 GB / 1,400 GB; regrid 3,443 GB / 1,418 GB. Per file: dims (member 10, lead 120, y, x), 10 members in EVERY init (no missing member like seasonal 202510).
      - Chunks (P1) identical across all 61 inits for every variable: raw (10,12,200,200); regrid (2,30,211,194). Fill raw 9.96921e36, regrid NaN. Dim order already (member, lead, ...): no transpose needed.
      - **Codec chain check (needed for the FMRC concat): 0 codec mismatches and 0 chunk mismatches across inits for every variable, both grids** (gzip 2 + shuffle).
      - P7: `lead` is float "days since <init>-01-01" (calendar gregorian) at mid-month: 15.5, 45.0|45.5, 74.5|75.5, ... up to 3636.5|3637.5; 120 steps; shape [120] in all files. The vector depends on leap years
        (sampled 1965 == 2025 but 1968, 2000, 2020 differ in several entries; 2 distinct vectors in 5 samples, count over all 61 inits not computed). `average_T1/T2` use calendar proleptic_gregorian ("days since <init>"), `average_DT` = days in month.
      - Bundled in raw files: `average_T1/T2/DT` (member, lead) and `time_bnds` (member, lead, nv); the 10 member rows are identical in every sampled file (5 inits, T1/T2). No geolon/geolat bundled. Regrid files bundle only lat/lon/lead/member.
      - No volcello, no P2 (annual inits, 120-step files, regular chunks), no P5.
      - **Statics PROBLEMS** (netCDF3 `CDF\x02`, read via scipy): `ocean_static.nc` (65,608,792 B, different from the hindcast one) is the NWA grid 845x775 but geolon/geolat are MASKED over land (228,416 of 654,875 cells; P4).
        `ice_static.nc` (5.6 MB) is the WRONG REGION: 816x342 (NEP grid), geolon 156.9..255.0E: must not be used for NWA.
      - NOT DOABLE/BLOCKED: nothing blocking the FMRC cube itself. Raw `geolon/geolat` cannot be taken from this release's ocean_static (masked); needs another source (see schema choices).
- [x] 3j. **nwa multi_decadal_outlook r20260331** header+codec scan (`hdrc_nwa_multidecadal.parquet`, `an_md.py`, `md_time.py`; 108 files, 0 errors; ~2 min).
      - Layout: one file per (variable, scenario); 4 scenarios SSP126, SSP245, SSP370, SSP585; yearly 1970-2100 (131 steps; `days since 1900-01-01`, gregorian, mid-year 1970-07-02 .. 2100-07-02; time_bnds = Jan 1 .. next Jan 1).
        Files named <var>.nwa.full.multi_decade.yearly.<grid>.r20260331.<SSP>.1970-2100.nc. raw: 19 vars x 4 = 76 files; regrid: 8 vars x 4 = 32 files. NO static files in this release directory; geolon/geolat are NOT bundled.
      - Volume (uncompressed / on disk): raw 727 GB / 287.5 GB; regrid 151 GB / 41.2 GB.
      - Variables raw (19): MLD_003 so sos speed ssh thetao tob tos [on (yh,xh): 8]; tauuo + S_adx T_adx umo uo [on xq: 5]; tauvo vmo_2d + S_ady T_ady vmo vo [on yq: 6]. Regrid (8): MLD_003 so sos speed ssh thetao tob tos.
        4-D vars (z_l 52): so thetao (+ the u/v-grid S_adx T_adx umo uo S_ady T_ady vmo vo). Raw shapes (131, [52,] 845|846, 775|776); regrid (131, [52,] 844, 774).
      - Chunks (P1), identical across scenarios for each variable: raw 4-D (22,8,141,130), 3-D (44,282,259); regrid 4-D (22,8,141,129), 3-D (44,282,258). Fill: raw 1e+20 (differs from the 9.97e36 used elsewhere), regrid NaN.
      - Codec chain: 0 mismatches across scenarios for every (grid, variable), gzip 2 + shuffle. Only chunk-only difference is raw `time_bnds`/`time` shape (130 vs 131) from the P6 file.
      - **P6 confirmed and located**: `T_adx` SSP585 RAW has 130 steps; it is missing YEAR 2045 (index 75 of 131; its time jumps 2044 -> 2046). All other 107 files (incl. T_adx SSP126/245/370 and every regrid file) share the same 131-step time axis (years checked for the odd file; exact values equal for the rest).
      - Grids: all 32 regrid files lon start -98.0, step 0.0801, end -36.08 (the "grid B" of the NWA hindcast monthly sea-ice variables, NOT its ocean grid A at -98.4423), lat 5.2725..58.1608; raw xh[0]=-98.0, yh[0]=5.2725 (same NWA grid as the hindcast).
      - ***NOT DOABLE virtually***: T_adx SSP585 in the same scenario-stacked array as the other 3 scenarios. The file is 130 steps, the cube needs 131; the missing year sits mid-array (index 75), so every later year would have to shift one slot inside the file's chunks, which a virtual reference cannot express (and concat on a new `scenario` dim requires equal shapes).
        Options: exclude T_adx; keep T_adx for 3 scenarios only; or write T_adx SSP585 as NATIVE chunks with 2045 = NaN (~17.7 GB uncompressed, 7.9 GB on disk), like the seasonal 202510 decision.
- [x] 3k. **pacific_islands hindcast daily + monthly r20260427** (DRAFT product; NOT covered by the audit) header+codec scan (`hdrc_pci.parquet`, `an_pci.py`, `pci_facts.py`; 793 nc files, 0 errors; ~9 min).
      Grid: raw (yh 539, xh 725) [u: xq 726; v: yq 539]; regrid (lat 538, lon 724). Region check: static geolon 171.0..220.5 E, geolat 8.9..38.1 N (Pacific Islands). All files are FULL-PERIOD (199301-202512), one per variable: NO per-year files, no 4-D in daily, so P2 does not apply.
      | product | files (nc) | vars | uncompressed | on disk |
      |---|---|---|---|---|
      | daily raw | 19 (18 var + ocean_static) | 18 (16 on h, ssu on xq, ssv on yq) | 339 GB | 271.5 GB |
      | daily regrid | 16 | 16 | 300 GB | 161.8 GB |
      | monthly raw | 422 (420 var files + ocean_static + 1 more non-var file) | 420 (402 h, 9 u, 9 v) | 4,734 GB (2,610 GB excl. the 66 bundled volcello copies = 2,124 GB) | 2,987 GB |
      | monthly regrid | 336 | 336 (335 2-D + `calc` 4-D) | 239 GB | 122.8 GB |
      - Time: daily 12,053 steps (1993-01-01 .. 2025-12-31); monthly 396 (1993-01 .. 2025-12); `days since 1993-01-01` (data files "... 00:00:00" or without it, same epoch). Static files carry a length-1 `time`.
      - Chunks (P1): daily raw 3-D (20,100,100); daily regrid (100,200,200); monthly raw 3-D (20,100,100) [346 files], 4-D (10,10,50,50) [74 files, ~1 MB uncompressed each: ~40k chunks per variable, ~3M chunk references in total]; monthly regrid 3-D (100,200,200) [335], `calc` 4-D (100,10,200,200).
        Compression varies BY VARIABLE TYPE (e.g. daily raw data vars gzip 2 without shuffle; regrid gzip 2 + shuffle; coordinates gzip level 0): fine, since each variable is a single file. Fill: raw 1e+20, regrid NaN.
      - Dim groups (monthly raw): (yh,xh) 402 vars = 333 2-D + 67 z_l + 1 zi (`rsdo`) + 1 empty; (yh,xq) 9 vars (S_adx_2d T_adx_2d tauuo udml_restrat uhml uml_restrat umo umo_2d uo; T_adx_2d empty); (yq,xh) 9 vars (S_ady_2d T_ady_2d tauvo vdml_restrat vhml vml_restrat vmo vmo_2d vo). `speed` exists only in regrid; 85 raw-only variables.
      - P3: 1 standalone `volcello` (monthly raw) + 66 raw files bundling a full 4-D volcello (2,124 GB): leave out per the user's decision; no vollcello typo; none in daily.
      - P5 does NOT apply: all 352 regrid files (16 daily, 336 monthly) share one grid: lon 171.0012 .. 220.539 step 0.06852 (n=724), lat 8.9384 .. 38.0883 (n=538). (Start/step/end per file; exact array equality not checked.)
      - Statics: `ocean_static.nc` in daily/raw and monthly/raw (both 11,424,376 B, probably identical, unverified) is netCDF4/HDF5 (NOT netCDF3 as elsewhere), 539x725, UNMASKED geolon/geolat (0 fill cells; land mask only in deptho/areacello/wet/sftof: 8,965 / 8,486 cells), `mask` int8; includes geolon_c/geolat_c, _u/_v, areacello (+_bu/_cu/_cv), dxt/dyt..., Coriolis. No ice_static. Data files carry NO geolon/geolat.
      - ***NOT DOABLE/BLOCKED***: 2 monthly raw variables are EMPTY (zero time steps, shape (0, ...)): `T_adx_2d` (yh,xq) and `ffedet_btm` (yh,xh): there is nothing to reference and they cannot join a 396-step group; they are left out and flagged for NOAA. Nothing else is blocked.
- [x] All products scanned (Pacific Islands was the last).

## Plan: nwa hindcast monthly (r20250715) -> `noaa-cefi/cefi-nwa-hindcast-monthly` (step 4, CONFIRMED by user 2026-10-22)

Group by horizontal grid (same principle as nep hindcast monthly); ocean and sea-ice variables are separate groups because they use different dim names (raw) / different lon grids (regrid).
All variables are whole-period single files (372 steps), so every array is a direct virtual reference; no time concat.
```
raw/main      time: 372, z_l: 52, zi: 76, yh: 845, xh: 775        427 vars (357 2-D + 69 z_l + 1 zi; incl. standalone volcello)        ~3,918 GB uncompressed
raw/u_grid    time: 372, z_l: 52, yh: 845, xq: 776                 9 vars (6 2-D + 3 z_l)                                               ~158 GB
raw/v_grid    time: 372, z_l: 52, yq: 846, xh: 775                 9 vars (6 + 3)                                                       ~158 GB
raw/ice       time: 372, ct: 5, yT: 845, xT: 775                   37 vars (36 2-D + 1 ct)                                              ~40 GB
raw/ice_xB    time: 372, yT: 845, xB: 776                          3 vars                                                               ~3 GB
raw/ice_yB    time: 372, yB: 846, xT: 775                          3 vars                                                               ~3 GB
regrid/main   time: 372, z_l: 52, zi: 76, lat: 844, lon: 774 (lon A: -98.4423, step 0.0807)   427 vars (357 + 69 + 1)                   ~3,909 GB
regrid/ice    time: 372, ct: 5, lat: 844, lon: 774 (lon B: -98.0,    step 0.0801)   37 vars (36 + 1 ct)                                    ~40 GB   [P5, see options]
```
(Group sizes sum to 488 raw / 464 regrid variables. Ocean/ice regrid GB split is approximate: 36 two-D ice vars x ~0.97 GB + ct var 4.9 GB.)
- time: CF-decoded datetime64 monthly 1993-01 .. 2023-12, native (the 18 `_rotate` files spell the epoch "days since 1993-01-01" vs "... 00:00:00": same epoch, decode identically; verify equal in step 5).
- Chunks inherited, no rechunking: 3-D (100,200,200); 4-D (100,10,200,200); ct raw (100,5,200,200), regrid (75,1,169,155).
- Dropped: the 62 bundled 4-D `volcello` copies in raw files (3,142 GB); only the standalone `volcello` is used.
- Native (small): time axis, raw `time_bnds` + `average_T1/T2/DT`, coordinates; statics from `ocean_static.nc` (netCDF3, read with scipy): geolon, geolat, deptho, areacello, wet (+ geolon_c/geolat_c: CONFIRMED present in ocean_static.nc)
  into raw/main, as chosen for nep daily (core 5 + stagger coords). Raw data files carry no geolon/geolat, so these come ONLY from the static file.
  `ice_static.nc` (netCDF4): CELL_AREA for raw/ice; its GEOLON/GEOLAT are masked (-1e34), see choices.
- Fill: raw 9.96921e36, except the 18 `_rotate` arrays (NaN); regrid NaN. No concat so no unification is needed; each array keeps its own fill.
- 24 raw-only variables (FA_X ... vo, uo, etc.) live in raw/main, u_grid, v_grid; regrid has none of them.
- Volume if later ingested natively: raw 4,280 GB (excl. bundled volcello), regrid 3,949 GB uncompressed; on disk 2,177 / 1,250 GB.
- Step-5 checks (deferred to real ingestion): yT/xT vs yh/xh numerically equal?; static geolon/geolat vs lat/lon-grid consistency; exact lat equality across all 464 regrid files; lon arrays within each of the two lon groups identical; time axis identical across files; pandera schemas; group membership by dims.

### Confirmed choices (user, 2026-10-22): nwa hindcast monthly
- P5: keep all 464 regrid variables; the 37 sea-ice variables go in a SEPARATE `regrid/ice` group with their own lon axis (group attrs document that lon differs from regrid/main). This supersedes the earlier unconfirmed proposal to skip NWA monthly regrid (INGESTION_REQUIREMENTS.md relabelled that line).
- raw/ice geolon/geolat: take them from `ocean_static.nc` ONLY IF step 5 confirms yT/xT == yh/xh numerically; otherwise fall back to carrying no ice lon/lat (not the masked ice_static GEOLON/GEOLAT) and re-ask the user. `CELL_AREA` from ice_static is carried regardless.

### Clarification (2026-10-22): what "drop volcello" means
Only the BUNDLED `volcello` arrays inside other variables' files are left out (NEP monthly raw 62 files / 1,425 GB; NWA monthly raw 62 files / 3,142 GB; NEP daily raw, per-year, excluded anyway).
The STANDALONE `volcello` file (one per grid) is ingested. Dropping the bundled copies was Claude's proposal, not a user decision, and it is UNVERIFIED that they equal the standalone data
(same shape only). TODO at real ingestion: compare bundled vs standalone values on a sample (several time steps/levels) before leaving them out; if they differ, stop and ask the user.

### Decision recorded (user, 2026-10-22): bundled volcello copies are LEFT OUT
Confirmed unconditionally. The standalone `volcello` is ingested. The earlier TODO to compare bundled vs standalone values is no longer a gate; it can still be run as an optional sanity check at ingestion and reported, but does not change the decision.

## Plan: nwa hindcast daily (r20250715) -> `noaa-cefi/cefi-nwa-hindcast-daily` (step 4, CONFIRMED by user 2026-10-22)

```
cefi-nwa-hindcast-daily
  raw/main     time: 11322, yh: 845, xh: 775      11 vars float32, virtual, chunks (100, 200, 200), fill 9.96921e36
               + native: time_bnds (time, nv), average_T1/T2/DT (time); statics geolon, geolat, deptho, areacello, wet + geolon_c, geolat_c from ocean_static.nc
  regrid/main  time: 11322, lat: 844, lon: 774    11 vars float32, virtual, chunks (100, 200, 200), fill NaN
```
Variables (same in both grids): btm_co3_ion btm_co3_sol_arag btm_co3_sol_calc btm_htotal btm_o2 chlos mesozoo_200 no3os pco2surf phos phycos.
- time: CF-decoded datetime64 daily, 1993-01-01 .. 2023-12-31 (11,322 steps), native; one file per variable so no concat, every data array is a single virtual reference.
- Nothing excluded or dropped: no P2, P3 or P5 issues in this product. ice_static is not needed (no sea-ice variables).
- Statics (same set as nep daily / nwa monthly): `ocean_static.nc` is netCDF3 (scipy read) -> native. The static file lists geolon/geolat (+_c/_u/_v), deptho, areacello (+_bu/_cu/_cv), wet (+_c/_u/_v), sftof, dxt/dyt/dxCu/..., Coriolis.
  Data files carry no geolon/geolat, so these statics are the only source of raw coordinates.
- Volume if later ingested natively: 326 GB raw / 325 GB regrid uncompressed; on disk 131.7 / 133.4 GB.
- Step-5 checks (deferred to real ingestion): exact lon/lat equality across the 11 regrid files; time axis identical across files; static geolon/geolat vs the grid; is this ocean_static byte-identical to NWA monthly's; pandera schema.

### Confirmed (user, 2026-10-22): nwa hindcast daily schema as drafted above (raw/main + regrid/main, 11 vars each, native time extras + core statics + geolon_c/geolat_c).

## Plan: nwa decadal forecast monthly (r20250925) -> `noaa-cefi/cefi-nwa-decadal-forecast-monthly` (step 4, CONFIRMED by user 2026-10-22 with the recommended choices below)
FMRC, same pattern as the decadal yearly rebuild (`cefi/scripts/build_decadal_fmrc.py`) and the confirmed seasonal plans.
```
raw/main     time: 61, member: 10, step: 120, yh: 845, xh: 775     18 vars float32, virtual, chunks (1, 10, 12, 200, 200), fill 9.96921e36
             + native: valid_time (time, step), average_T1/T2/DT and time_bnds (decoded datetimes), statics (see choices)
regrid/main  time: 61, member: 10, step: 120, lat: 844, lon: 774      18 vars float32, virtual, chunks (1, 2, 30, 211, 194), fill NaN
             + native: valid_time (time, step)
```
- time = forecast_reference_time: 1965-01-01 .. 2025-01-01 (61 annual inits, from the filename `.iYYYYMM`), time chunk 1 (one run per chunk).
- step = integer months 0..119 (units "months", standard_name forecast_period), consistent with the confirmed seasonal and decadal-yearly choices; the real timestamps are the 2-D `valid_time` (time, step) =
  decoded source `lead` (mid-month datetimes) because the day offsets vary with the leap-year cycle and cannot form one shared lead coordinate.
- member 1..10 (identical in all inits: to verify in step 5). Dim order already (member, step): no transpose codec needed. One codec chain per variable verified at header level above.
- Nothing excluded or dropped. Per-file group attrs (title, cefi_init_date, ...) replaced by collection-level attrs (featureType forecast_model_run_collection), as in the yearly build.
- Volume if later ingested natively: 3,451 GB raw / 3,443 GB regrid uncompressed; on disk 1,400 / 1,418 GB.
- Step-5 checks (deferred): lead->valid_time decode for ALL 61 inits (count distinct lead vectors; lead == months-since-init mid-month); average_T1/T2/DT/time_bnds identical across members; member values 1..10 in all inits; lat/lon/yh/xh identical across all files; pandera schema; static source checks below.
- Open choices: (a) the 10 identical member rows of average_T1/T2/DT/time_bnds: keep published shape (time, member, step) or store once as (time, step); (b) raw geolon/geolat source: this release's ocean_static is masked, its ice_static is the wrong region.

### Confirmed choices (user, 2026-10-22): nwa decadal forecast monthly (the recommended options)
- `average_T1/T2/DT` and `time_bnds`: store ONCE per (time, step) instead of per member, but only after verifying at ingestion that the 10 member rows are identical in every file; if any file differs, stop and ask the user.
- Raw `geolon`/`geolat` (and the other statics: deptho, areacello, wet, geolon_c/geolat_c): take them from the NWA hindcast monthly r20250715 `ocean_static.nc` (same 845x775 grid, unmasked), verified at ingestion to equal this release's `ocean_static.nc` wherever the latter is unmasked, and to match the data files' yh/xh. If the check fails, stop and ask. This release's masked `ocean_static` and its wrong-region `ice_static` (NEP grid) are NOT used.

## Plan: nwa multi_decadal_outlook (r20260331) -> `noaa-cefi/cefi-nwa-multi-decadal-outlook-yearly` (step 4, CONFIRMED by user 2026-10-22; see the T_adx SSP585 decision below)
Scenario-stacked cube (new dimension `scenario`; NOT a forecast-run collection: SSPs are climate scenarios with a shared time axis, so no FMRC).
```
raw/main     scenario: 4, time: 131, z_l: 52, yh: 845, xh: 775    8 vars  (6 surface/bottom 2-D + so, thetao)       chunks (1,44,282,259) / (1,22,8,141,130), fill 1e20
raw/u_grid   scenario: 4, time: 131, z_l: 52, yh: 845, xq: 776    5 vars  (tauuo 2-D; S_adx, T_adx, umo, uo)
raw/v_grid   scenario: 4, time: 131, z_l: 52, yq: 846, xh: 775    6 vars  (tauvo, vmo_2d 2-D; S_ady, T_ady, vmo, vo)
regrid/main  scenario: 4, time: 131, z_l: 52, lat: 844, lon: 774  8 vars                                           chunks (1,44,282,258) / (1,22,8,141,129), fill NaN
```
- scenario = ["SSP126","SSP245","SSP370","SSP585"] (native string coordinate); time = datetime64 yearly mid-year 1970-07-02 .. 2100-07-02 (131 steps) plus native `time_bnds` (131, 2) decoded.
- Scenario chunk size 1 (one file per scenario); all other chunk shapes inherited unchanged, no rechunking; one codec chain per variable verified across scenarios.
- Raw geolon/geolat + statics (deptho, areacello, wet, geolon_c/geolat_c): none in this release -> from the NWA hindcast monthly ocean_static.nc, as decided for decadal monthly, verified at ingestion against the data files' xh/yh.
- T_adx SSP585 (P6): pending the user's choice (options in 3j).
- Volume if ever native: 727 GB raw / 151 GB regrid uncompressed; on disk 287.5 / 41.2 GB.
- Step-5 checks (deferred): all 107 normal files' time axes exactly equal; xh/yh/lat/lon identical across all files; z_l identical; time_bnds consistent; pandera schemas; hindcast-static vs data-file coordinate equality.

### Decision (user, 2026-10-22): multi-decadal T_adx SSP585 (P6)
Write `T_adx` SSP585 (raw) as NATIVE chunks with the missing year 2045 inserted as NaN, in the SAME scenario-stacked array as the other three scenarios (which stay virtual). Same mechanism as the seasonal init 202510 decision: placeholder slot in the virtual concat (matching metadata, empty chunk manifest), then region-write the native data (to_zarr region on the scenario slot; the 130 real years are placed at their correct time positions, index 75 = 2045 stays NaN).
Volume: ~7.9 GB on disk / ~17.7 GB uncompressed; a local job. Source file untouched; noted in repo metadata and SOURCE-FIXES.md. Validation: the 130 real years equal the source exactly (read by matching time values, not by position), 2045 all NaN, other three scenarios still virtual.
(The rest of the multi-decadal schema was confirmed by the user on 2026-10-22.)

### Confirmed (user, 2026-10-22): nwa multi_decadal_outlook schema as drafted (scenario-stacked; raw/main, raw/u_grid, raw/v_grid, regrid/main; T_adx SSP585 native with 2045 = NaN; raw coords from the NWA hindcast monthly ocean_static).

## Plans: pacific_islands hindcast daily and monthly (step 4, CONFIRMED by user 2026-10-22; one repo per frequency)
```
noaa-cefi/cefi-pci-hindcast-daily
  raw/main      time: 12053, yh: 539, xh: 725     16 vars float32, virtual, chunks (20,100,100), fill 1e20
  raw/u_grid    time: 12053, yh: 539, xq: 726     1 var (ssu)
  raw/v_grid    time: 12053, yq: 539, xh: 725     1 var (ssv)
  regrid/main   time: 12053, lat: 538, lon: 724   16 vars, chunks (100,200,200), fill NaN
  vars (h): btm_co3_ion btm_co3_sol_arag btm_co3_sol_calc btm_htotal btm_o2 chlos mesozoo_200 no3os pco2surf phos phycos ssh sshmax sshmin tob tos
noaa-cefi/cefi-pci-hindcast-monthly
  raw/main      time: 396, z_l: 52, zi: 76, yh: 539, xh: 725   401 vars (333 2-D + 67 z_l + rsdo(zi); incl. standalone volcello)   chunks 2-D (20,100,100), 4-D (10,10,50,50)
  raw/u_grid    time: 396, z_l: 52, yh: 539, xq: 726           8 vars (T_adx_2d excluded: empty)
  raw/v_grid    time: 396, z_l: 52, yq: 539, xh: 725           9 vars
  regrid/main   time: 396, z_l: 52, lat: 538, lon: 724         336 vars (335 2-D + calc)                                         chunks (100,200,200), calc (100,10,200,200)
```
- Excluded: monthly raw `ffedet_btm` and `T_adx_2d` (empty files); the 66 bundled volcello copies (user decision; standalone volcello ingested). Nothing else dropped.
- time: CF-decoded datetime64 (daily / monthly), native; raw also carries native `time_bnds` and `average_T1/T2/DT` (decoded), as chosen for NEP daily / NWA decadal products.
- Statics for raw/main: core 5 (geolon, geolat, deptho, areacello, wet) + geolon_c/geolat_c from the product's own `ocean_static.nc` (netCDF4, unmasked coordinates, same grid as the data), as in the other products; read natively, tiny. Same file serves daily and monthly (identical? to verify).
- Volume if ever native: daily 339 GB raw / 300 GB regrid; monthly 2,610 GB raw (excl. bundled volcello) / 239 GB regrid (uncompressed); on disk 271.5 / 161.8 / 2,987 / 122.8 GB (monthly raw on-disk includes the bundled copies).
- Access caveat (P1, inherited): monthly raw 4-D chunks (10,10,50,50) are tiny (~1 MB), so large reads issue many small requests, and the store carries ~3M virtual chunk references; daily raw (20,100,100) is time-series oriented. Only NOAA rewriting the files can change this.
- Step-5 checks (deferred): time axes identical across files; xh/yh/lat/lon identical; the daily and monthly ocean_static identical; static geolon/geolat vs data grid; pandera schemas; the empty-file exclusion list confirmed again at ingestion time.

### User note (2026-10-22): pacific_islands P1 caveat accepted
The small monthly raw 4-D chunks (10,10,50,50) (~1 MB each, ~3 million virtual chunk references) are acceptable to the user; no action needed. (The pacific_islands schemas were then confirmed on 2026-10-22.)

### Confirmed (user, 2026-10-22): pacific_islands daily + monthly schemas as drafted (excluded: monthly raw T_adx_2d and ffedet_btm [empty], the 66 bundled volcello copies). ALL products now have confirmed schemas.


---

# Ingestion log (real runs; started 2026-10-22)

Scripts live in `ingest/` (durable). Unexpected findings are recorded here as they happen.

## 1. nwa decadal forecast yearly (r20250819) -> `noaa-cefi/cefi-nwa-decadal-forecast-yearly`
- Script: `ingest/ingest_nwa_decadal_yearly.py` (`--dry-run` = in-memory repo + validation; `--write` = org setup, create repo, commit to main, validate). Parses the NetCDF4 files directly with VirtualiZarr `HDFParser`.
- Scale: 732 small files (~50 MB each), ~0.55 s/file with 8 threads => ~3.3 min per grid locally. NO Coiled cluster needed for this product.
- org: `get_orgs()` for this login does not list `noaa-cefi`, but `get_bucket_config`/`list_repos`/VCAP calls work. No VCAP existed yet (created by the script, whole source bucket, public).
- **UNEXPECTED (statics):** this release's `raw/ocean_static.nc` is HDF5-parsable but masks land with 1e20 in geolon/geolat/geolon_c/geolat_c/areacello/wet (235,107 cells in geolon; wet is 1e20 rather than 0 on land). The 4 coordinate fields equal the NWA hindcast monthly r20250715 `ocean_static.nc` wherever this release is unmasked, and the hindcast ones are gap-free.
  Also `deptho`/`wet` differ from the hindcast static at exactly 288 cells (this release has ocean where the hindcast has land), so the hindcast mask is NOT valid for the decadal model.
  **Decision (user, 2026-10-22): hybrid.** geolon/geolat/geolon_c/geolat_c from the hindcast static (script asserts equality where valid); deptho/areacello/wet from this release's own file (land left as the source 1e20 fill). Applies to the decadal MONTHLY product too (supersedes "all statics from the hindcast" in its plan): its static must be compared the same way at ingestion.
  Recorded in `SOURCE-FIXES.md`.
- Dry run (in-memory repo) PASSED 2026-10-22: raw (61,10,10,845,775) + regrid (61,10,10,844,774); 366/366 (init, var) checks per grid vs the source files read independently with h5py (full 100-row slabs for inits 196501/196801/202501, single-chunk points for the rest); valid_time == decoded source `lead`. Wall time 23 min (open ~3.5 min/grid; validation dominates). Validator bug found on the way: `open_zarr` masked the 1e20 fill to NaN while h5py returns raw values -> compare with `mask_and_scale=False`.
- Source hiccup (expected, as in the pilot): NODD drops reads occasionally (`StorageError`); the validator retries.
- **UNEXPECTED (write attempt 1, 2026-10-22): repo creation failed; nothing was written.** The org's only bucket config (`noaa-oar-cefi-regional-mom6-pds`) has prefix `arraylake/`, and Arraylake requires every `authorize_virtual_chunk_access` / VCAP URL to sit inside that prefix. The source NetCDFs are at the bucket root (`s3://noaa-oar-cefi-regional-mom6-pds/northwest_atlantic/...`), outside it, so no VCAP can be created against this bucket config for them.
  Side effect: the setup step had already created an org VCAP for `s3://noaa-oar-cefi-regional-mom6-pds/arraylake/` with `public=True` (the wrong target: that is the repo-storage prefix, not the source data). Nothing uses it. Deleting it was blocked by the permission classifier, so it is still there pending the user's decision. The script's `ensure_org_setup` must be fixed before rerunning (it currently assumes a whole-bucket VCAP).
  Likely fix: a second bucket config for the same bucket with NO prefix (anonymous auth, read-only use for virtual chunks), plus a VCAP on it; repos keep using the `arraylake/` config for their own storage. This applies to every product, so it should be decided once.
- **Resolved (user, 2026-10-22):** deleted the wrong `arraylake/`-prefix VCAP; created bucket config `noaa-oar-cefi-regional-mom6-pds-source` (same bucket, no prefix, anonymous) and a public VCAP on `s3://noaa-oar-cefi-regional-mom6-pds/` against it. Repos keep `noaa-oar-cefi-regional-mom6-pds` (prefix `arraylake/`) for their own storage and pass `authorize_virtual_chunk_access={"s3://noaa-oar-cefi-regional-mom6-pds": "noaa-oar-cefi-regional-mom6-pds-source"}`. Use this for ALL products. `ensure_org_setup` in the script fixed accordingly (idempotent).
- **DONE 2026-10-22: `noaa-cefi/cefi-nwa-decadal-forecast-yearly` written and validated.** Commit `AT9BQE6YFNWDTXA1V89G` on `main` (raw + regrid FMRC cubes, hybrid statics, repo tags set). Validation of the committed Arraylake repo read back through the VCAP: 366/366 (init, var) checks per grid vs source file bytes, valid_time == source lead. Write run 18.5 min locally (no Coiled). 

### Consumer-style sanity check of the written repo (2026-10-23)
Fresh process, `Client().get_repo(...)`, `xr.open_zarr(readonly_session(branch="main").store, group=..., consolidated=False, zarr_format=3)` with default decoding. Separate from the in-writer validation (which compared bytes against the source files).
- Structure: single branch `main`; raw dims (time 61, member 10, step 10, yh 845, xh 775, plus yq 846, xq 776, nv 2 for statics/time bounds), regrid (61, 10, 10, 844, 774). `time`/`valid_time` decode to datetime64[ns] by default; `time` carries `standard_name = forecast_reference_time`; `step` = 0..9 with units `years`.
- `valid_time[init 1965]` = 1965-07-02 12:00 ... 1974-07-02 12:00; `valid_time[init 2025]` = 2025-07-02 ... 2034-07-02 (mid-year of each lead year, as designed; the exact 12:00 comes from the source `lead` offsets 182.5, 547.5, ...).
- Values (init 1995, member 1, lead 2): tos mean 20.8 degC raw / 22.1 degC regrid, range 1.3-28.7; tob -1.8..28.6; land mask 40.2% NaN raw, 38.3% regrid; btm_o2 mean 2.4e-4 mol/kg (it first printed 0.000 only because of my 3-decimal formatting); btm_htotal, btm_co3_ion, btm_co3_sol_arag in plausible ranges.
- Regrid point (42N, -68E) ensemble-mean lead-0 values across inits ~10.8-11.5 degC; mean over all inits lead 9 (13.5) > lead 0 (12.8), consistent with warming.
- Raw statics: geolon/geolat fully finite (-98.44..-36.08E, 5.27..58.16N); wet in {0,1}; deptho max 6500 m. `geolon geolat valid_time` are attached as coordinates (stored in the `coordinates` attribute; xarray moves it to `.encoding`).
- Not checked: a plotted map; non-xarray readers (e.g. rolodex BestEstimate); anonymous (non-Arraylake) access to the virtual chunks; consolidated-metadata/open performance.

### Lessons for the next products (from product 1)
- Timing reality check: my estimate was 15-20 min per dry run and it took 23; `--write` took 18.5 min. Opening is ~0.55 s/file (8 threads, small files); validation dominates. Plan 2x the estimate.
- Always run scripts by absolute path from background jobs (cwd resets to the project root; one run silently did nothing).
- Always compare stored values with `mask_and_scale=False` when validating against h5py reads.
- Put checks that can fail (statics assertions) BEFORE the slow open loop.
- Do not create org resources in a script without first reading `list_bucket_configs`: VCAP URLs must sit inside a bucket config's prefix (see the resolved item above).
- Dry run first (in-memory repo, same validation), then `--write`.

## 2. nwa decadal forecast monthly (r20250925) -> `noaa-cefi/cefi-nwa-decadal-forecast-monthly` (DONE 2026-10-23)
- Script: `ingest/ingest_nwa_decadal_monthly.py` with phases `--parse` (Coiled, workspace `earthmover-devs`), `--reparse-failed` (local patch), `--dry-run`, `--write`. Parse results (virtual datasets + per-file checks + sha1 of source chunk windows used as independent ground truth) are cached in `ingest/.cache/` (gitignore-worthy, NOT durable) so the expensive phase runs once.
- Scale / compute: 2,196 files (1,098 raw + 1,098 regrid, ~1.3 GB each, 2.8 TB total). Local parse would be ~1.4 s/file x 2,196 (GIL-bound, threads do not help) => ~50 min + the same for validation, so Coiled was used: 32 spot workers x 2 vCPU/8 GB, 1 thread each (single-threaded because HDF5 parsing holds the GIL), us-east-1, software env `cefi-ingest` (python 3.13, pinned VirtualiZarr commit). Cluster up in ~2-5 min; parse of all 2,196 files finished in ~5 min of wall time (~7 min with env build and retries). Total cluster time for two runs (first lost to the bug below) ~15 min.
- Coiled gotcha: the default Coiled account on this machine was NOT `earthmover-devs`; every call needs `workspace="earthmover-devs"`. The user then made it the default (`~/.config/dask/coiled.yaml`).
- **UNEXPECTED #1 (first cluster run lost):** the first parse run hit one failing task after ~1,800 files and, because results were only saved at the end, lost everything. Fix: tasks now return error records instead of raising, and results are saved even with failures. Lesson: persist before judging.
- **UNEXPECTED #2 (source defect):** `regrid/nsmz_100...i199601.nc` has zero chunks written (shape/chunks/attrs present, 119 MB file vs 1.36 GB for its siblings). It reads as all NaN. VirtualiZarr cannot build a manifest for it (tries to allocate an 11.7 GiB string array: a library bug on empty manifests). Header-level scanning in the planning phase missed it because it only checked metadata. A file-size outlier check over the whole release found no other such file (all 2,195 others within 3% of their variable's median size).
  Handling (faithful to the source, which also reads as NaN): the variable is dropped at parse time and rebuilt for that init as a virtual array with an EMPTY chunk manifest, so it reads as the fill value (NaN); validation hashes the source's (fill) read against the store's. Recorded in `SOURCE-FIXES.md`.
- **UNEXPECTED #3:** this release's `ocean_static.nc` is netCDF3, not HDF5 (VirtualiZarr `HDFParser` fails with "file signature not found"); read with scipy like the hindcast static. 228,416 masked coordinate cells; same 288-cell `wet` difference from the hindcast as in the yearly product, so the hybrid-statics decision applies unchanged.
- `lead` has a single distinct vector across the files checked (identical offsets in every init, 15.5, 45, 74.5 ... days) even though the units are Gregorian "days since <init>"; decoded valid_time is asserted to fall in the month init + step for every init.
- Build fix on the way: the empty-variable rebuild first failed because it carried the sibling init's `lead` coordinate (4 distinct `lead` vectors across the 61 inits, i.e. the leap-year drift from the audit's P7); xarray tried to realign and VirtualiZarr cannot split chunks. Fixed by building a bare `xr.Variable` (no coordinates).
- **DONE 2026-10-23:** commit `CX18CBBVKTAFQWKJQPK0` on `main`. Groups `raw/main` (time 61, member 10, step 120, yh 845, xh 775; 18 vars; hybrid statics; time fields once per (time, step)) and `regrid/main` (61, 10, 120, 844, 774; 18 vars). Repo tags set. Validation of the committed repo (read back through Arraylake) against sha1 of source chunk windows hashed independently on the Coiled workers: raw 2,292 windows + regrid 3,012 windows, all match (every chunk of tos and btm_o2 for inits 196501/196801/202501 plus one pseudo-random chunk window for each of the 2,196 files). The never-written `nsmz_100` init reads NaN, matching the source. Dry run on the full cache first (11.5 min), then the write + validation (11.7 min).
- Source-member check: `average_T1/T2/DT`, `time_bnds` member rows identical in all 1,098 raw files (otherwise the run aborts: it did not).
- Consumer-style sanity check (fresh process, 2026-10-23): single branch `main`; `step` 0..119 units `months`; `valid_time` in the month init+step for all 61 inits (mid-month, e.g. 1965-01-16 12:00, 1965-02-15 00:00; the 12:00/00:00 alternation comes from the source `lead` offsets); tos mean 21.7 degC raw / 22.9 degC regrid, land NaN 40% / 38%; btm_o2 mean 2.4e-4 mol/kg; MLD_003 mean ~20 m; time fields are datetime64 (time, step[, nv]); geolon finite; wet {0,1}; deptho max 6500 m; `nsmz_100` init 1996-01 all NaN while the neighbouring init and other variables at 1996 are normal.
- Not checked: a plotted map; non-xarray readers.
- Coiled spend: two cluster runs (the first one lost by the all-or-nothing save; ~7 min each, 32 spot x 2 vCPU).
- Lessons: persist results before judging them; use bare Variables for rebuilt entries; one cluster pass can parse all 2,196 files in ~5 min, so re-running is cheap and a local `--reparse-failed` patch is cheaper still.

### Decision (user, 2026-10-23): group naming -- no `main` level unless there are sibling grid groups
The decadal monthly repo was first written with `raw/main` and `regrid/main` (my plan reused the multi-grid naming template). `main` added nothing there and made it inconsistent with the decadal-yearly repo (`raw`, `regrid`). User decision: remove the extra level.
- **Rule (user decision, from the example of decadal monthly):** where a side (`raw` or `regrid`) has ONE horizontal-grid group, its variables sit directly in `raw` / `regrid`; there is no `main` level. `main` is kept only as the name of the h-grid group when it has sibling groups (`raw/main` + `raw/u_grid` + `raw/v_grid` ...; `regrid/main` + `regrid/ice`).
- **Applies now (flat `raw` and `regrid`):** NEP hindcast daily, NWA hindcast daily, NWA seasonal forecast, NWA seasonal reforecast, NWA decadal forecast yearly (done), NWA decadal forecast monthly (done, changed).
- **Keep `raw/main` (has u/v/ice siblings):** NEP hindcast monthly, NWA hindcast monthly, NWA multi-decadal outlook, Pacific Islands daily and monthly. **Open (Claude's proposal, not yet confirmed by the user):** for those products the regrid side with a single group (NEP monthly, multi-decadal, Pacific Islands) would then be plain `regrid`, while NWA hindcast monthly keeps `regrid/main` + `regrid/ice`. Confirm when each of those schemas is next shown.
- Older plan text in this file that says `raw/main` / `regrid/main` for the "applies now" products is superseded by this rule.
- Done for `cefi-nwa-decadal-forecast-monthly` with the Icechunk rearrange API (metadata-only, no data touched): `repo.rearrange_session("main")`; `session.move("/raw", "/_old_raw")` then `session.move("/_old_raw/main", "/raw")` (a direct `move("/raw/main", "/raw")` fails because `/raw` exists), then delete the empty placeholder group in a normal writable session. Same for `regrid`. Commits `A4Z27KE00FJH4CS86JQ0` (move) and `3TQNNWD697G0W64J0020` (placeholder cleanup); old commit `CX18CBBVKTAFQWKJQPK0` remains in history. Verified against the pre-move snapshot: 38 raw + 24 regrid arrays, shapes/chunks/attrs identical, data identical (small arrays fully, large arrays one chunk). Tested first on a throwaway in-memory repo. `ingest_nwa_decadal_monthly.py` updated so a rerun writes the flat layout.

## 3. nwa seasonal forecast (r20250710) -> `noaa-cefi/cefi-nwa-seasonal-forecast-monthly` (DONE 2026-10-23)
- Script: `ingest/ingest_nwa_seasonal.py --product forecast` (also handles the reforecast). Flat groups `raw`, `regrid` (user rule 2026-10-23). 144 files (4 inits x 18 vars x 2 grids), each bundling `<var>_anom`, scalar `init`, scalar `month` and (raw) `valid_time`.
- Result: `raw` (time 4, member 10, step 12, yh 845, xh 775) + core-5 statics; `regrid` (4, 10, 12, 844, 774); 36 arrays per grid (18 + 18 `_anom`), dim order (time, member, step, y, x) with chunks (1, 10, 12, 200, 200). Virtual commit `5TCDSA8WQS353VV4BR5G`; native init 202510 committed per grid (`XX1SBC5X2HQPGPC750J0` raw, `XTHFRJFKS6X8HW6DPGG0` regrid).
- **How each planned item was realised (all tested for real):**
  - transpose (lead, member) -> (member, step): Zarr `transpose` codec prepended to every forecast array. The time axis must be added BEFORE the permutation (`ma[np.newaxis, ...]`, then a 5-D permute): VirtualiZarr's `expand_dims`/`stack` rebuilds metadata and fails on a transpose codec whose order no longer matches the dimensionality. Placeholder (empty-manifest) arrays need an explicit chunk-grid shape.
  - init 202510 native: the virtual concat gets a placeholder (sibling init's metadata, EMPTY manifest, so the chunk grid matches), then the data is region-written with `xarray.to_zarr(region={"time": slice(0, 1)})` (user decision 2026-10-20), member 6 = NaN. Works on arrays carrying the transpose codec. Written on Coiled workers via `session.fork()` + `session.merge()` per grid.
  - fill NaN: CORRECTED 2026-10-23 (my first note here was wrong: it looked at the netCDF `_FillValue` attribute, not the HDF5-level fill). Proper check (`--fillcheck`, Coiled, all 144 files, full scan): raw arrays all have HDF5 fill 9.97e36 (144/144), regrid all NaN (144/144); every chunk of every array is written; zero stored values with |x| > 1e30; NaN fraction 0.402 raw / 0.383 regrid (= land). So overriding raw's fill to NaN is lossless (the fill only applies to unwritten chunks, and there are none).
  - `valid_time` = init + step months (month starts); for raw it was asserted equal to the source's per-file `valid_time` for every file (days since init). Source `lead` = 0..11 int32 months in all 144 files; members 1..10 in every file except 202510 (1-5, 7-10); coordinates identical across all files within each grid.
  - dropped per-file scalars `init`, `month` (both derivable from `time`) and raw `valid_time` (recomputed, 2-D). `_anom` arrays keep their names with attrs `long_name` "<base long_name> anomaly", `cefi_base_variable`, and a comment that the reference climatology is not described in the files.
  - statics: this release's `ocean_static.nc` (netCDF3) masks land with 1e20 in geolon/geolat/areacello/wet (228,416 cells) and deptho (263,153); coordinates from the hindcast static (hybrid rule), deptho/areacello/wet from this release's own file, as for the decadal products.
- Validation: chunk windows hashed from the source files on the parse workers (every chunk of 3 inits x 36 arrays x 20 chunks, 2,160 windows per grid) == the committed store, via the transpose; native 202510: all 36 arrays per grid == source read independently (member 6 all NaN). Run on Coiled (54 tasks per grid + 18 native checks per grid).
- Consumer-style check (fresh process): single branch `main`; dims/chunks as above; `step` 0..11 `months`; `valid_time` 2025-10-01..2026-09-01 for the 202510 init (month starts, e.g. 2025-10-01, 2025-11-01, 2026-01-01); member 6 is all NaN only for 202510; tos member-1 mean 19.9/18.1/24.5 degC (202510/202601/202607) raw, 21.4/19.8/25.8 regrid, land NaN 40%/38%; `tos_anom` mean +0.39 raw / +0.44 regrid (plausible, not verified further).
- Tags were first missing (the killed local run never reached that step); set afterwards.
### Compute / process notes (seasonal forecast)
- I first ran this product entirely locally ("it is small") and it was NOT: parse 14.6 min, two dry runs 43 + 27 min, and the write was heading for another hour (reading 20 GB of native-init source from NODD over the laptop link plus frequent transient `StorageError` retries). User then asked for Coiled: the same remaining work (regrid native write + validating the whole committed repo) took 10.5 min on 8 spot workers. RULE for the rest: parse AND validate on Coiled; only the metadata commit is cheap enough locally.
- **Memory:** the first dry run was silently killed (no traceback) when the native init for both grids was held in memory (~10 GB per grid). Stream one variable at a time.
- **Distributed Icechunk pattern that works with Arraylake repos:** workers receive `repo.readonly_session(...).store` (pickles to ~4 KB, reads virtual and transposed chunks in a fresh process) or `repo.writable_session("main").fork()`; the client merges the returned forks with `session.merge(*forks)` and commits. The worker software env needs `arraylake` too (pickled sessions import it) -> env `cefi-ingest-2`.
- Local parse caches live in `ingest/.cache/` (not durable, ~220 MB, VirtualiZarr manifests + chunk hashes, NOT NetCDF data). No NetCDF files were ever downloaded to disk by this work.

## 4. nwa seasonal reforecast (r20250413) -> `noaa-cefi/cefi-nwa-seasonal-reforecast-monthly` (DONE 2026-10-23)
- Script: `ingest/ingest_nwa_seasonal.py --product reforecast`. 960 files (120 quarterly inits 1994-01..2023-10 x 4 vars x 2 grids), each with `<var>_anom`. Flat groups `raw`, `regrid`.
- Result: `raw` (time 120, member 10, step 12, yh 845, xh 775; chunks (1,10,12,200,200)) + core-5 statics; `regrid` (120, 10, 12, 844, 774; chunks (1,4,4,282,258) as published); 8 arrays per grid (sos, sob, tos, tob + `_anom`). No transpose (files already (member, lead)); no native init. Commit `F6FNF9SEA2JZ7VKVT2NG` on `main`; tags set.
- Compute (all Coiled, `earthmover-devs`, 16 spot workers x 2 vCPU, env `cefi-ingest-2`): parse 4 min (first pass, ~$0.x) then 20 min for the second pass that adds the full-array fill scan (reads all ~600 GB uncompressed); write + validation 3 min wall (1,416 raw + 2,880 regrid source chunk windows == committed store).
- **UNEXPECTED #1 (my check was vacuous):** I first judged the fill override safe because the netCDF `_FillValue` ATTRIBUTE was NaN in every file. But the header scan's fill split (9.97e36 vs NaN) is the HDF5-LEVEL fill value (`dataset.fillvalue`), which is what unwritten chunks hold; my parse never looked at it, so the "no stored 9.97e36" scan never ran ("0 hits" was meaningless). Fixed: per array record `hdf_fill`, chunks written vs expected, and a full scan of every stored value. Result for the reforecast: raw 780 arrays HDF5-fill 9.97e36 + 180 NaN; regrid 960 NaN; EVERY chunk of EVERY array is written; zero stored values with |x| > 1e30 in all 1,920 arrays => overriding the fill to NaN is lossless (the fill only ever applies to unwritten chunks, and there are none). The check is now a hard assertion in `check_file_set`.
- **UNEXPECTED #2 (metadata inconsistency, harmless):** reforecast raw scalar `init` is `days since 1994-01-01` or `1994-04-01` with an OFFSET value in 473 of 480 files (and `days since <init>` = 0 in the others and in all regrid files); units + value decode to the filename init in all 960 files, `month` also agrees everywhere. My first check demanded `days since <init>` = 0 and failed; now it decodes. Recorded in SOURCE-FIXES.md.
- `lead` has no units attribute in the reforecast (values 0..11); asserted to be exactly 0..11 in every file. Coordinates identical across all 120 inits per grid; members 1..10 everywhere.
- Consumer check (fresh process): `time` 1994-01-01..2023-10-01 quarterly and monotonic; `valid_time == init + step months` (sampled inits); tos means 17.5-18.8 degC raw / 19.2-20.4 regrid, land NaN 40%/38%, no all-NaN member; `sos` ~35.1, `tob` ~3.7, anomalies near 0 (sos_anom +0.03, tob_anom -0.04); no value anywhere near 9.97e36 in sampled arrays.
- Follow-up DONE: the same vacuous fill check had applied to the seasonal FORECAST repo; re-checked properly with `--fillcheck` on Coiled (4.5 min): lossless there too, see section 3 (corrected).

## 5. nwa hindcast daily (r20250715) -> `noaa-cefi/cefi-nwa-hindcast-daily` (DONE 2026-10-23)
- Script: `ingest/ingest_nwa_hindcast_daily.py` (`--parse` Coiled, `--dry-run`, `--write`). 22 data files (11 variables x 2 grids; one file per variable over the whole period, 8.5-13.9 GB each), no concatenation. Flat groups `raw`, `regrid` (user rule 2026-10-23).
- Result: `raw` (time 11322, yh 845, xh 775; chunks (100,200,200)) 11 variables + native time_bnds (time, nv), average_T1/T2 (datetimes), average_DT + statics geolon, geolat, deptho, areacello, wet, geolon_c, geolat_c; `regrid` (11322, 844, 774) 11 variables. Commit `7Y70H6477B6Y74NG4WP0`; tags set. `time` 1993-01-01T12:00 .. 2023-12-31T12:00 (11,322 consecutive days, Gregorian).
- Compute (Coiled, `earthmover-devs`, 11 spot workers x 2 vCPU, env `cefi-ingest-2`): parse 4 min, write + validation 3 min wall. 1,342 source chunk windows per grid (122 per file: random + corner chunks) hashed on the workers == committed store, for all 11 variables.
- **UNEXPECTED #1 (fill value would have been wrong):** raw land is stored as exactly 1e20 (= the file's `_FillValue` attribute; 40.2% of cells; 1.82 billion sampled values, zero NaN, zero 9.97e36, zero anything else), but VirtualiZarr sets the zarr `fill_value` from the HDF5-level fill (9.97e36). Left alone, a reader would see land as 1e20 data. NaN is not an option (the stored bytes are 1e20 and a virtual store cannot change them), so the metadata `fill_value` is set to 1e20 (store-side metadata only). Regrid stores NaN and its metadata fill is already NaN. The consumer view then masks correctly (NaN fraction 0.402 raw / 0.388 regrid). The script now classifies every stored value of the sampled windows into NaN / attribute-fill / HDF5-fill / other and asserts ONE sentinel per grid; use this check for EVERY remaining product (earlier repos were verified with the consumer view and, for the seasonal ones, a full scan).
- Every chunk written in every file (2,280 of 2,280 per array); `time`, `nv`, coordinates identical across the 11 files of each grid; the four time extras identical across the 11 raw files.
- Statics: this release's `ocean_static.nc` (netCDF3) is NOT masked (0 masked cells in geolon/geolat/_c/areacello/wet; deptho 263,441 land cells masked, as expected) and is identical to the NWA hindcast monthly r20250715 static for all seven fields; no substitution was needed (unlike the decadal and seasonal releases).
- Consumer check (fresh process): single branch; daily step exactly 1 day everywhere; `btm_o2` max 3.7e-4 / min 0 mol/kg raw, no value > 1e15; time extras are datetimes; geolon finite; wet {0,1}; deptho max 6500 m.

### Decision (user, 2026-10-23): regrid side with a single group is flat `regrid`
For products whose raw side keeps `raw/main` + `raw/u_grid` + `raw/v_grid` (siblings), the regrid side, having ONE group, is plain `regrid` (variables directly in the group), per the rule "no `main` level unless sibling groups exist". Applies to: Pacific Islands daily + monthly, NEP hindcast monthly (b_x/b_y siblings on raw only), NWA multi-decadal outlook. NWA hindcast monthly keeps `regrid/main` + `regrid/ice` (siblings). Resulting layouts: PCI daily/monthly: `raw/main`, `raw/u_grid`, `raw/v_grid`, `regrid`; NEP monthly: `raw/main`, `raw/u_grid`, `raw/v_grid`, `raw/b_x`, `raw/b_y`, `regrid`; multi-decadal: `raw/main`, `raw/u_grid`, `raw/v_grid`, `regrid`; NWA monthly: `raw/main`, `raw/u_grid`, `raw/v_grid`, `raw/ice`, `raw/ice_xB`, `raw/ice_yB`, `regrid/main`, `regrid/ice`.

## 6. pacific_islands hindcast daily (r20260427) -> `noaa-cefi/cefi-pci-hindcast-daily` (DONE 2026-10-23)
- Script: `ingest/ingest_pci_hindcast_daily.py` (`--parse` Coiled, `--dry-run`, `--write`, `--validate`). 34 data files (18 raw + 16 regrid; 9-17 GB each), one file per variable over the whole period (1993-01-01..2025-12-31, 12,053 days). Commit `248DT400RB8V04X0DFRG`; tags set.
- Layout (user decisions 2026-10-23): `raw/main` (time, yh 539, xh 725; 16 variables; chunks (20,100,100)) + native time_bnds, average_T1/T2/DT + statics geolon, geolat, deptho, areacello, wet, geolon_c, geolat_c; `raw/u_grid` (time, yh, xq 726; ssu); `raw/v_grid` (time, yq 539, xh; ssv); `regrid` flat (time, lat 538, lon 724; 16 variables; chunks (100,200,200)).
- Compute (Coiled, `earthmover-devs`, 12 spot workers x 2 vCPU, env `cefi-ingest-2`): parse 10.5 min (302 windows hashed + classified per file), write ~1 min locally, validation 5 min on the cluster for 10,268 source chunk windows over all 34 arrays == committed store.
- Same fill problem as NWA hindcast daily: raw land is stored as exactly 1e20 (= `_FillValue` attribute), VirtualiZarr reads the HDF5 fill (9.97e36); zarr `fill_value` set to 1e20 in the store. 800 M+ sampled raw values: only valid values and 1e20 (no NaN, no 9.97e36, nothing else); regrid stores NaN. Every chunk written (28,944 per raw array). In SOURCE-FIXES.md.
- Statics: product's own `ocean_static.nc` is NETCDF4 (parsed with the HDF5 parser, unlike the NWA releases), geolon/geolat/_c have 0 masked cells and cover 171.00..220.54 E, 8.94..38.09 N (the Pacific Islands region, as expected). Source quirk (kept as published): `wet` is 1e20 at 8,486 land cells and 0 at 479 cells (not a clean mask), areacello 1e20 at 8,486, deptho 1e20 at 8,965; read as NaN in the consumer view.
- **UNEXPECTED #1:** `xq` (u_grid) and `yq` (v_grid) were left as VIRTUAL coordinates (I had not loaded them natively); opening the group then needs a source read and hit a transient local DNS error. Fixed (`nativize_coords`: any coordinate still a ManifestArray is read straight from the file; asserted equal to the static file's xq/yq). RULE: coordinates must always be native; check for ManifestArray coordinates in every build.
- **UNEXPECTED #2:** the first validation on the cluster ran >18 min then a task was cancelled with an unknown reason (a hung NODD read or lost spot worker; one random chunk read takes ~0.2 s, so 34 tasks should take ~2 min). Validation now: batches of 40 windows, `retries=2`, a 10-minute deadline per round, stragglers resubmitted; re-run via `--validate` took 5 min with all 272 batches in round one. The write had already committed, so only validation was repeated.
- Stray `nv` coordinate dropped from `raw/u_grid`/`raw/v_grid` (no `time_bnds` there).
- Consumer check (fresh process): top level `raw`, `regrid`; raw has `main`, `u_grid`, `v_grid`; groups open in 0.4-1.4 s; daily step exactly 1 day in every group; tos at t=6000: mean 23.4 degC, NaN 2.3% raw vs 15.0% regrid (the regrid grid evidently includes corner cells outside the raw curvilinear domain; not investigated further); ssu/ssv within +-0.56 m/s; no value > 1e15.

## 7. pacific_islands hindcast monthly (r20260427) -> `noaa-cefi/cefi-pci-hindcast-monthly` (DONE 2026-10-26)
- Script: `ingest/ingest_pci_hindcast_monthly.py` (`--parse` Coiled + resumable per-file cache, `--dry-run`, `--write` committing per group, `--validate`). Scope after exclusions: 754 data files (raw 418: main 401 + u_grid 8 + v_grid 9; regrid 336 incl. `speed`), ~2.7 M virtual chunk references on the raw side (4-D chunks (10,10,50,50): 39,600 per array).
- **Third data-less raw file found:** raw `speed.*.nc` (35 KB) has coordinates and time fields but NO `speed` array at all (the regrid `speed` file is fine). With `T_adx_2d` and `ffedet_btm` (zero time steps, no chunks written) that makes 3 empty raw files; they are excluded (`EXCLUDED`). This also explains the plan's counts: 421 raw data files - 3 = 418 variables.
- **INCIDENT 2026-10-24/25 (cost ~2 h of 24 spot workers, a few dollars; nothing saved):** the first Coiled parse ran 2 h 2 min then died with `FutureCancelledError ... cancelled for reason: unknown`, and its results were lost because they were only written at the end. Cause: the Coiled CLIENT runs on the laptop, which was on battery and entering deep-idle sleep (`pmset -g log`: repeated `Entering Sleep state ... Using Batt`, a DarkWake about every 17 min). A sleeping client drops its scheduler connection and the scheduler cancels its futures. Wall-clock timings measured while the laptop slept are meaningless: e.g. a local test showed `chunk_iter` over 39,600 chunks taking 27,441 s, but with an instrumented reader the same walk needs only ~670 range requests (706 MB at 1 MB blocks) and ~20 s of CPU; my 240 s alarms fired after 7,000+ s. Two earlier "cancelled for reason: unknown" events (pci daily validation, 18 min) and some of the slow local runs I attributed to NODD/network in the seasonal forecast section were very likely sleep too.
- **RULES from this incident:** (1) wrap every long client command in `caffeinate -i` (prevents idle sleep; keep the laptop plugged in and the lid open: closing the lid on battery still sleeps it); (2) results are saved per file the moment they arrive and every parse is resumable; (3) a hard deadline on the parse (45 min) so nothing runs away silently; (4) never judge speed from wall-clock without checking `pmset -g log` for sleep.
- **Retry history (after the sleep incident):** the second parse attempt failed at cluster creation (`Cluster status is stopped (reason: Scheduler process exited)`, 16 min, nothing ran; Coiled gave no logs; retried once with a larger scheduler, m6i.xlarge, which came up). The third attempt (24 spot workers x 2 vCPU, `caffeinate -i`) parsed all 754 files in 887 s (~15 min), 0 failed, 574 MB of per-file pickles in `ingest/.cache/pci_hindcast_monthly/`. So a 4-D file with 39,600 tiny chunks is NOT slow to parse; the hours seen earlier were laptop sleep.
- **Build findings (all fixed in the script):** (1) the monthly `ocean_static.nc` looked different from the daily one, but only because loading decodes the 1e20 fill to NaN and `np.array_equal` treats NaN != NaN (and the "masked cells" counter read 0 for the same reason): the two files are identical (same 8,965 / 8,486 / 8,486 masked cells, 0 differences); checks now NaN-aware. (2) the vertical interface coordinates are `z_i` (53) and `zi` (76), not just `zi`: `z_i` was left as a virtual coordinate and my native-read helper looked in the wrong file; `nativize_coords` now finds a carrier file per coordinate and cross-checks 3 more. (3) unused dimension coordinates (`z_i` in groups with no variable on it) are dropped.
- **Result (commits `94A0Y91CFCMYKZ7H1AYG` raw/main, `36JZ35M9VDC71WJ3G6BG` raw/u_grid, `17JQ316SQKNEYSWCP2JG` raw/v_grid, `T7RZ7RR5M0YHGT208NWG` regrid):** `raw/main` (time 396, z_l 52, zi 76, yh 539, xh 725) 401 arrays (68 of them 4-D, chunks (10,10,50,50)) + native time extras + core statics; `raw/u_grid` (yh, xq 726) 8 arrays; `raw/v_grid` (yq, xh) 9 arrays; `regrid` (lat 538, lon 724) 336 arrays (1 4-D `calc`, chunks (100,10,200,200)). Tags set. Raw: every array stores 1e20 as 'no data' (127 M sampled values, no NaN, no other sentinel) and the zarr fill_value is set to 1e20; regrid: every array stores NaN. 66 bundled `volcello` copies dropped from other variables' files (the standalone `volcello` is present); `T_adx_2d`, `ffedet_btm`, raw `speed` are absent as planned. Statics identical to the daily product's.
- Validation: 33,168 source chunk windows over all 754 arrays (1,508 batches of 40, one round, 7 min on 24 spot workers) == the committed store. Whole run (write local + validation) 8.7 min.
- Consumer check (fresh process): 5 commits on `main`; groups open in 0.45-1.4 s; 396 monthly steps 1993-01-16..2025-12-16 (28-31 d); tos NaN 2.3% raw / 15.0% regrid at t=200, `volcello` min 8.4e6 max 2.0e8 m3, `rsdo` on `zi`, `calc` ~1e-6, `speed` (regrid) 0.05-0.69 m/s, no value > 1e15.

## 8. nwa multi-decadal outlook (r20260331) -> `noaa-cefi/cefi-nwa-multi-decadal-outlook-yearly` (DONE 2026-10-26)
- Script: `ingest/ingest_nwa_multidecadal.py` (`--parse` Coiled + resumable cache, `--dry-run`, `--write`, `--finish`, `--validate`). 108 files (raw 76 = 19 variables x 4 SSPs, regrid 32 = 8 x 4), 3-12 GB each, one per (variable, scenario). Layout (user rules): `raw/main` (scenario 4, time 131, [z_l 52], yh 845, xh 775; 8 arrays + native time_bnds + statics), `raw/u_grid` (xq 776; 5 arrays incl. T_adx), `raw/v_grid` (yq 846; 6 arrays), `regrid` flat (lat 844, lon 774; 8 arrays). Scenario axis = native string coordinate, chunk 1; time 1970-07-02..2100-07-02 (131 yearly steps), chunks inherited ((1,44,282,259) 3-D, (1,22,8,141,130) 4-D).
- Commits (all on `main`): raw/main `HZSQ8P7JZ6QJAAV76FPG`, raw/u_grid `9YB1ZT78JSKQEQ932VQG`, raw/v_grid `4TFEDHSQ7P2E1GHM5XNG`, regrid `0JPJ8G6FK9R9QPWKPHFG`, native T_adx SSP585 `SWBJJWSSF9DXT0FGTC80`. Tags set.
- Compute (Coiled, `earthmover-devs`, 12 spot workers x 2 vCPU, m6i.xlarge scheduler, `caffeinate -i`): parse 5.6 min (108 files, 0 failed), native write + validation ~12 min, validation rerun 10.7 min. Write itself ~1 min.
- Checks (step 5): the 107 normal files share one time axis (hash), z_l, xh/yh/xq/yq/lat/lon identical across files, time_bnds identical; `T_adx` SSP585 has 130 steps and exactly one missing step: index 75, year 2045, all other time values equal the common axis. Every array stores ONE sentinel (raw 1e20, regrid NaN, same across the four scenarios of each variable); every chunk written in every file; fill_value set accordingly.
- **Decision (user, 2026-10-26) / UNEXPECTED #1, raw statics:** this release ships no static file. The plan borrowed the NWA hindcast monthly static, but the mask check I added (compare where tos SSP126 year 1 has data with the borrowed `wet`/`deptho`) found data at 288 cells the hindcast calls land (391,722 data cells vs 391,434 ocean cells), the same 288 cells as in the decadal release. The NWA decadal-forecast r20250819 static matches the data exactly (391,722; 0 mismatches either way). User chose the hybrid: geolon/geolat/geolon_c/geolat_c from the hindcast static (gap-free), deptho/areacello/wet from the decadal-yearly static (1e20 -> NaN), recorded in each field's attrs; the mask is now a hard assertion at ingestion (0 mismatches). This supersedes the plan line "all statics from the hindcast monthly static".
- **P6 native T_adx SSP585 (raw/u_grid):** placeholder slot (SSP126's metadata, empty manifest) in the virtual stack, then 42 blocks (6 time chunks x 7 depth chunks of 22 x 8 steps) read from the 130-step source file and region-written by Coiled workers into forked sessions (`to_zarr(region={"scenario", "time", "z_l"})`), real years at their true time positions (matched by time value, not position), merged and committed. **UNEXPECTED #2:** the missing year 2045 is stored as 1e20, not NaN: xarray encodes NaN as the target array's existing `_FillValue` (1e20, the same marker land uses), and the consumer view decodes it to NaN (all NaN in the xarray view; the other three scenarios have real 2045 values). My first validation demanded stored NaN and flagged 7 'missing year' blocks (one per depth block); the check now accepts NaN or the array's fill value and separately the consumer check confirms NaN. All other native blocks equal the source exactly.
- **UNEXPECTED #3:** Arraylake rejects repo descriptions over 255 characters (pydantic `string_too_long`); the first `--write` died at repo creation before writing anything. The description is now asserted <= 255 characters in the script.
- Validation (Coiled): 6,414 source chunk windows over 107 (variable, scenario) arrays (every chunk of all 3-D arrays, corners + 100 random windows of each 4-D array) == committed store; 42 native blocks == source; year 2045 no-data.
- Consumer check (fresh process): 6 commits on `main`; groups open in 0.2-0.6 s; scenarios ['SSP126','SSP245','SSP370','SSP585']; `tos` means (1970/2045/2100) 19.4/20.2/20.2 (SSP126) ... 19.5/20.9/22.5 (SSP585) degC, higher SSP warmer by 2100; `so` ~35; `T_adx` SSP585 year 2045 = NaN while the other three scenarios have values; time_bnds 1970-01-01..1971-01-01; geolon finite; wet {0,1}; deptho max 6500 m; nothing > 1e15.

## 9. nep hindcast daily (r20260701), 16 two-dimensional variables -> `noaa-cefi/cefi-nep-hindcast-daily` (DONE 2026-10-26)
- Script: `ingest/ingest_nep_hindcast_daily.py` (`--parse` Coiled + resumable cache, `--dry-run`, `--write`, `--validate`). 32 files (16 raw 98.7 GB + 16 regrid 58.0 GB; files named `*.199301-202512.nc` in the r20260701 directory only: the directory also holds other releases and the per-year 4-D files, which stay excluded per P2). Flat groups `raw` (time 12053, jh 816, ih 342, + jq 817, iq 343 for geolon_c/geolat_c; chunks (213,109,45)) and `regrid` (lat 815, lon 341; chunks (100,200,200)). Commit `7699H5WBXYHBECQ4B85G`; tags set.
- Compute (Coiled, `earthmover-devs`, 12 spot workers x 2 vCPU, `caffeinate -i`): parse 4 min, write + validation 3.7 min wall. 4,864 source chunk windows over 32 arrays == committed store (150 windows per raw/regrid file + corners).
- Same raw fill issue as the other daily products: land stored as 1e20 (= `_FillValue` attr) in all 16 raw arrays (568 M sampled values; no NaN, no other sentinel), zarr fill_value 1e20; regrid stores NaN (4.4 G sampled values NaN, 2.4 G valid). Every chunk written (3,648 per raw array, 1,210 per regrid array).
- Checks (all hard assertions): time axis (12,053 days, Gregorian), ih/jh/iq/jq/nv, geolon/geolat/_c, time_bnds, average_T1/T2/DT identical across all 16 raw files; lat/lon identical across the 16 regrid files. This release's own netCDF3 `ocean_static.nc` has the SAME dim names as the data (ih 342, iq 343, jh 816, jq 817: no renames needed), 1e20 on land (deptho 73,374 cells, areacello/wet 57,677; stored NaN); its land/sea mask matches where the data has values exactly (205,698 cells; 0 mismatches either way: no borrowed-static problem here). Bundled geolon/geolat equal the static's on 221,395 of 279,072 cells (the rest masked in the static).
- Consumer check: groups open in 1.0 / 0.3 s; daily step exactly 1 day; tos t=6000: raw NaN 26%, regrid NaN 64% (the regrid box is a large rectangle around the curvilinear NEP domain that reaches the Arctic, so most of it is outside the model domain); geolon 156.92..254.97 E, geolat 10.81..80.72 N, finite; wet {0,1}; deptho max 6500 m; time extras are datetimes; no value > 1e15.
- The excluded per-year 4-D variables (the P2 problem): see the note in `INGESTION_REQUIREMENTS.md`. Verified 2026-10-26: HDF5 stores the last (partial) time chunk of a per-year file as a FULL-size (100,10,200,200) chunk padded with the fill value (checked on regrid `thetao` 1993, 365 days: 35 padded steps all NaN; 1996, 366 days: 34 padded steps all NaN; only one corner chunk sampled, which is all-NaN so the real-step content is not verified). That makes a (year, day-of-year 366) virtual layout look feasible without variable-length chunks. NOT tested end to end; recorded as an idea, awaiting the user's decision on a prototype.
- Repo tags added on `cefi-nep-hindcast-daily` (user, 2026-10-26, after the question "you just ingested the dataset that needed variable-length chunks"): `coverage` = "2D variables only (16 per grid); the per-year 3D variables are NOT included" and `excluded` = "per-year 3D ocean state (thetao, so, o2, no3, dissic, uo, vo, volcello, ...): needs variable-length chunks". The repo is deliberately incomplete (about 3% of the product's data); the name alone does not say so. Lesson: when reporting a product "done" with a documented exclusion, state the exclusion and its share of the data in the same sentence.

## 10. nep hindcast monthly (r20260701) -> `noaa-cefi/cefi-nep-hindcast-monthly` (DONE 2026-10-26)
- Script: `ingest/ingest_nep_hindcast_monthly.py` (`--parse` Coiled + resumable cache, `--scan-bad`, `--dry-run`, `--write`, `--validate`). 898 files (raw 461, regrid 437), one per variable over 396 months. Groups: `raw/main` (437 arrays, 65 of them 4-D; z_l 52, zi 76, ct 5; + native bundled geolon/geolat/geolon_c/geolat_c, time_bnds, average_T1/T2/DT, deptho/areacello/wet), `raw/u_grid` (9; iq 343), `raw/v_grid` (9; jq 817), `raw/b_x` (3; xB 343), `raw/b_y` (3; yB 817), flat `regrid` (437; lat 815, lon 341). The 62 bundled `volcello` copies were dropped (the standalone `volcello` is present once). Commits: raw/main `RCV6Z84KJH9C3PV27ZH0`, raw/u_grid `8D2E2XKC7YD0N2AYR090`, raw/v_grid `8GHZPPSHNFPD8CJSRTV0`, raw/b_x `M0ABV3CB0X8ZN55J88NG`, raw/b_y `VQ706ACMMFJ2R1W8XX1G`, regrid `3TZP1BZXBMXC3NVNPP9G`. Tags set (incl. `data_quality`).
- Compute (Coiled, `earthmover-devs`, 24 spot workers x 2 vCPU, `caffeinate -i`): parse 14.7 min (898 files, 0 failed, 480 MB cache); `--scan-bad` x2 (8 workers, a few min each); write + validation 13.6 min wall (39,670 source chunk windows over all 898 arrays == committed store, 1,423 batches in one round).
- Checks (hard assertions): the product's own netCDF3 `ocean_static.nc` (same dim names as the data) matches where the data has values exactly (205,698 cells, 0 mismatches either way); bundled geolon/geolat == the static's on 221,395 of 279,072 cells; time axis, coordinates (ih/jh/iq/jq/xB/yB/z_l/zi/ct/lat/lon/geolon...) and time extras identical across all files (time unit spelling differs in places, compared by base date and values); every chunk written in every file; ONE sentinel per array: raw 1e20 (most), -1e34 (the ice arrays incl. all of raw/b_x and raw/b_y), regrid NaN.
- **UNEXPECTED / decision (user, 2026-10-26): source garbage values.** The strict sentinel check stopped on raw `limfemisc`: 282 of 110,512,512 values are garbage (-1e24 .. -1e19; valid range 0.05-1.0). Sample-based count over all 898 arrays found exactly 8 affected arrays (`limfemisc`, `limnmisc`, `limpmisc`, `limirrmisc`, raw and regrid) and nothing else. A full scan on Coiled gave exact counts: raw 282/254/282/282 and regrid 135/129/135/135 values outside [-1, 2] (in 67/57 and 30/28 of 396 months, all within one small region), no values slightly outside [0, 1]. User chose "ingest as published + flag": data unchanged; the 8 arrays are the only allow-listed ones (every other array must still have zero such values); each carries a `cefi_data_quality` attribute with its exact numbers; repo tag `data_quality`; row in SOURCE-FIXES.md. (A first scan used |x| > 1e15 as the garbage test and under-counted `limirrmisc` because some garbage lies below that: the test is now "outside [-1, 2]".)
- Consumer check (fresh process): 7 commits; groups open in 0.4-1.5 s; 396 monthly steps 1993-01-16..2025-12-16; tos t=200 raw NaN 26% / regrid 64% (regrid box larger than the curvilinear domain); thetao/so/volcello/uo/vo in plausible ranges; ice arrays with the -1e34 fill read as NaN (b_x/b_y ~27%) with no value near 1e34; the four flagged arrays show the attribute in both grids and no other array does; geolon finite; wet {0,1}; deptho max 6500 m.

## Volume ingested (computed 2026-10-26 from the parse caches: sums of the chunk-manifest lengths = bytes the references point at; native NEP 3-D row added 2026-10-27 from the repo's chunk storage, 4.15 TB)
| Repo | Referenced (compressed, GB) | Uncompressed (TB) | Chunk refs (M) |
|---|---|---|---|
| NWA decadal forecast monthly | 2,818 | 6.89 | 0.57 |
| PCI hindcast monthly | 2,002 | 2.85 | 3.29 |
| NEP hindcast monthly | 1,057 | 3.44 | 0.12 |
| NWA multi-decadal outlook (excl. native T_adx SSP585) | 309 | 0.86 | 0.07 |
| NWA hindcast daily | 265 | 0.65 | 0.05 |
| NWA seasonal reforecast | 233 | 0.60 | 0.10 |
| PCI hindcast daily | 433 | 0.64 | 0.54 |
| NEP hindcast daily (2-D subset) | 157 | 0.43 | 0.08 |
| NWA seasonal forecast (excl. native init 202510) | 27 | 0.07 | 0.00 |
| NWA decadal forecast yearly (no cache; from the scan tables: 37 + 38 GB on disk, 96 + 96 GB uncompressed) | ~75 | ~0.19 | small |
| NWA hindcast monthly | 2,549 | 7.93 | 0.13 |
| **Total, virtual references (11 repos)** | **~9.9 TB** | **~24.5 TB** | **~4.95 M** |
| NEP hindcast daily per-year 3-D variables (NATIVE copy, added 2026-10-27; 20 arrays: raw 11 + regrid 9; stored bytes, not references) | 4,150 | ~14.0 | ~0.48 (upper bound; all-fill chunks are not stored) |
| **Grand total (virtual referenced + native stored)** | **~14.1 TB** | **~38.5 TB** | **~5.4 M** |
- Native (non-virtual) data stored in the repos: the NEP daily per-year 3-D variables (4.15 TB stored, ~14.0 TB uncompressed; section 6b) + seasonal forecast init 202510 (36 arrays x 2 grids, ~20 GB uncompressed) + multi-decadal T_adx SSP585 (17.7 GB uncompressed); small statics/time axes. Only the NEP 3-D copy is large; storage cost applies to it (the virtual repos store only manifests).
- Not ingested: bundled `volcello` copies, the `vollcello` duplicate series (NEP daily, identical to `volcello`), empty/unreadable files (six NWA hindcast monthly arrays, the decadal-monthly `nsmz_100` 1996-01 file), older releases (the bucket holds 36.95 TB in 26,707 objects). The NEP daily per-year 3-D variables are NOT excluded any more: ingested natively on 2026-10-27 (section 6b).

## 11. nwa hindcast monthly (r20250715) -> `noaa-cefi/cefi-nwa-hindcast-monthly` (DONE 2026-10-26)
- Script: `ingest/ingest_nwa_hindcast_monthly.py` (`--parse` Coiled + resumable cache, `--dry-run`, `--write`, `--validate`). 952 files listed (raw 488, regrid 464). Parse: 952 files on 24 spot workers in 21 min; 946 parsed, 6 failed (see below). Groups (planned): `raw/main` 426, `raw/u_grid` 9, `raw/v_grid` 9, `raw/ice` 37 (+ CELL_AREA), `raw/ice_xB` 3, `raw/ice_yB` 3, `regrid/main` 422 (lon grid A), `regrid/ice` 37 (lon grid B, P5). 62 bundled `volcello` copies dropped (the standalone raw `volcello` is present).
- **FINDING 1 (statics, user decisions 2026-10-26): the hindcast's own `ocean_static.nc` has a land/sea mask that does not match the hindcast's data.** The new hard mask check stopped the dry run: tos day 1 has 391,722 cells with data, the static's `wet == 1` on 391,434 (288 cells where data exists but `wet`/`deptho` say land; none the other way). The NWA decadal-yearly r20250819 static matches the data exactly (391,722; 0 mismatches), as do the seasonal repos' own statics. Checked 2026-10-26 on the written repos (first-month data cells vs `wet == 1`): `cefi-nwa-seasonal-forecast-monthly` 391,722 vs 391,722, `cefi-nwa-seasonal-reforecast-monthly` 391,722 vs 391,722, `cefi-nwa-decadal-forecast-yearly` 391,722 vs 391,722 (decadal monthly and the multi-decadal outlook use the same decadal mask; the multi-decadal check is a hard assertion), and **`cefi-nwa-hindcast-daily` 391,722 vs 391,434: it carries the mismatching static** (it was written before the mask check existed). Questions put to the user and answered: (a) monthly statics: hybrid like the other NWA repos (coordinates from this release's gap-free static; `deptho`/`areacello`/`wet` from the decadal-yearly static; dry run now passes with 0 mismatches); (b) fix `cefi-nwa-hindcast-daily` in place (small native commit replacing raw `deptho`/`areacello`/`wet`; old commit stays in history): APPROVED and DONE 2026-10-26, commit `4FHQ979N4KNE8K3QEJ80` (see "Daily statics fix" below). Rule written up in INGESTION_REQUIREMENTS.md.
- **FINDING 2 (six source arrays with an empty chunk index):** the six failed parses (`MemoryError: Unable to allocate 188 GiB ... StringDType`, VirtualiZarr trying to build a dense manifest for an array with no chunks, as for the decadal-monthly `nsmz_100` file) are raw `vmo_rotate` and regrid `dissic`, `talk`, `volcello`, `psm`, `q_p_2_n_Sm`. Verified with h5py: each file holds only that one variable, 0 of 480 chunks written, HDF5 `get_storage_size() == 0`, a data read (t=200, z 0-9, 100 x 100) is 100% NaN, yet the files are 2.9-14.5 GB (regrid `thetao`, written, is 16 GB). So the bytes may exist but HDF5 cannot find them (interrupted/unflushed write). EXCLUDED (claude's call, reversible, reported to the user; same treatment as the empty PCI files; suggested checks, and writing to a transactional store such as Icechunk as an option to consider, are in SOURCE-FIXES.md): listed in the repo tag `excluded`, in SOURCE-FIXES.md and INGESTION_REQUIREMENTS.md. Effect: regrid has no `volcello`, `dissic` or `talk`.
- Other build checks already passed: every shared axis/coordinate identical across files (time unit spelling differs between files, compared by base date + values); one 'no data' sentinel per array (raw 1e20 for 409 arrays + NaN for 17, ice arrays -1e34, regrid NaN); every chunk written; no out-of-range values outside the fill (the strict check passes for every array); data xT/yT numerically equal to yh/xh, so `geolon`/`geolat` are copied into `raw/ice` as planned; the regrid lon axes differ between `regrid/main` and `regrid/ice` as expected (documented in the `regrid/ice` group attrs).
- Next steps were: `--write`, the daily statics fix, consumer check: all DONE (below).

### Daily statics fix (user-approved 2026-10-26, DONE: commit `4FHQ979N4KNE8K3QEJ80`)
Replace raw `deptho`, `areacello`, `wet` of `cefi-nwa-hindcast-daily` with the decadal-yearly-derived arrays (1e20 -> NaN), asserting 0 mismatches against the data mask first. One native commit on main; the virtual data is not touched. `areacello` becomes masked over land (consistent with the other NWA repos; it was defined everywhere before). Done with `ingest/fix_nwa_hindcast_daily_statics.py` (compare-only by default, `--apply` to commit): it first asserted that the decadal static reproduces the repo's data mask exactly (data cells 391,722; before: `wet == 1` on 391,434 with 288 data-but-not-wet cells; after: 391,722 on both, 0 mismatches), wrote the three native arrays (attrs `comment` records the source) and read the committed repo back (all three fields equal the new arrays; `wet == 1` equals the data mask on every cell). The verification of the other NWA repos is in FINDING 1 above.

### Result (nwa hindcast monthly, DONE 2026-10-26)
- Commits on `main`: raw/main `ATF29Y1201878WNN5PY0` (426 arrays, 69 4-D), raw/u_grid `RQX18CG1ZTCDGFKZ8E8G` (9), raw/v_grid `KC5X7ZYZ0TYPMKMR962G` (9), raw/ice `C5W6YV3ARJX7ZBQZRNN0` (37 + CELL_AREA + geolon/geolat), raw/ice_xB `6Q2GZAVTMS9T35624R2G` (3), raw/ice_yB `0NN05EB7J6DF7VZCNXY0` (3), regrid/main `C11SD2WTKXA6C153CKTG` (422), regrid/ice `C13PAF6CM6PMTRCMBM70` (37). Tags set, including `excluded` (the six arrays). 946 arrays in total.
- Compute (Coiled, `earthmover-devs`, 24 spot workers x 2 vCPU, `caffeinate -i`): parse 21 min (952 files, 6 failed = the six empty-chunk-index files, later excluded); write ~3 min; validation 16.5 min wall (42,572 source chunk windows over 946 arrays, 1,892 batches in one round == committed store).
- Statics (hybrid, see FINDING 1): geolon/geolat/geolon_c/geolat_c from this release's gap-free static; deptho/areacello/wet from the decadal-yearly r20250819 static; the mask is a hard assertion and reproduces the data exactly (391,722 cells, 0 mismatches both ways, verified again in the consumer check). `raw/ice`: data xT/yT equal yh/xh numerically, so geolon/geolat were copied (as the plan allowed); CELL_AREA from `ice_static.nc` (228,416 cells masked -> NaN).
- Sentinels: raw 1e20 (409 arrays) or NaN (17), ice arrays -1e34, regrid NaN; every chunk written; no out-of-range values outside the fill in any array. The 18 `_rotate` arrays' differently spelled time unit and the 62 bundled `volcello` copies were handled as planned.
- Validation hiccup: the first validation died with `TimeoutError` after a 10-minute round limit: `distributed.wait(timeout=...)` RAISES on timeout instead of returning the unfinished futures, and ~2,000 batches on 4-D arrays take longer. The write had already committed. Fixed (the exception now falls back to per-future status; round limit 30 min); the rerun finished in 16.5 min. The other scripts never hit the limit.
- Consumer check (fresh process): 9 commits; groups open in 0.4-1.7 s; 372 monthly steps 1993-01-16..2023-12-16; tos t=200 raw NaN 40% / regrid 39%; `volcello`, `dissic`, `talk` present in raw/main and absent from regrid/main (excluded); `regrid/main` and `regrid/ice` lon axes differ (-98.4423 step 0.0807 vs -98.0 step 0.0801; lat 5.2725..58.1608 in both) and the `regrid/ice` group comment says so; ice arrays with -1e34 fill read as NaN; no value > 1e15 anywhere sampled.

## FINAL STATE (2026-10-26): all 11 repos in `noaa-cefi` are written and validated
| Repo | Notes |
|---|---|
| cefi-nwa-decadal-forecast-yearly | FMRC; 61 inits; hybrid statics |
| cefi-nwa-decadal-forecast-monthly | FMRC; 61 inits x 120 months; hybrid statics; one regrid chunk-empty variable-init rebuilt as an empty manifest |
| cefi-nwa-seasonal-forecast-monthly | FMRC; transpose codec; init 202510 native; `_anom` kept |
| cefi-nwa-seasonal-reforecast-monthly | FMRC; 120 inits; fill unified to NaN (verified lossless) |
| cefi-nwa-hindcast-daily | statics replaced in place (commit `4FHQ979N4KNE8K3QEJ80`); raw fill 1e20 |
| cefi-nwa-multi-decadal-outlook-yearly | scenario stack; T_adx SSP585 native (2045 no-data); hybrid statics |
| cefi-nwa-hindcast-monthly | 8 groups; 6 unreadable arrays excluded; hybrid statics |
| cefi-nep-hindcast-daily | 16 2-D variables per grid (virtual) + per-year 3-D variables as NATIVE chunks (raw 11, regrid 9; added 2026-10-27, see section 6b) |
| cefi-nep-hindcast-monthly | 6 groups; 8 arrays with garbage values flagged |
| cefi-pci-hindcast-daily | 3 empty-free groups + flat regrid; fill 1e20 |
| cefi-pci-hindcast-monthly | 3 empty files + 66 bundled copies excluded |
- Volume referenced by all 11 repos: about 9.9 TB compressed (7.38 TB from the 10 repos in the table above + NWA hindcast monthly 2.55 TB), about 24.5 TB uncompressed (16.6 + 7.93), about 4.95 M virtual chunk references; tens of GB native.
- Option for NOAA (user note, 2026-10-26): corrupted/incomplete HDF5 writes (the six unreadable NWA hindcast-monthly arrays and the empty decadal-monthly `nsmz_100` file) would be avoided if they wrote directly to Icechunk instead (atomic commits); written up in SOURCE-FIXES.md ("Suggestions for NOAA/GFDL") as an option, with cheaper write-time checks listed first.
- Open items: (1) NEP daily per-year 3-D variables: DONE 2026-10-27 as NATIVE chunks (section 6b); a virtual version stays possible later (variable-length chunks or the untested (year, day-of-year) layout); (2) NWA hindcast monthly regrid lacks `volcello`/`dissic`/`talk`/`psm`/`q_p_2_n_Sm` and raw lacks `vmo_rotate` until NOAA regenerates those files; (3) the source issues for NOAA are in `SOURCE-FIXES.md`; (4) parse caches in `ingest/.cache/` (~3 GB) were DELETED 2026-10-27 (reruns re-parse, 5-25 min per product on Coiled; the NEP 3-D script needs `--probe` again, ~20 min, before `--write`/`--validate`).


## 6b. NEP hindcast daily: per-year 3-D variables ingested as NATIVE chunks (2026-10-27)
User decision 2026-10-27 (see INGESTION_REQUIREMENTS.md): ingest the variables excluded by P2 as native chunks, via Xarray on Coiled. Script `ingest/ingest_nep_hindcast_daily_3d_native.py` (`--probe`, `--plan`, `--create`, `--write [--only grid/var]`, `--validate [--only]`, `--fix-fill-attrs`).
- **Result:** `raw` 11 arrays (dissic no3 o2 po4 si so talk thetao uo vo volcello), `regrid` 9 (same minus uo, vo); shape (time 12053, z_l 52, y, x); chunks (30, 10, 200, 200) float32, Blosc zstd clevel 3 + shuffle; native `z_l`. `vollcello` not stored (identical to `volcello` in 462/462 sampled windows per grid). 20 arrays x ~175 B values = ~14 TB uncompressed; stored 4.15 TB (source files: 6.5 TB gzip incl. bundled copies).
- **Commits (main):** placeholders `K7D49FWCJ6MR01ECCHA0`; data one commit per array (message `native 3-D: <grid>/<var>`): regrid/so `D291VDDPKA24G8NHFR90`, raw dissic `CQHCWRTM6CA1S99873EG`, no3 `V4SR3TRBHWZMNJ5DEB4G`, o2 `0DQYDQ37HHM4WDA6KCQ0`, po4 `V7SXJWV12BQMVXE7TWTG`, si `NHRJHQ8XQSC161QJ7WZG`, so `MKYXA65HC80G970YKXSG`, talk `WQS9TYJ3NBVFM1G2SGJ0`, thetao `QB8CSNCDQQ9JRHRD29G0`, uo `S2BWY70GT2Z8B7X0E620`, vo `D7J3GN30GJ1Z8R2SVX2G`, volcello `7R081VDZ6WHVXGDW7AG0`; regrid dissic `G6GEP1W4V2H0D3NAJAQ0`, no3 `EZ85449EPW2GQ0NBWDTG`, o2 `WPXSY6ADZMC5HFE4B2V0`, po4 `2V34R8KY3CE7GWY75X7G`, si `X2M901QP1F0139B3GKCG`, talk `HTCE8SC2M6ZQARP0JDMG`, thetao `YDC89156Q31712SHZJJG`, volcello `6TDVCKE6WTCJ799CN8T0`. Attribute fix: `6TXN0KM4KG0FYH0CPTDG` (bad, left unreadable by xarray) then `G5BV7RJ50WCGG6M4J8DG` (repair). Description/metadata updated: `excluded` removed, `coverage` rewritten.
- **Checks passed:** probe of all 726 per-year files (20 min on 16 workers; 12 random source chunk windows hashed per file = ground truth); every file's time axis equals the repo's axis at its date offset; coordinates equal the repo's; one sentinel per array (raw 1e20, regrid NaN), no unwritten HDF5 chunks; raw `thetao` 1993-01-01 data mask == the repo's static `wet` (205,698 cells, 0 mismatches at the surface or any level); every element of every array classified at write time (valid or the sentinel, nothing else; counts equal the array size); 9,240 sampled source windows hashed from the committed store match bit for bit; consumer check (fresh process): surface `thetao` vs `tos` on 2010-06-15 12.29 vs 12.29 degC (raw), 9.263 vs 9.267 (regrid), no fill values leaking, `thetao` -1.79..27.7 degC.
- **Compute:** write = one task per 60 days x 10 levels (1,206 per array), forked Icechunk sessions merged and committed per array, spot workers 2 vCPU / 8 GB, one thread. With 120 workers: ~315 s per raw array, ~190 s per regrid array (the last 12 arrays took 47 min); with 40 workers ~13-18 min per raw array, ~8 min per regrid array. Tasks are read-bound (11 s to read 667 MB + 4.7 s to open the file, vs 1.7 s to classify and 1 s to write, measured on a laptop).
- **Findings / lessons:**
  1. FILL ATTRIBUTE (found by the consumer check, not by validation): xarray does NOT mask from a zarr v3 `fill_value` alone. The raw arrays came back with 1e20 as data (thetao mean 2.6e19) until `_FillValue` (base64 of the little-endian float64, the same encoding the virtual 2-D arrays carry) and `missing_value` were set as attributes. Bit-exact validation reads raw zarr values, so it could not see this; always open a new array with xarray and compare a mean before calling it done. A first attempt with a plain-number `_FillValue` made the whole raw group unopenable (`Failed to decode fill_value`) until repaired.
  2. Laptop sleep killed two runs (`scheduler-connection-lost`): lid closed on battery (`Clamshell Sleep` in `pmset -g log`), which `caffeinate -i` does not prevent. The run lost only the array in flight (one commit per array); nothing was half-written.
  3. Memory: 90-day tasks on 8 GB workers killed four workers on one task (raw thetao, first attempt); 60-day tasks with classification in 5-day slices run fine on 8 GB workers. `raw/thetao` itself was not special.
  4. Arraylake credentials in forked sessions cannot be refreshed on workers; a session handle older than ~1 h fails with `AuthException ... Not logged in`. The script now builds a fresh `Client().get_repo()` per array.
  5. One-thread workers on bigger VMs waste CPU (3 of 4 vCPUs idle); more small workers is the cheaper way to scale read-bound tasks.
- **For NOAA:** the P2 problem (time chunk 100 not dividing 365/366 in per-year files) disappears with a time chunk that divides the series or one file per variable; see `SOURCE-FIXES.md`.


## 6c. CF packing attributes check (2026-11-09)
Header-only scan of ALL 7,576 source NetCDF files of the 11 ingested releases (HDF5 attributes read for 60,971 datasets; the 10 netCDF3 static files read separately with scipy): NO dataset carries `scale_factor`, `add_offset`, `scale`, `offset`, `valid_range`, `valid_min` or `valid_max`, no group-level packing attributes, and none uses the HDF5 scale-offset filter. So the attributes are absent everywhere, which makes them trivially consistent within every dataset, and no unpacking is needed. Data variables are float32; the only integer arrays are small coordinates/flags (`member`, `init`, `lead`, `month`, `valid_time`, PCI `mask*` int8). Big-endian float32 appears only in some small coordinates (PCI monthly `time/xh/xq/yh/nv`, multidecadal `bnds`) and the netCDF3 statics; zarr/numpy handle the byte order. The committed repos show the same (no packing attributes on any of their 3,200+ arrays).
