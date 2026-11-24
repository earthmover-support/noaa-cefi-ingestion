# Revisions requested by NOAA after the initial ingestion

Source: NOAA review notes (task tracker + Kelly's comments), received 2026-11-24. Nothing here is started.

## A. Reorganize groups and datasets (tracker: Tom Nicholas, "Reformat arrays/groups")

Kelly's proposal. Today the temporal grid (daily/monthly/yearly) is split at the repo level while the spatial grid is at the
group level; she wants both at the group level and the repo to be the simulation.

- [ ] A1. Move the temporal grouping (daily / monthly / yearly) from the repo level into groups, so one repo holds a
      whole simulation (model domain + experiment; 11 repos become 7, see Decisions).
- [ ] A2. Put the spatial grouping above the temporal one (users mostly stay in one spatial grid and move across
      temporal archives). Decided: all cell-position grids (h / q / u / v: the yh xh, yq xq, jh iq, yT xB ... dims) share
      ONE group per side, as xGCM does, so no h/q/u/v subgroups. Target layout:

      simulation
        native
          static
          daily / monthly / yearly
        regrid
          static
          daily / monthly / yearly

      (Kelly's fallback with h/q/u/v subgroups is not needed. Her tree omits `regrid/yearly`; decided that was an
      oversight, so `regrid` gets `yearly` too.)
- [ ] A3. Standardize group names across all simulations, whatever coordinate naming convention (yh/xh, jh/ih, yT/xT)
      each one uses. Today they differ, e.g. `main`, `u_grid`, `v_grid`, `ice`, `ice_xB`, `ice_yB`, `b_x`, `b_y`.
- [ ] A4. Coordinate variable names stay as they are in existing simulations (explicitly not worth undoing); explain the
      differences in the supporting documentation (see A6).
- [ ] A5. Fix `geolat`/`geolon` coordinates: add the missing `geolat_u/v` and `geolon_u/v`, and mark `geolat_c`/`geolon_c`
      as coordinate variables. Check every group, not just the ones she noticed.
- [ ] A6. Write supporting documentation covering the coordinate naming differences between simulations (and the final
      layout).

## B. Static variables

Kelly: only a small subset of `ocean_static` / `ice_static` made it into the repos (her NEP hindcast list has about 28
names, e.g. `Coriolis`, `areacello_bu/cu/cv`, `dxCu/dxCv/dxt`, `dyCu/dyCv/dyt`, `geolat/geolon` `_c/_u/_v`, `sftof`,
`wet_c/_u/_v`, `CELL_AREA`, `COSROT`, `SINROT`). Her question: not processed, or missing from NODD?

- [x] B1. Verify the static variables are on NODD (Roland, NOAA affiliate). ANSWERED (Tom): all of the
      variables on Kelly's list are in the `ocean_static` and `ice_static` files in the AWS bucket (NEP hindcast list). So
      the gap is on our side (we did not ingest them), not missing data on NODD.
- [x] B2. Same verification (Kevin, NOAA affiliate). Covered by B1.
- [ ] B3. Add the full static set to every repo (see B4 for where it goes). No longer blocked on NODD for the NEP
      statics; check the NWA and PCI static files for the same variable set before relying on it.
- [x] B4. DECIDED: every repo carries the full static set, and within a repo each static variable is stored only once (no
      copies per daily / monthly / yearly group). Layout: a `static` group under `native` and one under `regrid`
      (`native/static`, `regrid/static`), since the native and regridded grids have different statics. The `[static?]`
      markers in A2 become `static`.
- [ ] B5. Our side: list which static variables each repo currently has, to make the comparison with NOAA's list. (Her
      bold marking of the present ones was lost in the paste.)

## C. Model domain names

- [ ] C1. Name the repos `<domain>-<experiment>` (no `cefi-` prefix; see Decisions) and rename to the published model names: `nep` -> `nep10k`, `nwa` -> `nwa12` (no "k", intentionally),
      `pci` -> `hic6k`. Applies to repo names, repo descriptions and tags, scripts, the notes and the README.
- [ ] C2. Work out how to rename: the Arraylake repos may not be renameable in place, so this may mean creating new repos
      and retiring the old names. Combine with the A reorganization so repos are rebuilt once.

## D. Missing datasets

- [ ] D1. NEP10k seasonal forecast. Check first whether it exists on NODD (we have only the NWA seasonal products).
- [ ] D2. NEP10k seasonal reforecast. Same check.
- [ ] D3. DEFERRED (decided): older releases are out of scope for now. We ingested only the latest release of each product.
      Revisit later; it needs a decision on how releases are represented (separate repos, branches or tags, or groups).

## E. Not an ingestion task

- [ ] E1. Arraylake web app: let users set A-Z as the default sort; the main page resets to sorting by "recent" every
      time. Kelly raises it and seconds Roland's comment. Pass to the Arraylake web team.

## Decisions so far

- Spatial grids: one group per side (native / regrid), like xGCM (A2).
- `regrid` gets `yearly` as well; Kelly's omission is treated as an oversight (A2).
- Older releases: ignored for now (D3).
- Repo granularity: one repo per model domain + experiment, with daily / monthly / yearly as groups inside; 11 repos become
  7: `nwa12` hindcast, decadal forecast, seasonal forecast, seasonal reforecast, multi-decadal outlook; `hic6k` hindcast;
  `nep10k` hindcast. Forecast products keep their init / member / step layout (A1).
- Repo names: `<domain>-<experiment>` with no `cefi-` prefix (the org is already `noaa-cefi`): `nwa12-hindcast`,
  `nwa12-decadal-forecast`, `nwa12-seasonal-forecast`, `nwa12-seasonal-reforecast`, `nwa12-multi-decadal-outlook`,
  `hic6k-hindcast`, `nep10k-hindcast` (C1).
- Statics: all of them in every repo, stored once per repo in `native/static` and `regrid/static` (B4).
  DataTree inheritance of the statics by `daily` / `monthly` / `yearly` is NOT required (Tom, 2026-11-24). What matters: the
  static data is in the repo, and the group structure makes clear which datasets it applies to. So keep `native/static` and
  `regrid/static` as sibling groups, and give each a group attribute naming the groups it applies to.

## Still open

Nothing outstanding. (Repo granularity was settled by Tom: see Decisions.)

## Implementation notes

Reconciled with `RESTRUCTURING.md` / `utilities.py` (commit `1a20fd8`), which supersede the earlier assumption here that merging
repos means re-ingesting.

- Most of the reorganization needs no re-ingest and no NOAA file reads: `rename_variable`, `set_coords` / `reset_coords`,
  `move_node` (arrays and groups) and `copy_node` only rewrite Zarr metadata and chunk references. Virtual chunks stay virtual.
- Merging repos (A1) is `copy_node` from each source repo into the new repo (e.g. `/raw` -> `/native/daily`). Virtual chunks are copied
  as references; native chunks are copied byte for byte. The new repo starts with a fresh history.
- The heavy case is `cefi-nep-hindcast-daily`: its ~4.2 TB of native 3-D chunks would be downloaded and re-uploaded. Options to
  evaluate: run the copy from AWS us-east-1; or register the existing Icechunk repo under the new name with
  `Client.import_repo(name, bucket_config_nickname, prefix)` and restructure it in place (not tested; also unknown what deleting the
  old Arraylake record does to the underlying data).
- Arraylake has no rename operation for repos (`modify_repo` changes description and metadata only), so a new repo name means a new
  repo (create + copy, or import).
- The guide uses the org name `NOAA-CEFI`; our org is `noaa-cefi`. Check whether the case matters.
- `utilities.py` was tested on small local repos; our repos have up to ~1,000 arrays and 3.3 M chunk references (PCI monthly), so
  check speed and memory at that scale.
- Merging the `main` / `u_grid` / `v_grid` / `ice*` / `b_*` subgroups of a repo into one group per side (A2) is not covered by the
  guide: it needs a name-collision check first (e.g. `time`, `z_l` must be identical across subgroups; `geolon`/`geolat` exist
  on more than one grid).
- Keep the current repos until the new layout is validated, then retire them.
- The native NEP 3-D copy is in `cefi-nep-hindcast-daily`; it moves along with the rest of that repo.
