# Dimension and coordinate names in the `noaa-cefi` repos

The repos keep the names that NOAA's NetCDF files use. They are **not** unified across domains: the NWA and Pacific Islands (hic6k)
data use one scheme, the NEP data another, and the sea-ice grids a third. This page says which names to expect where. A snippet at
the end shows how to line two domains up if you need to.

All repos are Icechunk repositories in the Arraylake org `noaa-cefi`. Open a group with xarray like this (`consolidated=False` is needed):

```python
import xarray as xr
from arraylake import Client

client = Client()
store = client.get_repo("noaa-cefi/nwa12-hindcast").readonly_session(branch="main").store
ds = xr.open_zarr(store, group="native/monthly", consolidated=False, zarr_format=3)
```

## The cell positions

MOM6 uses a staggered (Arakawa C) grid. A field lives at one of four positions, and each position has its own pair of dimensions:

| Position | Where | NWA, hic6k | NEP | 2-D coordinates |
|---|---|---|---|---|
| h | cell centre (tracers, most diagnostics) | `yh`, `xh` | `jh`, `ih` | `geolon`, `geolat` |
| q | cell corner | `yq`, `xq` | `jq`, `iq` | `geolon_c`, `geolat_c` |
| u | east/west cell face (zonal velocity) | `yh`, `xq` | `jh`, `iq` | `geolon_u`, `geolat_u` |
| v | north/south cell face (meridional velocity) | `yq`, `xh` | `jq`, `ih` | `geolon_v`, `geolat_v` |

All positions of a domain share one group (for example `native/monthly`); each variable's dimensions tell you its position. The h-position
1-D axes (`yh`, `xh` or `jh`, `ih`) are the dimension coordinates; `geolon`/`geolat` are the 2-D longitude and latitude and are listed in each
variable's `coordinates` attribute, so xarray attaches them. The u- and v-grid variables list `geolon_u`/`geolat_u` and `geolon_v`/`geolat_v`.

## What each repo uses

| Repo | Native horizontal dimensions | 2-D coordinates in the data groups | Other dimensions |
|---|---|---|---|
| `nwa12-hindcast` | `yh xh yq xq` | `geolon geolat` (+ `_u`, `_v` in `native/monthly`) | `time`, `z_l`, `zi`, `nv`; sea ice (below) |
| `nwa12-decadal-forecast` | `yh xh` (`yq xq` only as 1-D axes) | `geolon geolat` | `time`, `member`, `step`, `nv` |
| `nwa12-seasonal-forecast`, `nwa12-seasonal-reforecast` | `yh xh` | `geolon geolat` | `time`, `member`, `step` |
| `nwa12-multi-decadal-outlook` | `yh xh yq xq` | `geolon geolat geolon_u geolat_u geolon_v geolat_v` | `time`, `scenario`, `z_l`, `bnds` |
| `hic6k-hindcast` | `yh xh yq xq` | `geolon geolat` + `_u`, `_v` | `time`, `z_l`, `zi`, `nv` |
| `nep10k-hindcast` | `jh ih jq iq` | `geolon geolat` + `_u`, `_v` | `time`, `z_l`, `zi`, `nv`, `ct`, `xB`, `yB` |

Regridded groups (`regrid/...`) use `lat` and `lon` in every repo, with the same other dimensions as the native groups (`z_l`, `zi`, `time`,
`member`, `step`, `scenario`, `ct`).

## Conventions that are the same everywhere

- `time`: the model time. In the forecast repos (`nwa12-decadal-forecast`, the two seasonal repos) `time` is the **initialization** time, `member`
  the ensemble member, `step` the integer lead in months (or years in `nwa12-decadal-forecast/yearly`), and `valid_time` a 2-D array
  (`time`, `step`) with the time each value is valid for.
- `z_l` are layer centres, `zi` layer interfaces.
- `nv` is the length-2 dimension of time-bounds variables (`average_T1`, `average_T2`, `average_DT`). `nwa12-multi-decadal-outlook` calls it
  `bnds`.
- `scenario` (SSP126, SSP245, SSP370, SSP585) exists only in `nwa12-multi-decadal-outlook`.

## Where the names differ

- **NEP versus the rest:** the NEP uses index-style names (`jh`, `ih`, `jq`, `iq`), the others use `yh`, `xh`, `yq`, `xq`. The same position
  has the same meaning in both, as in the table above.
- **Sea ice, NWA hindcast monthly** (`nwa12-hindcast`, `native/monthly/ice`): the ice variables are on the sea-ice model's own dimensions, not
  the ocean's: `yT`, `xT` for the tracer points (numerically equal to `yh`, `xh`), `xB` and `yB` for the B-grid corner points, `ct` for the
  ice thickness categories. They are a separate group because those names would otherwise clash with the ocean's (the ice `geolon`/`geolat` have
  different dimensions than the ocean's, and the regridded ice has its own `lon` axis that differs from the ocean's). The ice `geolon` and `geolat`
  are present there.
- **Sea ice, NEP:** the ice variables share the ocean dimensions (`jh`, `ih`); the `b_x`/`b_y` B-grid variables add `xB` and `yB`, in the same
  group as the ocean variables.
- **Pacific Islands q axis:** the `yq` axis has 539 points (the same as `yh`) and `xq` has 726, so q-position and u-position fields have the
  same shape here, and v-position fields have the same shape as h-position fields. Use the dimension names, not the shape, to tell positions apart.
- **Only some groups hold the `_u`/`_v` coordinates:** `geolon_u`, `geolat_u`, `geolon_v` and `geolat_v` are added to a group only where it has
  u- or v-grid variables. They are always in `native/static`. In the NEP, they are NaN on land (NOAA's static file has no gap-free version);
  `geolon`, `geolat`, `geolon_c` and `geolat_c` have no gaps.

## Static fields

Every repo has `native/static`, the full static set of the domain, stored once for all frequencies. It has the four horizontal axes of the
domain and these 25 fields on the positions above: `areacello`, `areacello_bu`, `areacello_cu`, `areacello_cv`, `deptho`, `dxt`, `dyt`, `dxCu`,
`dyCu`, `dxCv`, `dyCv`, `Coriolis`, `sftof`, `wet`, `wet_c`, `wet_u`, `wet_v` and the eight `geolon*`/`geolat*` arrays. Land is NaN. Differences:

- `hic6k-hindcast` also has the integer masks `mask`, `mask_u`, `mask_v`.
- `nep10k-hindcast` also has the extra axes `nv`, `z_l`, `zi`, `zl` (75 levels) and `z_i` (53), and the ice fields `CELL_AREA`, `COSROT`,
  `SINROT` on the ocean's `jh`, `ih`.
- `nwa12-hindcast` has `native/static/ice` with `CELL_AREA`, `COSROT`, `SINROT`, `GEOLON` and `GEOLAT` on `yT`, `xT` (plus the axes `xTe`,
  `yTe`); `GEOLON` and `GEOLAT` are NaN on land there.
- There is no `regrid/static`: NOAA ships no static file for the regridded grid, and the regridded `lat` and `lon` are in the `regrid` groups.

Each static group has an `applies_to` attribute listing the data groups it belongs to, and each data group has a `static_group` attribute
pointing back. Static fields on the same dimensions as a data group can be merged in directly:

```python
static = xr.open_zarr(store, group="native/static", consolidated=False, zarr_format=3)
tos = xr.merge([ds[["tos"]].isel(time=0), static[["areacello", "wet"]]], compat="no_conflicts")
```

## Working across domains

To line the NEP up with the NWA names, rename the four dimensions (only those that the dataset has):

```python
ren = {"jh": "yh", "ih": "xh", "jq": "yq", "iq": "xq"}
nep = xr.open_zarr(client.get_repo("noaa-cefi/nep10k-hindcast").readonly_session(branch="main").store,
                   group="native/monthly", consolidated=False, zarr_format=3)
nep = nep.rename({k: v for k, v in ren.items() if k in nep.variables or k in nep.dims})
```

(The two snippets above were run against the repos before this page was written.) The NEP grid and the NWA grid are different grids, so
renaming makes the names match, not the data comparable cell by cell.

## History

The names were left as NOAA published them on purpose: unifying them was judged not worth the effort for the existing simulations, and the
task was to document the differences (see `REVISIONS.md`, task A4/A6). The one thing that was changed is the placement of the static fields and
the addition of the missing `geolon_u`/`geolat_u`/`geolon_v`/`geolat_v` coordinates (`LOG.md`).
