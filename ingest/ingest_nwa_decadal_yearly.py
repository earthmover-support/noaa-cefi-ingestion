#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "arraylake>=0.16",
#   "virtualizarr[hdf] @ git+https://github.com/zarr-developers/VirtualiZarr.git@de4e669f18e323b2814f0625785dbd5766d65652",
#   "icechunk>=1.2", "obstore>=0.5", "obspec-utils>=0.2", "xarray", "zarr>=3", "numpy", "pandas",
#   "h5py", "h5netcdf", "scipy",
# ]
# ///
"""NWA decadal forecast yearly (r20250819) -> `noaa-cefi/cefi-nwa-decadal-forecast-yearly`.

Virtual Icechunk repo, parsed directly from the source NetCDF4 files (not NOAA's Kerchunk JSONs).
One FMRC cube per grid:

    raw    (time: 61, member: 10, step: 10, yh: 845, xh: 775)   + statics from the release's ocean_static.nc
    regrid (time: 61, member: 10, step: 10, lat: 844, lon: 774)

  time        forecast_reference_time (init, Jan 1 of the init year)
  step        integer lead year 0..9 (forecast_period, units "years")
  valid_time  2-D (time, step): decoded source `lead` (mid-point of each lead year)

The source `lead` is float "days since <init>" and drifts with the leap-year cycle, hence the integer step.
Per-init time fields (average_T1/T2/DT, time_bnds) are decoded and written as native chunks; data arrays are
virtual references. Source files are never modified.

Usage:
    ./ingest_nwa_decadal_yearly.py --dry-run     # in-memory repo + validation, touches nothing remote
    ./ingest_nwa_decadal_yearly.py --write       # org setup + create repo + commit to main + validate
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import io
import sys
import time
from collections import defaultdict

import h5py
import icechunk as ic
import numpy as np
import obstore as obs
import pandas as pd
import virtualizarr as vz
import xarray as xr
import zarr
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser

ORG = "noaa-cefi"
REPO = f"{ORG}/cefi-nwa-decadal-forecast-yearly"
BUCKET = "noaa-oar-cefi-regional-mom6-pds"
SOURCE_URL = f"s3://{BUCKET}"
NICKNAME = BUCKET  # repo storage: bucket config with prefix `arraylake/`
SOURCE_NICKNAME = f"{BUCKET}-source"  # same bucket, NO prefix, anonymous: virtual chunk source (outside `arraylake/`)
REGION = "us-east-1"
RELEASE = "r20250819"
PREFIX = "northwest_atlantic/full_domain/decadal_forecast/yearly"
GRIDS = ("raw", "regrid")

PER_INIT_TIMES = ("average_T1", "average_T2", "time_bnds")
LOADABLE = ["lead", "member", "lat", "lon", "xh", "yh", "nv", "average_DT", *PER_INIT_TIMES]
TIME_ENCODING = {"units": "days since 1965-01-01 00:00:00", "calendar": "gregorian", "dtype": "float64"}
DROP_GROUP_ATTRS = ("coordinates", "title", "cefi_filename", "cefi_ori_filename",
                    "cefi_variable", "cefi_init_date", "associated_files")
LONLAT = {"geolon": "longitude", "geolat": "latitude"}
DEGREE_UNITS = {"degrees_east", "degree_east", "degrees_north", "degree_north"}
OPEN_WORKERS = 8


def source_store():
    return obs.store.S3Store(BUCKET, region=REGION, skip_signature=True)


def list_nc(store, grid: str) -> list[str]:
    pre = f"{PREFIX}/{grid}/{RELEASE}/"
    return sorted(o["path"] for b in store.list(prefix=pre) for o in b if o["path"].endswith(".nc"))


def parse_name(path: str) -> tuple[str, str] | None:
    """(variable, init 'YYYYMM') for per-init files; None for ocean_static.nc."""
    name = path.rsplit("/", 1)[1]
    if name.startswith("ocean_static"):
        return None
    return name.split(".")[0], name.rsplit(".i", 1)[1].removesuffix(".nc")


def open_var(path: str, reg) -> xr.Dataset:
    return vz.open_virtual_dataset(f"{SOURCE_URL}/{path}", registry=reg, parser=HDFParser(),
                                   loadable_variables=LOADABLE, decode_times=False)


def to_fmrc(ds: xr.Dataset, init: str) -> xr.Dataset:
    t0 = pd.Timestamp(f"{init[:4]}-{init[4:]}-01")
    assert ds["lead"].attrs["units"] == f"days since {t0:%Y-%m-%d}", (init, ds["lead"].attrs)

    def decode(days) -> np.ndarray:
        days = np.asarray(days, "float64")
        return (t0 + pd.to_timedelta(days.ravel(), unit="D")).values.reshape(days.shape)

    years = (np.asarray(ds["lead"].values) // 365).astype("int64")
    assert (years == np.arange(ds.sizes["lead"])).all(), (init, years)
    valid = decode(ds["lead"].values)
    out = ds.drop_vars("lead").rename_dims(lead="step")
    for name in PER_INIT_TIMES:
        if name in out:
            out[name] = (out[name].dims, decode(out[name].values),
                         {k: v for k, v in out[name].attrs.items()
                          if k not in ("units", "calendar", "calendar_type")})
    out = out.assign_coords(
        step=("step", np.arange(out.sizes["step"]), {
            "standard_name": "forecast_period", "long_name": "forecast lead year", "units": "years",
            "comment": "0 = first year after initialization; the value is the annual mean over that "
                       "year (see valid_time for its mid-point)"}),
        valid_time=("step", valid, {"standard_name": "time",
                                    "long_name": "valid time (mid-point of lead year)"}),
    )
    out = out.expand_dims(time=[t0])
    out["valid_time"] = out["valid_time"].expand_dims(time=[t0])
    return out.set_coords("valid_time")


HINDCAST_STATIC = "northwest_atlantic/full_domain/hindcast/monthly/raw/r20250715/ocean_static.nc"
COORD_STATICS = ("geolon", "geolat", "geolon_c", "geolat_c")  # from the hindcast static (this release's are masked)
OWN_STATICS = ("deptho", "areacello", "wet")  # this release's own (its land/sea mask differs from the hindcast at 288 cells)


def load_statics(store, reg) -> xr.Dataset:
    """Hybrid statics (user decision 2026-10-22): gap-free coordinates from the NWA hindcast monthly
    ocean_static (netCDF3, read via scipy), asserted equal to this release's wherever it is unmasked;
    deptho/areacello/wet from this release's own ocean_static.nc."""
    from scipy.io import netcdf_file

    want = [*COORD_STATICS, *OWN_STATICS]
    own = vz.open_virtual_dataset(
        f"{SOURCE_URL}/{PREFIX}/raw/{RELEASE}/ocean_static.nc", registry=reg, parser=HDFParser(),
        loadable_variables=[*want, "xh", "yh", "xq", "yq"], decode_times=False)[want].load()
    hc = netcdf_file(io.BytesIO(bytes(obs.get(store, HINDCAST_STATIC).bytes())), mmap=False)
    out = own[list(OWN_STATICS)].assign_coords(xq=own.xq, yq=own.yq)
    for n in COORD_STATICS:
        v = hc.variables[n]
        h = np.array(v[:], dtype="float32")
        assert np.isfinite(h).all() and (np.abs(h) < 1e19).all(), f"hindcast {n} has gaps"
        o = own[n].values
        valid = np.abs(o) < 1e19
        assert o.shape == h.shape and np.array_equal(o[valid], h[valid]), f"{n}: hindcast != release where valid"
        attrs = {k: (getattr(v, k).decode() if isinstance(getattr(v, k), bytes) else getattr(v, k))
                 for k in ("long_name", "units") if hasattr(v, k)}
        attrs["comment"] = ("taken from the NWA hindcast monthly r20250715 ocean_static.nc: this release's own "
                            "ocean_static.nc masks land with 1e20; identical wherever that file is unmasked")
        out[n] = (own[n].dims, h, attrs)
    for d in ("xh", "yh", "xq", "yq"):
        assert np.array_equal(np.asarray(hc.variables[d][:], "float64"), own[d].values), d
    return out


def build_grid(grid: str, reg, store) -> tuple[xr.Dataset, xr.Dataset | None]:
    by_init: dict[str, dict[str, str]] = defaultdict(dict)
    static_path = None
    for p in list_nc(store, grid):
        info = parse_name(p)
        if info is None:
            static_path = p
        else:
            by_init[info[1]][info[0]] = p
    var_sets = {tuple(sorted(v)) for v in by_init.values()}
    assert len(var_sets) == 1, f"[{grid}] inits have different variable sets: {var_sets}"
    print(f"  [{grid}] {len(by_init)} inits x {len(next(iter(var_sets)))} vars "
          f"({'with' if static_path else 'no'} static file)", flush=True)

    statics = load_statics(store, reg) if grid == "raw" else None  # fail fast, before the slow open loop
    jobs = [(init, var, p) for init, vp in by_init.items() for var, p in vp.items()]
    opened: dict[str, list[xr.Dataset]] = defaultdict(list)
    t = time.time()
    with cf.ThreadPoolExecutor(max_workers=OPEN_WORKERS) as pool:
        futs = {pool.submit(open_var, p, reg): (init, var) for init, var, p in jobs}
        for n, fut in enumerate(cf.as_completed(futs), 1):
            opened[futs[fut][0]].append(fut.result())  # any failure is fatal
            if n % 100 == 0 or n == len(futs):
                print(f"  [{grid}] opened {n}/{len(futs)} ({time.time() - t:.0f}s)", flush=True)

    slices = []
    for init in sorted(opened):
        merged = xr.merge(opened[init], compat="override", join="exact", combine_attrs="drop_conflicts")
        slices.append(to_fmrc(merged, init))
    cube = xr.concat(slices, dim="time", coords="minimal", compat="override",
                     combine_attrs="drop_conflicts", join="exact")
    cube.attrs = {k: v for k, v in cube.attrs.items() if k not in DROP_GROUP_ATTRS}
    cube.attrs["cefi_init_date"] = "i" + ",i".join(sorted(opened))
    cube.attrs["featureType"] = "forecast_model_run_collection"
    cube["time"].attrs = {"standard_name": "forecast_reference_time",
                          "long_name": "forecast initialization time"}
    cube["member"].attrs.setdefault("long_name", "ensemble member")
    for name in ("time", "valid_time", *PER_INIT_TIMES):
        if name in cube.variables:
            cube[name].encoding = dict(TIME_ENCODING)
    if grid == "raw":
        for d in ("yh", "xh"):  # statics share xh/yh with the data
            assert np.array_equal(statics[d].values, cube[d].values), f"static {d} != data {d}"
        cube = cube.assign(statics)
    print(f"  [{grid}] cube {dict(cube.sizes)}", flush=True)
    return cube, statics


def patch_group_metadata(store, group: str) -> None:
    """CF/tiles metadata on one group (same rules as the NOAA-PMEL pilot)."""
    g = zarr.open_group(store, mode="r+", path=group)
    arrays = dict(g.arrays())
    dims_of = lambda a: tuple(a.metadata.dimension_names or ())  # noqa: E731
    if "geolon" in arrays and "geolat" in arrays:
        geo = set(dims_of(arrays["geolon"]))
        for name, std in LONLAT.items():
            arrays[name].attrs["standard_name"] = std
        for name, a in arrays.items():
            if name not in LONLAT and geo <= set(dims_of(a)):
                a.attrs["coordinates"] = "geolon geolat"
        for name, a in arrays.items():  # demote nominal 1-D degree coords so geolon/geolat win
            if name not in LONLAT and len(dims_of(a)) == 1 and a.attrs.get("units") in DEGREE_UNITS:
                a.attrs["nominal_units"] = a.attrs.pop("units")
    else:
        coord_names = {n for n, a in arrays.items() if len(dims_of(a)) == 1 and dims_of(a)[0] == n}
        for name, a in arrays.items():
            cs = [d for d in dims_of(a) if d in coord_names]
            if name not in coord_names and cs:
                a.attrs["coordinates"] = " ".join(cs)
    if "coordinates" in dict(g.attrs):
        del g.attrs["coordinates"]
    # valid_time is a (time, step) coordinate of every time/step data variable
    for _, a in g.arrays():
        dims = a.metadata.dimension_names or ()
        coords = a.attrs.get("coordinates")
        if "time" in dims and "step" in dims and coords and "valid_time" not in coords.split():
            a.attrs["coordinates"] = f"{coords} valid_time"


def write(store, cubes: dict[str, xr.Dataset]) -> None:
    for grid, cube in cubes.items():
        accessor = getattr(cube, "vz", None) or cube.virtualize
        accessor.to_icechunk(store, group=grid)
        patch_group_metadata(store, grid)


class _Reader(io.RawIOBase):
    """h5py-compatible file object over an obstore reader (ground truth straight from the source file)."""
    def __init__(self, r): self.r = r
    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.r.tell()
    def seek(self, o, w=0): return self.r.seek(o, w)
    def readinto(self, b):
        d = bytes(self.r.read(len(b)))
        b[:len(d)] = d
        return len(d)


def validate(store, src, cubes: dict[str, xr.Dataset], full_inits=("196501", "196801", "202501")) -> None:
    """Compare the written store against the source NetCDFs read independently with h5py."""
    for grid, cube in cubes.items():
        new = xr.open_zarr(store, group=grid, consolidated=False, zarr_format=3,
                           decode_times=False, mask_and_scale=False)  # compare stored values, as h5py returns them
        print(f"\n--- {grid}\n{new}", flush=True)
        times = [f"{t:%Y%m}" for t in pd.DatetimeIndex(cube.time.values)]
        by_init: dict[str, dict[str, str]] = defaultdict(dict)
        for p in list_nc(src, grid):
            info = parse_name(p)
            if info:
                by_init[info[1]][info[0]] = p
        data_vars = sorted(next(iter(by_init.values())))
        spatial = [d for d in new[data_vars[0]].dims if d not in ("time", "member", "step")]
        mid = [new.sizes[d] // 2 for d in spatial]

        def check(k: int, init: str) -> int:
            n = 0
            for v in data_vars:
                for attempt in range(4):
                    try:
                        f = h5py.File(_Reader(obs.open_reader(src, by_init[init][v], buffer_size=256 * 1024)), "r")
                        if init in full_inits:
                            sl = (slice(None), slice(None), slice(mid[0] - 50, mid[0] + 50))
                            a = new[v].isel(time=k, **{spatial[0]: sl[2]}).values
                            b = f[v][sl]
                        else:
                            a = new[v].isel(time=k, member=0, step=0, **{spatial[0]: mid[0], spatial[1]: mid[1]}).values
                            b = f[v][0, 0, mid[0], mid[1]]
                        assert a.shape == b.shape and np.array_equal(a, b, equal_nan=True), (grid, init, v)
                        n += 1
                        break
                    except (ic.IcechunkError, OSError) as exc:
                        if attempt == 3:
                            raise
                        print(f"  [{grid}] i{init}/{v}: transient read error, retry ({type(exc).__name__})", flush=True)
            return n

        total = 0
        with cf.ThreadPoolExecutor(max_workers=4) as pool:
            for fut in [pool.submit(check, k, i) for k, i in enumerate(times)]:
                total += fut.result()
        # valid_time == decoded source lead for a few inits
        for k in (0, 30, 60):
            init = times[k]
            f = h5py.File(_Reader(obs.open_reader(src, by_init[init][data_vars[0]], buffer_size=256 * 1024)), "r")
            t0 = pd.Timestamp(f"{init[:4]}-{init[4:]}-01")
            exp = (t0 + pd.to_timedelta(f["lead"][:], unit="D")).values
            got = cube.valid_time.isel(time=k).values
            assert np.array_equal(exp, got), (init, exp, got)
        print(f"  [{grid}] OK: {total} (init, var) checks vs source file bytes "
              f"(full 100-row slabs for {list(full_inits)}); valid_time matches source lead", flush=True)


def ensure_org_setup(client, dry: bool) -> None:
    """Source bucket config (no prefix, anonymous) + VCAP. Arraylake requires VCAP URLs to sit inside a bucket
    config's prefix, and the source files are outside the repo-storage config's `arraylake/` prefix."""
    if SOURCE_NICKNAME not in {b.nickname for b in client.list_bucket_configs(org=ORG)}:
        client.create_bucket_config(org=ORG, nickname=SOURCE_NICKNAME, uri=SOURCE_URL,
                                    extra_config={"region_name": REGION}, auth_config={"method": "anonymous"})
        print(f"  [setup] created bucket config {SOURCE_NICKNAME!r}")
    vcaps = client.list_virtual_chunk_access_policies(ORG)
    if any(p.url_prefix.rstrip("/") == SOURCE_URL and p.subprefix == "" for p in vcaps):
        print("  [setup] VCAP exists")
    else:
        client.set_virtual_chunk_access_policy(ORG, bucket_nickname=SOURCE_NICKNAME, subprefix="", public=True)
        print("  [setup] set org VCAP (whole source bucket, public)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true", help="in-memory Icechunk repo + validation")
    g.add_argument("--write", action="store_true", help="create the Arraylake repo and commit to main")
    ap.add_argument("--grids", default=",".join(GRIDS))
    args = ap.parse_args()
    grids = [x for x in args.grids.split(",") if x]

    src = source_store()
    reg = ObjectStoreRegistry({SOURCE_URL: src})
    cubes = {}
    for grid in grids:
        cubes[grid], _ = build_grid(grid, reg, src)

    if args.dry_run:
        cfg = ic.RepositoryConfig.default()
        cfg.set_virtual_chunk_container(ic.VirtualChunkContainer(
            f"{SOURCE_URL}/", ic.s3_store(region=REGION, anonymous=True)))
        mem = ic.Repository.create(
            ic.in_memory_storage(), config=cfg,
            authorize_virtual_chunk_access=ic.containers_credentials({f"{SOURCE_URL}/": ic.s3_anonymous_credentials()}))
        s = mem.writable_session("main")
        write(s.store, cubes)
        s.commit("dry run")
        validate(mem.readonly_session("main").store, src, cubes)
        print("\n[dry-run] nothing written to Arraylake")
        return 0

    from arraylake import Client
    client = Client()
    ensure_org_setup(client, dry=False)
    if REPO.split("/")[1] in {r.name for r in client.list_repos(ORG)}:
        print(f"repo {REPO} already exists; refusing to overwrite", file=sys.stderr)
        return 1
    repo = client.create_repo(
        REPO, bucket_config_nickname=NICKNAME,
        description="NOAA CEFI MOM6 Northwest Atlantic decadal forecast (yearly, r20250819): virtual FMRC "
                    "(time=init, member, step=lead year) over the NODD NetCDF files; raw + regrid grids.",
        authorize_virtual_chunk_access={SOURCE_URL: SOURCE_NICKNAME})
    print(f"created repo {REPO}")
    session = repo.writable_session("main")
    write(session.store, cubes)
    snap = session.commit(
        f"FMRC layout ({', '.join(grids)}): (time, member, step, ...) virtual cube per grid from the "
        f"{RELEASE} NetCDFs; integer lead-year step + 2-D valid_time; raw statics from ocean_static.nc")
    print(f"committed {snap} to main")
    client.modify_repo(REPO, update_metadata={
        "model": "MOM6-COBALT", "region": "northwest_atlantic", "experiment": "decadal_forecast",
        "experiment_kind": "forecast", "frequency": "yearly", "release": RELEASE, "license": "CC-BY-4.0"})
    validate(repo.readonly_session(branch="main").store, src, cubes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
