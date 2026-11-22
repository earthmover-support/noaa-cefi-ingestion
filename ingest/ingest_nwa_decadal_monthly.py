#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "arraylake>=0.16", "coiled", "distributed",
#   "virtualizarr[hdf] @ git+https://github.com/zarr-developers/VirtualiZarr.git@de4e669f18e323b2814f0625785dbd5766d65652",
#   "icechunk>=1.2", "obstore>=0.5", "obspec-utils>=0.2", "xarray", "zarr>=3", "numpy", "pandas",
#   "h5py", "h5netcdf", "scipy",
# ]
# ///
"""NWA decadal forecast monthly (r20250925) -> `noaa-cefi/cefi-nwa-decadal-forecast-monthly`.

Virtual FMRC, parsed directly from the NetCDF4 files. Two groups:

    raw          (time: 61, member: 10, step: 120, yh: 845, xh: 775)   18 vars + statics + time fields
    regrid       (time: 61, member: 10, step: 120, lat: 844, lon: 774) 18 vars

  time        forecast_reference_time (init, from the filename)
  step        integer months 0..119 (forecast_period, units "months")
  valid_time  2-D (time, step) = decoded source `lead` (mid-month)
  average_T1/T2/DT, time_bnds: stored ONCE per (time, step) after verifying all 10 member rows are identical
                              (any difference aborts the run: ask the user)
  raw statics (hybrid, user decision 2026-10-22): geolon/geolat/_c from the NWA hindcast monthly static (gap-free,
                              asserted equal to this release's wherever unmasked); deptho/areacello/wet from this release.

Three phases, so the expensive one runs once:
  --parse    open all 2,198 files on a Coiled cluster (workspace `earthmover-devs`): VirtualiZarr manifest +
             per-file checks + sha1 of source chunk windows (ground truth). Caches results in ingest/.cache/.
  --dry-run  build the cubes from the cache, write an in-memory Icechunk repo, validate against the cached hashes.
  --write    create `noaa-cefi/cefi-nwa-decadal-forecast-monthly`, commit to main, validate the committed repo.
`--local` runs the parse phase in a local process pool instead (use with --inits for a small test).
Source files are never modified.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import io
import itertools
import pickle
import sys
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

ORG = "noaa-cefi"
REPO = f"{ORG}/cefi-nwa-decadal-forecast-monthly"
BUCKET = "noaa-oar-cefi-regional-mom6-pds"
SOURCE_URL = f"s3://{BUCKET}"
NICKNAME = BUCKET  # repo storage: bucket config with prefix `arraylake/`
SOURCE_NICKNAME = f"{BUCKET}-source"  # same bucket, no prefix, anonymous: virtual chunk source
REGION = "us-east-1"
RELEASE = "r20250925"
PREFIX = "northwest_atlantic/full_domain/decadal_forecast/monthly"
GRIDS = ("raw", "regrid")
NSTEP = 120
CACHE = Path(__file__).resolve().parent / ".cache" / "nwa_decadal_monthly.pkl"

COORD_STATICS = ("geolon", "geolat", "geolon_c", "geolat_c")  # from the hindcast static
OWN_STATICS = ("deptho", "areacello", "wet")  # this release's own (its mask differs from the hindcast's)
HINDCAST_STATIC = "northwest_atlantic/full_domain/hindcast/monthly/raw/r20250715/ocean_static.nc"
PER_INIT_TIMES = ("average_T1", "average_T2", "time_bnds")  # "days since init" -> datetimes
TIME_FIELDS = (*PER_INIT_TIMES, "average_DT")  # all stored once per (time, step)
LOADABLE = ["lead", "member", "lat", "lon", "xh", "yh", "nv", *TIME_FIELDS]
TIME_ENCODING = {"units": "days since 1965-01-01 00:00:00", "calendar": "gregorian", "dtype": "float64"}
DROP_GROUP_ATTRS = ("coordinates", "title", "cefi_filename", "cefi_ori_filename",
                    "cefi_variable", "cefi_init_date", "associated_files")
LONLAT = {"geolon": "longitude", "geolat": "latitude"}
DEGREE_UNITS = {"degrees_east", "degree_east", "degrees_north", "degree_north"}
FULL_CHECK_INITS = ("196501", "196801", "202501")  # every chunk of these files is hashed
FULL_CHECK_VARS = ("tos", "btm_o2")

WORKSPACE = "earthmover-devs"  # NOT the default Coiled account on this machine
ENV_NAME = "cefi-ingest"
N_WORKERS = 32


# --------------------------------------------------------------------------- #
# Source helpers (used on the workers too: imports are local to each function)
# --------------------------------------------------------------------------- #

def source_store():
    import obstore as obs
    return obs.store.S3Store(BUCKET, region=REGION, skip_signature=True)


def list_nc(store, grid: str) -> list[str]:
    pre = f"{PREFIX}/{grid}/{RELEASE}/"
    return sorted(o["path"] for b in store.list(prefix=pre) for o in b if o["path"].endswith(".nc"))


def parse_name(path: str) -> tuple[str, str] | None:
    """(variable, init 'YYYYMM'); None for the *_static.nc files."""
    name = path.rsplit("/", 1)[1]
    if name.endswith("_static.nc"):
        return None
    return name.split(".")[0], name.rsplit(".i", 1)[1].removesuffix(".nc")


class Reader:
    """h5py-compatible file object over an obstore reader."""
    def __new__(cls, r):
        import io as _io

        class _R(_io.RawIOBase):
            def readable(self): return True
            def seekable(self): return True
            def tell(self): return r.tell()
            def seek(self, o, w=0): return r.seek(o, w)
            def readinto(self, b):
                d = bytes(r.read(len(b)))
                b[:len(d)] = d
                return len(d)
        return _R()


def chunk_slices(shape, chunks, idx):
    return tuple(slice(i * c, min((i + 1) * c, n)) for i, c, n in zip(idx, chunks, shape))


def pick_chunks(grid_shape, key: str, full: bool):
    if full:
        return list(itertools.product(*[range(n) for n in grid_shape]))
    seed = int(hashlib.sha1(key.encode()).hexdigest()[:8], 16)
    rng = np.random.default_rng(seed)
    return [tuple(int(rng.integers(n)) for n in grid_shape)]


def parse_task(path: str, grid: str, init: str, var: str) -> dict:
    """Failure-tolerant wrapper: a bad file is returned as an error record (with its path), not raised."""
    import traceback
    try:
        return _parse_file(path, grid, init, var)
    except Exception as exc:  # noqa: BLE001
        return {"path": path, "grid": grid, "init": init, "var": var, "error": f"{type(exc).__name__}: {str(exc)[:300]}",
                "trace": traceback.format_exc()[-1500:]}


def _parse_file(path: str, grid: str, init: str, var: str) -> dict:
    """One source file: virtual dataset + per-file checks + sha1 of source chunk windows."""
    import h5py
    import obstore as obs
    import virtualizarr as vz
    from obspec_utils.registry import ObjectStoreRegistry
    from virtualizarr.parsers import HDFParser

    store = source_store()
    reg = ObjectStoreRegistry({SOURCE_URL: store})
    f = h5py.File(Reader(obs.open_reader(store, path, buffer_size=256 * 1024)), "r")
    d = f[var]
    assert d.chunks is not None, (path, var)
    # A variable that was never written (shape and chunking present, ZERO chunks allocated) reads as all fill.
    # VirtualiZarr cannot build a manifest for it, so it is parsed without the variable and rebuilt with an
    # empty manifest in build_cubes (source file itself is untouched).
    empty = d.id.get_num_chunks() == 0
    ds = vz.open_virtual_dataset(f"{SOURCE_URL}/{path}", registry=reg,
                                 parser=HDFParser(drop_variables=[var] if empty else None),
                                 loadable_variables=LOADABLE, decode_times=False)
    chk: dict = {}
    assert list(ds["member"].values) == list(range(1, 11)), ("member", path, ds["member"].values)
    lead = np.asarray(ds["lead"].values, "float64")
    assert len(lead) == NSTEP, (path, len(lead))
    chk["lead"] = tuple(lead)
    chk["lead_units"] = ds["lead"].attrs["units"]
    for name in TIME_FIELDS:  # all member rows identical?
        if name in ds:
            a = ds[name].values
            chk[f"{name}_members_identical"] = bool(all(np.array_equal(a[0], a[i]) for i in range(a.shape[0])))
            chk[f"{name}_units"] = ds[name].attrs.get("units")
    chk["empty"] = empty
    chk["fill"] = None if empty else ds[var].encoding.get("_FillValue", ds[var].attrs.get("_FillValue"))
    grid_shape = tuple(-(-n // c) for n, c in zip(d.shape, d.chunks))
    full = init in FULL_CHECK_INITS and var in FULL_CHECK_VARS
    truth = {}
    for idx in pick_chunks(grid_shape, f"{grid}/{init}/{var}", full):
        sl = chunk_slices(d.shape, d.chunks, idx)
        truth[idx] = hashlib.sha1(np.ascontiguousarray(d[sl], dtype="<f4").tobytes()).hexdigest()
    chk["shape"], chk["chunks"], chk["dtype"] = d.shape, d.chunks, str(d.dtype)
    return {"path": path, "grid": grid, "init": init, "var": var, "ds": ds, "chk": chk, "truth": truth}


# --------------------------------------------------------------------------- #
# Phase 1: parse (Coiled or local)
# --------------------------------------------------------------------------- #

def ensure_software_env() -> None:
    import coiled
    import dask
    import distributed
    if ENV_NAME in coiled.list_software_environments(workspace=WORKSPACE):
        print(f"  [coiled] software env {ENV_NAME!r} exists in {WORKSPACE}", flush=True)
        return
    py = f"{sys.version_info.major}.{sys.version_info.minor}"
    pip = [
        "virtualizarr[hdf] @ git+https://github.com/zarr-developers/VirtualiZarr.git@de4e669f18e323b2814f0625785dbd5766d65652",
        "icechunk>=1.2", "obstore>=0.5", "obspec-utils>=0.2", "xarray", "zarr>=3", "numpy", "pandas",
        "h5py", "h5netcdf", "scipy", f"distributed=={distributed.__version__}", f"dask=={dask.__version__}",
    ]
    print(f"  [coiled] building software env {ENV_NAME!r} (python {py}) in {WORKSPACE} ...", flush=True)
    coiled.create_software_environment(
        name=ENV_NAME, workspace=WORKSPACE,
        conda={"channels": ["conda-forge"], "dependencies": [f"python={py}", "pip", {"pip": pip}]})


def run_parse(jobs: list[tuple], local: bool) -> list[dict]:
    results: list[dict] = []
    t = time.time()
    if local:
        with cf.ProcessPoolExecutor(4) as pool:
            futs = [pool.submit(parse_task, *j) for j in jobs]
            for n, fut in enumerate(cf.as_completed(futs), 1):
                results.append(fut.result())
                if n % 25 == 0 or n == len(futs):
                    print(f"  [parse] {n}/{len(futs)} ({time.time() - t:.0f}s)", flush=True)
        return results

    import coiled
    from distributed import Client, as_completed
    ensure_software_env()
    cluster = coiled.Cluster(
        name="cefi-decadal-monthly", workspace=WORKSPACE, software=ENV_NAME, n_workers=N_WORKERS,
        region=REGION, spot_policy="spot_with_fallback", worker_cpu=2, worker_memory="8GiB",
        worker_options={"nthreads": 1}, scheduler_vm_types=["m6i.large"], idle_timeout="10 minutes",
        shutdown_on_close=True)
    try:
        print(f"  [coiled] cluster up: {cluster.name} ({N_WORKERS} workers) {cluster.dashboard_link}", flush=True)
        client = Client(cluster)
        futs = client.map(parse_task, *zip(*jobs), retries=1, pure=False)
        for n, fut in enumerate(as_completed(futs), 1):
            results.append(fut.result())
            if n % 100 == 0 or n == len(futs):
                print(f"  [parse] {n}/{len(futs)} ({time.time() - t:.0f}s)", flush=True)
    finally:
        cluster.close()
        print("  [coiled] cluster closed", flush=True)
    return results


def phase_parse(local: bool, inits: list[str] | None) -> None:
    store = source_store()
    jobs = []
    for grid in GRIDS:
        for p in list_nc(store, grid):
            info = parse_name(p)
            if info and (inits is None or info[1] in inits):
                jobs.append((p, grid, info[1], info[0]))
    print(f"[parse] {len(jobs)} files ({'local' if local else 'Coiled'})", flush=True)
    results = run_parse(jobs, local)
    CACHE.parent.mkdir(exist_ok=True)
    with open(CACHE, "wb") as fh:
        pickle.dump(results, fh)
    bad = [r for r in results if "error" in r]
    print(f"[parse] cached {len(results)} results ({len(bad)} failed) -> {CACHE}", flush=True)
    for r in bad:
        print(f"  FAILED {r['path']}\n    {r['error']}", flush=True)
    if bad:
        sys.exit(2)


# --------------------------------------------------------------------------- #
# Phase 2: build cubes from the cache
# --------------------------------------------------------------------------- #

def to_fmrc(ds, init: str, chk_by_var: dict):
    import xarray as xr
    t0 = pd.Timestamp(f"{init[:4]}-{init[4:]}-01")
    first = next(iter(chk_by_var.values()))
    assert first["lead_units"] == f"days since {t0:%Y-%m-%d}", (init, first["lead_units"])

    def decode(days) -> np.ndarray:
        days = np.asarray(days, "float64")
        return (t0 + pd.to_timedelta(days.ravel(), unit="D")).values.reshape(days.shape)

    valid = decode(ds["lead"].values)
    months = pd.period_range(t0, periods=NSTEP, freq="M")
    assert (pd.DatetimeIndex(valid).to_period("M") == months).all(), f"{init}: lead not in init month + step"
    out = ds.drop_vars("lead").rename_dims(lead="step")
    for name in TIME_FIELDS:
        if name in out:
            assert all(c[f"{name}_members_identical"] for c in chk_by_var.values() if f"{name}_members_identical" in c), \
                f"{init}/{name}: member rows differ -- STOP and ask the user"
            v = out[name].isel(member=0, drop=True)
            vals = decode(v.values) if name in PER_INIT_TIMES else v.values
            attrs = {k: a for k, a in v.attrs.items() if k not in ("units", "calendar", "calendar_type")}
            if name == "average_DT":
                attrs["units"] = "days"
            out[name] = (v.dims, vals, attrs)
    out = out.assign_coords(
        step=("step", np.arange(NSTEP), {
            "standard_name": "forecast_period", "long_name": "forecast lead (months since initialization)",
            "units": "months",
            "comment": "0 = first month after initialization; see valid_time for the mid-month timestamp"}),
        valid_time=("step", valid, {"standard_name": "time", "long_name": "valid time (mid-month)"}))
    out = out.expand_dims(time=[t0])
    out["valid_time"] = out["valid_time"].expand_dims(time=[t0])
    return out.set_coords("valid_time")


def _nc3(store, path: str):
    from scipy.io import netcdf_file
    import obstore as obs
    return netcdf_file(io.BytesIO(bytes(obs.get(store, path).bytes())), mmap=False)


def load_statics(store, reg):
    """Hybrid statics (user decision 2026-10-22). Both static files are netCDF3 (P4), read with scipy."""
    import xarray as xr
    own = _nc3(store, f"{PREFIX}/raw/{RELEASE}/ocean_static.nc")
    hc = _nc3(store, HINDCAST_STATIC)

    def var(nc, n):
        v = nc.variables[n]
        return v.dimensions, np.array(v[:]), {k: (getattr(v, k).decode() if isinstance(getattr(v, k), bytes) else getattr(v, k))
                                              for k in ("long_name", "units", "cell_methods") if hasattr(v, k)}

    out = xr.Dataset()
    notes = {}
    for n in OWN_STATICS:
        dims, a, attrs = var(own, n)
        out[n] = (dims, a.astype("float32"), attrs)
    for n in COORD_STATICS:
        dims, o, _ = var(own, n)
        _, h, attrs = var(hc, n)
        h = h.astype("float32")
        assert np.isfinite(h).all() and (np.abs(h) < 1e19).all(), f"hindcast {n} has gaps"
        valid = np.abs(o) < 1e19
        assert o.shape == h.shape and np.array_equal(o[valid], h[valid]), f"{n}: hindcast != release where valid"
        notes[n] = int((~valid).sum())
        attrs["comment"] = ("taken from the NWA hindcast monthly r20250715 ocean_static.nc: this release's own "
                            "ocean_static.nc masks land with 1e20; identical wherever that file is unmasked")
        out[n] = (dims, h, attrs)
    for d in ("xh", "yh", "xq", "yq"):
        assert np.array_equal(np.asarray(hc.variables[d][:], "float64"), np.asarray(own.variables[d][:], "float64")), d
        out = out.assign_coords({d: (d, np.asarray(own.variables[d][:], "float64"),
                                     {"units": own.variables[d].units.decode()} if hasattr(own.variables[d], "units") else {})})
    ow, hw = np.asarray(own.variables["wet"][:]), np.asarray(hc.variables["wet"][:])
    ok = np.abs(ow) < 1e19
    print(f"  [statics] this release's masked (1e20) cells per coordinate field: {notes}; "
          f"wet differs from the hindcast at {int((ow != hw)[ok].sum())} cells where this release is unmasked "
          f"(kept this release's own deptho/areacello/wet)", flush=True)
    return out


def build_cubes(results: list[dict]):
    import virtualizarr  # noqa: F401  (registers accessors)
    import xarray as xr
    store = source_store()
    from obspec_utils.registry import ObjectStoreRegistry
    reg = ObjectStoreRegistry({SOURCE_URL: store})
    cubes, truth = {}, {}
    for grid in GRIDS:
        rs = [r for r in results if r["grid"] == grid]
        by_init: dict[str, dict[str, dict]] = defaultdict(dict)
        for r in rs:
            by_init[r["init"]][r["var"]] = r
        var_sets = {tuple(sorted(v)) for v in by_init.values()}
        assert len(var_sets) == 1, f"[{grid}] inits have different variable sets: {var_sets}"
        n_vars = len(next(iter(var_sets)))
        print(f"  [{grid}] {len(by_init)} inits x {n_vars} vars", flush=True)
        leads = {r["chk"]["lead"] for r in rs}
        print(f"  [{grid}] distinct `lead` vectors across files: {len(leads)}", flush=True)
        from virtualizarr.manifests import ChunkManifest, ManifestArray
        slices = []
        for init in sorted(by_init):
            vs = by_init[init]
            for v, r in vs.items():
                if r["chk"].get("empty"):
                    sib = next(x for i2, d2 in by_init.items() for v2, x in d2.items()
                               if v2 == v and not x["chk"].get("empty"))
                    ref = sib["ds"][v]
                    ma = ref.data
                    assert tuple(ma.shape) == tuple(r["chk"]["shape"]), (grid, init, v)
                    # a bare Variable: no coordinates carried over (this init's `lead` differs from the sibling's)
                    r["ds"][v] = xr.Variable(ref.dims, ManifestArray(
                        metadata=ma.metadata, chunkmanifest=ChunkManifest({}, shape=ma.manifest.shape_chunk_grid)),
                        ref.attrs, ref.encoding)
                    print(f"  [{grid}] {init}/{v}: source variable never written (0 chunks) -> empty manifest "
                          f"(reads as fill)", flush=True)
            merged = xr.merge([r["ds"] for r in vs.values()], compat="override", join="exact",
                              combine_attrs="drop_conflicts")
            slices.append(to_fmrc(merged, init, {v: r["chk"] for v, r in vs.items()}))
        cube = xr.concat(slices, dim="time", coords="minimal", compat="override",
                         combine_attrs="drop_conflicts", join="exact")
        cube.attrs = {k: v for k, v in cube.attrs.items() if k not in DROP_GROUP_ATTRS}
        cube.attrs["cefi_init_date"] = "i" + ",i".join(sorted(by_init))
        cube.attrs["featureType"] = "forecast_model_run_collection"
        cube["time"].attrs = {"standard_name": "forecast_reference_time",
                              "long_name": "forecast initialization time"}
        cube["member"].attrs.setdefault("long_name", "ensemble member")
        for name in ("time", "valid_time", *PER_INIT_TIMES):
            if name in cube.variables:
                cube[name].encoding = dict(TIME_ENCODING)
        if grid == "raw":
            statics = load_statics(store, reg)
            for d in ("yh", "xh"):
                assert np.array_equal(statics[d].values, cube[d].values), f"static {d} != data {d}"
            cube = cube.assign(statics)
        cubes[grid] = cube
        truth[grid] = {(r["init"], r["var"]): r["truth"] for r in rs}
        print(f"  [{grid}] cube {dict(cube.sizes)}", flush=True)
    return cubes, truth


def patch_group_metadata(store, group: str) -> None:
    import zarr
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
        for name, a in arrays.items():
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
    for _, a in g.arrays():
        dims = a.metadata.dimension_names or ()
        coords = a.attrs.get("coordinates")
        if "time" in dims and "step" in dims and coords and "valid_time" not in coords.split():
            a.attrs["coordinates"] = f"{coords} valid_time"


def write(store, cubes) -> None:
    for grid, cube in cubes.items():
        accessor = getattr(cube, "vz", None) or cube.virtualize
        accessor.to_icechunk(store, group=grid)
        patch_group_metadata(store, grid)


def validate(store, cubes, truth) -> None:
    """Hash the same chunk windows from the written store as were hashed from the source files on the workers."""
    import zarr
    for grid, cube in cubes.items():
        g = zarr.open_group(store, mode="r", path=grid)
        times = [f"{t:%Y%m}" for t in pd.DatetimeIndex(cube.time.values)]
        tindex = {t: k for k, t in enumerate(times)}
        items = [(init, var, idx, h) for (init, var), tr in truth[grid].items() for idx, h in tr.items()]
        print(f"  [{grid}] validating {len(items)} chunk windows ({len(truth[grid])} files)", flush=True)

        def check(item) -> None:
            init, var, idx, want = item
            arr = g[var]
            chunks = arr.chunks[1:]  # (member, step, y, x)
            sl = chunk_slices(arr.shape[1:], chunks, idx)
            for attempt in range(4):
                try:
                    a = arr[(tindex[init], *sl)]
                    break
                except Exception as exc:  # NODD drops reads occasionally
                    if attempt == 3:
                        raise
                    print(f"  [{grid}] {init}/{var}: transient read error, retry ({type(exc).__name__})", flush=True)
            got = hashlib.sha1(np.ascontiguousarray(a, dtype="<f4").tobytes()).hexdigest()
            assert got == want, (grid, init, var, idx)

        with cf.ThreadPoolExecutor(16) as pool:
            for n, _ in enumerate(pool.map(check, items), 1):
                if n % 500 == 0:
                    print(f"  [{grid}] checked {n}/{len(items)}", flush=True)
        print(f"  [{grid}] OK: {len(items)} source chunk windows (every chunk of "
              f"{list(FULL_CHECK_INITS)} x {list(FULL_CHECK_VARS)}) match the written store", flush=True)


def ensure_org_setup(client) -> None:
    if SOURCE_NICKNAME not in {b.nickname for b in client.list_bucket_configs(org=ORG)}:
        raise SystemExit(f"bucket config {SOURCE_NICKNAME!r} missing; run the decadal-yearly script first")
    vcaps = client.list_virtual_chunk_access_policies(ORG)
    assert any(p.url_prefix.rstrip("/") == SOURCE_URL and p.subprefix == "" for p in vcaps), "VCAP missing"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--parse", action="store_true")
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--write", action="store_true")
    g.add_argument("--reparse-failed", action="store_true", help="re-run only the failed files locally; patch the cache")
    ap.add_argument("--local", action="store_true", help="--parse in a local process pool, not Coiled")
    ap.add_argument("--inits", default="", help="comma-separated YYYYMM subset (testing only)")
    args = ap.parse_args()

    if args.reparse_failed:
        with open(CACHE, "rb") as fh:
            results = pickle.load(fh)
        bad = [r for r in results if "error" in r]
        print(f"re-parsing {len(bad)} failed file(s) locally", flush=True)
        fixed = {r["path"]: parse_task(r["path"], r["grid"], r["init"], r["var"]) for r in bad}
        results = [fixed.get(r["path"], r) for r in results]
        still = [r for r in results if "error" in r]
        with open(CACHE, "wb") as fh:
            pickle.dump(results, fh)
        print(f"cache patched; {len(still)} still failing", flush=True)
        return 1 if still else 0

    if args.parse:
        phase_parse(args.local, [i for i in args.inits.split(",") if i] or None)
        return 0

    import icechunk as ic
    with open(CACHE, "rb") as fh:
        results = pickle.load(fh)
    print(f"loaded {len(results)} cached parse results", flush=True)
    assert not [r for r in results if "error" in r], "cache contains failed files; rerun --parse"
    cubes, truth = build_cubes(results)

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
        validate(mem.readonly_session("main").store, cubes, truth)
        print("\n[dry-run] nothing written to Arraylake")
        return 0

    from arraylake import Client
    client = Client()
    ensure_org_setup(client)
    if REPO.split("/")[1] in {r.name for r in client.list_repos(ORG)}:
        print(f"repo {REPO} already exists; refusing to overwrite", file=sys.stderr)
        return 1
    repo = client.create_repo(
        REPO, bucket_config_nickname=NICKNAME,
        description="NOAA CEFI MOM6 Northwest Atlantic decadal forecast (monthly, r20250925): virtual FMRC "
                    "(time=init, member, step=lead month) over the NODD NetCDF files; raw + regrid grids.",
        authorize_virtual_chunk_access={SOURCE_URL: SOURCE_NICKNAME})
    print(f"created repo {REPO}", flush=True)
    session = repo.writable_session("main")
    write(session.store, cubes)
    snap = session.commit(
        f"FMRC layout (raw, regrid): (time, member, step, ...) virtual cubes from the {RELEASE} "
        f"NetCDFs; integer lead-month step + 2-D valid_time; time fields once per (time, step); hybrid raw statics")
    print(f"committed {snap} to main", flush=True)
    client.modify_repo(REPO, update_metadata={
        "model": "MOM6-COBALT", "region": "northwest_atlantic", "experiment": "decadal_forecast",
        "experiment_kind": "forecast", "frequency": "monthly", "release": RELEASE, "license": "CC-BY-4.0"})
    validate(repo.readonly_session(branch="main").store, cubes, truth)
    return 0


if __name__ == "__main__":
    sys.exit(main())
