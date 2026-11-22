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
"""NWA seasonal forecast (r20250710) and reforecast (r20250413) -> `noaa-cefi/cefi-nwa-seasonal-{forecast,reforecast}-monthly`.

Virtual FMRC parsed directly from the NetCDF4 files; groups `raw` and `regrid` (flat; user rule 2026-10-23):

    (time: n_inits, member: 10, step: 12, <y>, <x>)     each source variable AND its bundled `<var>_anom`
  time        forecast_reference_time (init, from the filename)
  step        integer months 0..11 (forecast_period, units "months")
  valid_time  2-D (time, step) = init + step months (checked against the source `valid_time` where present)
  fill value  unified to NaN (P8; verified that no stored value equals the 9.97e36 fill, see `fill_hits`)
  dim order   (member, step): the forecast files are (lead, member), so every forecast array goes through the Zarr
              `transpose` codec (virtual chunks keep their bytes); reforecast files are already (member, lead).
  dropped     per-file scalars `init`, `month` and the raw per-file `valid_time` (all derivable from `time`/`step`)
  forecast init 202510 (only 9 members, member 6 missing, different chunks): enters the virtual concat as a
              placeholder (empty manifest) and is then written as NATIVE chunks (`to_zarr(region=...)`) with member 6 = NaN.
  raw statics: this release's `ocean_static.nc` (netCDF3), core 5; coordinates from the NWA hindcast monthly static
              if this release's are masked (hybrid rule, user 2026-10-22).

Phases (results of the expensive parse are cached in ingest/.cache/):
  --parse [--local]   open every file (Coiled workspace `earthmover-devs`, or a local process pool): virtual dataset,
                      per-file checks, sha1 of source chunk windows (independent ground truth).
  --reparse-failed    re-run only failed files locally and patch the cache.
  --dry-run           build from cache, write an in-memory repo, validate (hashes + native init 202510 vs source).
  --write             create the Arraylake repo, commit to main, validate the committed repo.
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
BUCKET = "noaa-oar-cefi-regional-mom6-pds"
SOURCE_URL = f"s3://{BUCKET}"
NICKNAME = BUCKET  # repo storage config (prefix arraylake/)
SOURCE_NICKNAME = f"{BUCKET}-source"  # same bucket, no prefix, anonymous
REGION = "us-east-1"
GRIDS = ("raw", "regrid")
NSTEP = 12
NATIVE_INIT = "202510"  # forecast only
HINDCAST_STATIC = "northwest_atlantic/full_domain/hindcast/monthly/raw/r20250715/ocean_static.nc"
CORE_STATICS = ("geolon", "geolat", "deptho", "areacello", "wet")
COORD_STATICS = ("geolon", "geolat")
SCALARS = ("init", "month", "valid_time")
LOADABLE = ["lead", "member", "xh", "yh", "lat", "lon", *SCALARS]
TIME_ENCODING = {"units": "days since 1994-01-01 00:00:00", "calendar": "gregorian", "dtype": "float64"}
DROP_GROUP_ATTRS = ("coordinates", "title", "cefi_filename", "cefi_ori_filename", "cefi_variable",
                    "cefi_init_date", "associated_files")
LONLAT = {"geolon": "longitude", "geolat": "latitude"}
DEGREE_UNITS = {"degrees_east", "degree_east", "degrees_north", "degree_north"}
WORKSPACE = "earthmover-devs"
ENV_NAME = "cefi-ingest-2"  # adds arraylake: pickled Arraylake-vended sessions import it on the workers

PRODUCTS = {
    "forecast": dict(
        repo=f"{ORG}/cefi-nwa-seasonal-forecast-monthly", exp="seasonal_forecast", release="r20250710",
        transpose=True, n_workers=8,
        desc="NOAA CEFI MOM6 Northwest Atlantic seasonal forecast (monthly, r20250710): virtual FMRC "
             "(time=init, member, step=lead month) incl. *_anom; init 202510 stored as native chunks."),
    "reforecast": dict(
        repo=f"{ORG}/cefi-nwa-seasonal-reforecast-monthly", exp="seasonal_reforecast", release="r20250413",
        transpose=False, n_workers=16,
        desc="NOAA CEFI MOM6 Northwest Atlantic seasonal reforecast (monthly, r20250413): virtual FMRC "
             "(time=init, member, step=lead month) incl. *_anom."),
}


def cfg_of(product: str) -> dict:
    return PRODUCTS[product]


def prefix_of(product: str, grid: str) -> str:
    c = cfg_of(product)
    return f"northwest_atlantic/full_domain/{c['exp']}/monthly/{grid}/{c['release']}/"


def cache_path(product: str) -> Path:
    return Path(__file__).resolve().parent / ".cache" / f"nwa_seasonal_{product}.pkl"


def full_inits(product: str) -> tuple[str, ...]:
    return ("202601", "202604", "202607") if product == "forecast" else ("199401", "200801", "202310")


# --------------------------------------------------------------------------- #
# Source helpers (run on workers too: imports are local)
# --------------------------------------------------------------------------- #

def source_store():
    import obstore as obs
    return obs.store.S3Store(BUCKET, region=REGION, skip_signature=True)


def list_nc(store, product: str, grid: str) -> list[str]:
    return sorted(o["path"] for b in store.list(prefix=prefix_of(product, grid))
                  for o in b if o["path"].endswith(".nc"))


def parse_name(path: str):
    name = path.rsplit("/", 1)[1]
    if name.endswith("_static.nc"):
        return None
    return name.split(".")[0], name.rsplit(".i", 1)[1].removesuffix(".nc")


class Reader:
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


def sha(a) -> str:
    return hashlib.sha1(np.ascontiguousarray(a, dtype="<f4").tobytes()).hexdigest()


def parse_task(path: str, product: str, grid: str, init: str, var: str) -> dict:
    """Failure-tolerant wrapper: a bad file comes back as an error record (with its path)."""
    import traceback
    try:
        return _parse_file(path, product, grid, init, var)
    except Exception as exc:  # noqa: BLE001
        return {"path": path, "grid": grid, "init": init, "var": var, "error": f"{type(exc).__name__}: {str(exc)[:300]}",
                "trace": traceback.format_exc()[-1500:]}


def _parse_file(path: str, product: str, grid: str, init: str, var: str) -> dict:
    import h5py
    import obstore as obs
    import virtualizarr as vz
    from obspec_utils.registry import ObjectStoreRegistry
    from virtualizarr.parsers import HDFParser

    store = source_store()
    reg = ObjectStoreRegistry({SOURCE_URL: store})
    ds = vz.open_virtual_dataset(f"{SOURCE_URL}/{path}", registry=reg, parser=HDFParser(),
                                 loadable_variables=LOADABLE, decode_times=False)
    f = h5py.File(Reader(obs.open_reader(store, path, buffer_size=256 * 1024)), "r")
    chk: dict = {"member": tuple(int(m) for m in ds["member"].values),
                 "lead": tuple(int(k) for k in ds["lead"].values),
                 "lead_units": ds["lead"].attrs.get("units"),
                 "init_units": ds["init"].attrs["units"], "init_value": int(ds["init"].values),
                 "month": int(ds["month"].values),
                 "valid_time": None if "valid_time" not in ds else tuple(int(v) for v in ds["valid_time"].values),
                 "valid_time_units": None if "valid_time" not in ds else ds["valid_time"].attrs["units"]}
    for c in ("xh", "yh", "lat", "lon"):
        if c in ds:
            chk[f"coord_{c}"] = hashlib.sha1(np.asarray(ds[c].values, "float64").tobytes()).hexdigest()
    ds = ds.drop_vars([n for n in SCALARS if n in ds])
    full = init in full_inits(product)
    arrays: dict = {}
    truth: dict = {}
    for name in (var, f"{var}_anom"):
        d = f[name]
        assert d.chunks is not None, (path, name)
        attr_fill = d.attrs.get("_FillValue")
        attr_fill = None if attr_fill is None else float(np.asarray(attr_fill).ravel()[0])
        # NB: the netCDF `_FillValue` attribute and the HDF5-level fill value (what unwritten chunks hold) can differ
        # (reforecast raw: attr NaN everywhere, HDF5 fill 9.97e36 in ~80% of files).
        hdf_fill = float(np.asarray(d.fillvalue).ravel()[0])
        expected = int(np.prod([-(-n // c) for n, c in zip(d.shape, d.chunks)]))
        a = d[...].astype("float64")
        info = {"shape": d.shape, "chunks": d.chunks, "dtype": str(d.dtype), "attr_fill": attr_fill, "hdf_fill": hdf_fill,
                "dims": ds[name].dims, "nchunks": int(d.id.get_num_chunks()), "expected_chunks": expected,
                "fill_hits": int((np.abs(a) > 1e30).sum()), "nan_count": int(np.isnan(a).sum()), "size": int(a.size)}
        del a
        arrays[name] = info
        if init == NATIVE_INIT and product == "forecast":
            continue  # written natively; validated against the source separately
        grid_shape = tuple(-(-n // c) for n, c in zip(d.shape, d.chunks))
        t = {}
        for idx in pick_chunks(grid_shape, f"{grid}/{init}/{name}", full):
            t[idx] = sha(d[chunk_slices(d.shape, d.chunks, idx)])
        truth[name] = t
    chk["arrays"] = arrays
    return {"path": path, "grid": grid, "init": init, "var": var, "ds": ds, "chk": chk, "truth": truth}


# --------------------------------------------------------------------------- #
# Phase 1: parse
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
        "h5py", "h5netcdf", "scipy", "arraylake>=0.16", f"distributed=={distributed.__version__}", f"dask=={dask.__version__}"]
    coiled.create_software_environment(
        name=ENV_NAME, workspace=WORKSPACE,
        conda={"channels": ["conda-forge"], "dependencies": [f"python={py}", "pip", {"pip": pip}]})


def run_parse(jobs: list[tuple], product: str, local: bool) -> list[dict]:
    results: list[dict] = []
    t = time.time()
    if local:
        with cf.ProcessPoolExecutor(4) as pool:
            futs = [pool.submit(parse_task, *j) for j in jobs]
            for n, fut in enumerate(cf.as_completed(futs), 1):
                results.append(fut.result())
                if n % 20 == 0 or n == len(futs):
                    print(f"  [parse] {n}/{len(futs)} ({time.time() - t:.0f}s)", flush=True)
        return results
    import coiled
    from distributed import Client, as_completed
    ensure_software_env()
    n_workers = cfg_of(product)["n_workers"]
    cluster = coiled.Cluster(
        name=f"cefi-seasonal-{product}", workspace=WORKSPACE, software=ENV_NAME, n_workers=n_workers,
        region=REGION, spot_policy="spot_with_fallback", worker_cpu=2, worker_memory="8GiB",
        worker_options={"nthreads": 1}, scheduler_vm_types=["m6i.large"], idle_timeout="10 minutes",
        shutdown_on_close=True)
    try:
        print(f"  [coiled] cluster up: {cluster.name} ({n_workers} workers)", flush=True)
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


def phase_parse(product: str, local: bool) -> None:
    store = source_store()
    jobs = []
    for grid in GRIDS:
        for p in list_nc(store, product, grid):
            info = parse_name(p)
            if info:
                jobs.append((p, product, grid, info[1], info[0]))
    print(f"[parse] {product}: {len(jobs)} files ({'local' if local else 'Coiled'})", flush=True)
    results = run_parse(jobs, product, local)
    save_cache(product, results)


def save_cache(product: str, results: list[dict]) -> None:
    cp = cache_path(product)
    cp.parent.mkdir(exist_ok=True)
    with open(cp, "wb") as fh:
        pickle.dump(results, fh)
    bad = [r for r in results if "error" in r]
    print(f"[parse] cached {len(results)} results ({len(bad)} failed) -> {cp}", flush=True)
    for r in bad:
        print(f"  FAILED {r['path']}\n    {r['error']}", flush=True)
    if bad:
        sys.exit(2)


# --------------------------------------------------------------------------- #
# Phase 2: build
# --------------------------------------------------------------------------- #

def with_fill_nan(ma):
    """Same ManifestArray with fill_value NaN (P8: unify the 9.97e36 / NaN split)."""
    from virtualizarr.manifests import ManifestArray
    from zarr.core.metadata.v3 import ArrayV3Metadata
    d = ma.metadata.to_dict()
    d["fill_value"] = "NaN"
    return ManifestArray(metadata=ArrayV3Metadata.from_dict(d), chunkmanifest=ma.manifest)


def permute_manifest_array(ma, order):
    """Logical array = stored array with axes permuted by `order`, via a prepended Zarr `transpose` codec
    (verified 2026-10-20: stored chunk bytes unchanged, read-back identical)."""
    from virtualizarr.manifests import ChunkManifest, ManifestArray
    from zarr.core.metadata.v3 import ArrayV3Metadata
    md = ma.metadata
    d = md.to_dict()
    d["shape"] = [md.shape[i] for i in order]
    d["chunk_grid"] = {"name": "regular", "configuration": {"chunk_shape": [md.chunks[i] for i in order]}}
    inv = [order.index(i) for i in range(len(order))]
    d["codecs"] = [{"name": "transpose", "configuration": {"order": inv}}] + list(d["codecs"])
    entries = {}
    for key, ent in ma.manifest.dict().items():
        idx = [int(k) for k in key.split(".")]
        entries[".".join(str(idx[i]) for i in order)] = ent
    grid = tuple(ma.manifest.shape_chunk_grid[i] for i in order)
    return ManifestArray(metadata=ArrayV3Metadata.from_dict(d), chunkmanifest=ChunkManifest(entries, shape=grid))


def empty_like(ma):
    from virtualizarr.manifests import ChunkManifest, ManifestArray
    return ManifestArray(metadata=ma.metadata, chunkmanifest=ChunkManifest({}, shape=ma.manifest.shape_chunk_grid))


def valid_times(t0: pd.Timestamp) -> np.ndarray:
    return np.array([np.datetime64(t0 + pd.DateOffset(months=k), "ns") for k in range(NSTEP)])


def to_fmrc(product: str, ds, init: str, chk_by_var: dict, placeholder: bool):
    import xarray as xr
    t0 = pd.Timestamp(f"{init[:4]}-{init[4:]}-01")
    transpose = cfg_of(product)["transpose"]
    out = ds.drop_vars("lead").rename_dims(lead="step")
    for name in list(out.data_vars):
        v = out[name]
        ma = v.data
        if placeholder:
            ma = empty_like(ma)
        ma = with_fill_nan(ma)[np.newaxis, ...]  # leading time axis (before any transpose: VirtualiZarr cannot add axes after one)
        dims = ("time", *v.dims)
        if transpose:
            assert dims[1:3] == ("step", "member"), (name, dims)
            ma = permute_manifest_array(ma, (0, 2, 1, 3, 4))
            dims = ("time", "member", "step", *dims[3:])
        out[name] = xr.Variable(dims, ma, dict(v.attrs), dict(v.encoding))
        if name.endswith("_anom"):
            base = name[: -len("_anom")]
            out[name].attrs = {"long_name": f"{out[base].attrs.get('long_name', base)} anomaly",
                               "comment": f"`{name}` as published alongside `{base}` in the same NOAA source files; "
                                          "the reference climatology is not described in the files",
                               "cefi_base_variable": base}
    out = out.assign_coords(
        time=("time", [t0], {}),
        step=("step", np.arange(NSTEP), {
            "standard_name": "forecast_period", "long_name": "forecast lead (months since initialization)",
            "units": "months", "comment": "0 = the initialization month; see valid_time"}),
        valid_time=(("time", "step"), valid_times(t0)[None], {"standard_name": "time", "long_name": "valid time (month start)"}))
    return out


def check_file_set(product: str, grid: str, by_init: dict) -> None:
    """Step-5 checks (deferred until the real ingestion, user 2026-10-20)."""
    members_ok = {i: tuple(range(1, 11)) for i in by_init}
    if product == "forecast" and NATIVE_INIT in by_init:
        members_ok[NATIVE_INIT] = (1, 2, 3, 4, 5, 7, 8, 9, 10)
    coords: dict = defaultdict(set)
    hits = 0
    nan_fill = other_fill = 0
    unwritten: list = []
    for init, vs in by_init.items():
        t0 = pd.Timestamp(f"{init[:4]}-{init[4:]}-01")
        for var, r in vs.items():
            c = r["chk"]
            assert c["member"] == members_ok[init], (grid, init, var, c["member"])
            assert c["lead"] == tuple(range(NSTEP)), (grid, init, var, c["lead"])
            if product == "forecast":
                assert c["lead_units"] == "months", (grid, init, var, c["lead_units"])
            assert c["month"] == t0.month, (grid, init, var, c["month"])
            # `init` is "days since <base>": base = the init itself (value 0) for most files, but a FIXED base with an
            # offset value in 473 reforecast raw files; either way units + value must decode to the filename init
            base = pd.Timestamp(c["init_units"].split("since ")[1].strip().split()[0])
            assert base + pd.Timedelta(days=c["init_value"]) == t0, (grid, init, c["init_units"], c["init_value"])
            if c["valid_time"] is not None:  # raw forecast: source valid_time == init + step months
                assert c["valid_time_units"].startswith(f"days since {t0:%Y-%m-%d}")
                got = [(t0 + pd.to_timedelta(d, unit="D")) for d in c["valid_time"]]
                assert got == [t0 + pd.DateOffset(months=k) for k in range(NSTEP)], (grid, init, var, c["valid_time"])
            for k in ("coord_xh", "coord_yh", "coord_lat", "coord_lon"):
                if k in c:
                    coords[k].add(c[k])
            for name, a in c["arrays"].items():
                if np.isnan(a["hdf_fill"]):
                    nan_fill += 1
                else:
                    other_fill += 1
                hits += a["fill_hits"]
                if a["nchunks"] != a["expected_chunks"]:
                    unwritten.append((grid, init, name, a["nchunks"], a["expected_chunks"]))
    assert all(len(v) == 1 for v in coords.values()), f"[{grid}] coordinates differ between files: { {k: len(v) for k, v in coords.items()} }"
    assert not unwritten, f"[{grid}] {len(unwritten)} arrays with unwritten chunks (HDF5 fill would apply): {unwritten[:5]}"
    assert hits == 0, f"[{grid}] {hits} stored values with |x| > 1e30 (the 9.97e36 fill): the NaN override is NOT safe"
    print(f"  [{grid}] checks OK: members/lead/month/init/valid_time, coordinates identical across all files; "
          f"arrays whose HDF5 fill is NaN: {nan_fill}, 9.97e36: {other_fill}; every chunk written in every array; "
          f"stored values with |x|>1e30 across all arrays: {hits} (full scan)", flush=True)


def load_statics(product: str, store):
    """core 5 from this release's netCDF3 ocean_static.nc; coordinates from the hindcast static if masked."""
    import obstore as obs
    import xarray as xr
    from scipy.io import netcdf_file
    nc = lambda p: netcdf_file(io.BytesIO(bytes(obs.get(store, p).bytes())), mmap=False)  # noqa: E731
    own = nc(prefix_of(product, "raw") + "ocean_static.nc")
    hc = None

    def var(n, src):
        v = src.variables[n]
        return v.dimensions, np.array(v[:]), {k: (getattr(v, k).decode() if isinstance(getattr(v, k), bytes) else getattr(v, k))
                                              for k in ("long_name", "units") if hasattr(v, k)}
    out = xr.Dataset()
    report = {}
    for n in CORE_STATICS:
        dims, a, attrs = var(n, own)
        a = a.astype("float32")
        masked = int((np.abs(a) > 1e19).sum())
        report[n] = masked
        if n in COORD_STATICS and masked:
            hc = hc or nc(HINDCAST_STATIC)
            _, h, hattrs = var(n, hc)
            h = h.astype("float32")
            assert np.isfinite(h).all() and (np.abs(h) < 1e19).all(), f"hindcast {n} has gaps"
            valid = np.abs(a) < 1e19
            assert a.shape == h.shape and np.array_equal(a[valid], h[valid]), f"{n}: hindcast != release where valid"
            hattrs["comment"] = ("taken from the NWA hindcast monthly r20250715 ocean_static.nc: this release's own "
                                 "ocean_static.nc masks land with 1e20; identical wherever that file is unmasked")
            out[n] = (dims, h, hattrs)
        else:
            out[n] = (dims, a, attrs)
    for d in ("xh", "yh"):
        out = out.assign_coords({d: (d, np.asarray(own.variables[d][:], "float64"), {})})
    print(f"  [statics] masked (1e20) cells per static field in this release: {report}", flush=True)
    return out


def build_cubes(product: str, results: list[dict]):
    import virtualizarr  # noqa: F401
    import xarray as xr
    store = source_store()
    cubes, truth = {}, {}
    for grid in GRIDS:
        by_init: dict[str, dict[str, dict]] = defaultdict(dict)
        for r in results:
            if r["grid"] == grid:
                by_init[r["init"]][r["var"]] = r
        var_sets = {tuple(sorted(v)) for v in by_init.values()}
        assert len(var_sets) == 1, f"[{grid}] inits have different variable sets: {var_sets}"
        print(f"  [{grid}] {len(by_init)} inits x {len(next(iter(var_sets)))} vars (+ _anom)", flush=True)
        check_file_set(product, grid, by_init)
        sib = None
        slices = []
        for init in sorted(by_init):
            placeholder = product == "forecast" and init == NATIVE_INIT
            vs = by_init[init]
            if placeholder:  # normal-metadata stand-in: take a sibling init's arrays, empty manifests
                sib = by_init[sorted(i for i in by_init if i != NATIVE_INIT)[0]]
                vs_ds = [sib[v]["ds"] for v in sorted(sib)]
                chk = {v: sib[v]["chk"] for v in sib}
            else:
                vs_ds = [vs[v]["ds"] for v in sorted(vs)]
                chk = {v: vs[v]["chk"] for v in vs}
            merged = xr.merge(vs_ds, compat="override", join="exact", combine_attrs="drop_conflicts")
            if placeholder:
                merged = merged.assign_coords(member=("member", np.arange(1, 11, dtype="int32"), merged["member"].attrs))
            slices.append(to_fmrc(product, merged, init, chk, placeholder))
        cube = xr.concat(slices, dim="time", coords="minimal", compat="override",
                         combine_attrs="drop_conflicts", join="exact")
        cube.attrs = {k: v for k, v in cube.attrs.items() if k not in DROP_GROUP_ATTRS}
        cube.attrs["cefi_init_date"] = "i" + ",i".join(sorted(by_init))
        cube.attrs["featureType"] = "forecast_model_run_collection"
        cube["time"].attrs = {"standard_name": "forecast_reference_time", "long_name": "forecast initialization time"}
        cube["member"].attrs.setdefault("long_name", "ensemble member")
        for name in ("time", "valid_time"):
            cube[name].encoding = dict(TIME_ENCODING)
        if grid == "raw":
            st = load_statics(product, store)
            for d in ("yh", "xh"):
                assert np.array_equal(st[d].values, cube[d].values), f"static {d} != data {d}"
            cube = cube.assign(st)
        cubes[grid] = cube
        truth[grid] = {(r["init"], r["var"]): r["truth"] for r in results if r["grid"] == grid and r["truth"]}
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


def iter_native_init(product: str, grid: str, src, only: str | None = None):
    """Forecast init 202510, one array at a time (memory: a whole init is ~10 GB): yields (name, array) with
    array (member=10, step, y, x) float32, member 6 = NaN, fill -> NaN."""
    import h5py
    import obstore as obs
    for p in list_nc(src, product, grid):
        info = parse_name(p)
        if not info or info[1] != NATIVE_INIT or (only is not None and info[0] != only):
            continue
        f = h5py.File(Reader(obs.open_reader(src, p, buffer_size=1024 * 1024)), "r")
        members = [int(m) for m in f["member"][:]]
        for name in (info[0], f"{info[0]}_anom"):
            a = f[name][...]  # (lead, 9, y, x)
            a = np.where(np.abs(a.astype("float64")) > 1e30, np.nan, a).astype("float32")
            full = np.full((10, a.shape[0], *a.shape[2:]), np.nan, dtype="float32")
            for k, m in enumerate(members):
                full[m - 1] = a[:, k]
            del a
            yield name, full


def write_native_init(store, product: str, grids, src) -> None:
    """Write the 202510 slot (time index 0) as native chunks into the virtual arrays, via xarray `to_zarr(region=)`,
    one variable per call."""
    import xarray as xr
    for grid in grids:
        g = xr.open_zarr(store, group=grid, consolidated=False, zarr_format=3, decode_times=False)
        n = 0
        for name, a in iter_native_init(product, grid, src):
            ds = xr.Dataset({name: (g[name].dims, a[None])})  # (time=1, member, step, y, x)
            ds.to_zarr(store, group=grid, mode="r+", region={"time": slice(0, 1)}, consolidated=False, zarr_format=3)
            n += 1
        print(f"  [{grid}] wrote native init {NATIVE_INIT}: {n} arrays", flush=True)


def write(store, cubes, product: str, src) -> None:
    for grid, cube in cubes.items():
        accessor = getattr(cube, "vz", None) or cube.virtualize
        accessor.to_icechunk(store, group=grid)
        patch_group_metadata(store, grid)


def validate(store, cubes, truth, product: str, src, skip_windows: bool = False) -> None:
    """(1) sha1 of the same source chunk windows from the written store; (2) native init 202510 == source."""
    import zarr
    transpose = cfg_of(product)["transpose"]
    for grid, cube in cubes.items():
        g = zarr.open_group(store, mode="r", path=grid)
        times = [f"{t:%Y%m}" for t in pd.DatetimeIndex(cube.time.values)]
        tindex = {t: k for k, t in enumerate(times)}
        items = [(init, var, name, idx, h) for (init, var), tr in truth[grid].items()
                 for name, per in tr.items() for idx, h in per.items()]
        if skip_windows:
            items = []
        print(f"  [{grid}] validating {len(items)} chunk windows", flush=True)

        def check(item) -> None:
            init, var, name, idx, want = item
            arr = g[name]
            if transpose:  # source order (lead, member, y, x) -> store order (member, step, y, x)
                chunks = (arr.chunks[2], arr.chunks[1], *arr.chunks[3:])
                shape = (arr.shape[2], arr.shape[1], *arr.shape[3:])
                sl = chunk_slices(shape, chunks, idx)
                sl = (sl[1], sl[0], *sl[2:])
            else:
                sl = chunk_slices(arr.shape[1:], arr.chunks[1:], idx)
            for attempt in range(4):
                try:
                    a = arr[(tindex[init], *sl)]
                    break
                except Exception as exc:  # NODD drops reads occasionally
                    if attempt == 3:
                        raise
                    print(f"  [{grid}] {init}/{name}: transient read error, retry ({type(exc).__name__})", flush=True)
            if transpose:
                a = np.transpose(a, (1, 0, *range(2, a.ndim)))
            assert sha(a) == want, (grid, init, name, idx)

        with cf.ThreadPoolExecutor(16) as pool:
            for n, _ in enumerate(pool.map(check, items), 1):
                if n % 500 == 0:
                    print(f"  [{grid}] checked {n}/{len(items)}", flush=True)
        print(f"  [{grid}] OK: {len(items)} source chunk windows match", flush=True)
        if product == "forecast":
            k = tindex[NATIVE_INIT]
            n = 0
            for name, want in iter_native_init(product, grid, src):
                got = g[name][k]
                assert np.array_equal(got, want, equal_nan=True), (grid, name)
                assert np.isnan(got[5]).all(), (grid, name, "member 6 not all NaN")
                n += 1
            print(f"  [{grid}] OK: native init {NATIVE_INIT} ({n} arrays) == source; member 6 all NaN", flush=True)


# --------------------------------------------------------------------------- #
# Remote (Coiled) write + validation: workers receive a pickled read-only store / forked write session
# --------------------------------------------------------------------------- #

def native_write_task(fork, product: str, grid: str, var: str):
    import xarray as xr
    src = source_store()
    g = xr.open_zarr(fork.store, group=grid, consolidated=False, zarr_format=3, decode_times=False)
    for name, a in iter_native_init(product, grid, src, only=var):
        xr.Dataset({name: (g[name].dims, a[None])}).to_zarr(
            fork.store, group=grid, mode="r+", region={"time": slice(0, 1)}, consolidated=False, zarr_format=3)
    return fork


def window_task(store, product: str, grid: str, tindex: dict, items: list) -> list:
    """Hash source-chunk windows from the written store; return the failures."""
    import zarr
    transpose = cfg_of(product)["transpose"]
    g = zarr.open_group(store, mode="r", path=grid)
    bad = []
    for init, var, name, idx, want in items:
        arr = g[name]
        if transpose:
            chunks = (arr.chunks[2], arr.chunks[1], *arr.chunks[3:])
            shape = (arr.shape[2], arr.shape[1], *arr.shape[3:])
            sl = chunk_slices(shape, chunks, idx)
            sl = (sl[1], sl[0], *sl[2:])
        else:
            sl = chunk_slices(arr.shape[1:], arr.chunks[1:], idx)
        for attempt in range(4):
            try:
                a = arr[(tindex[init], *sl)]
                break
            except Exception:  # noqa: BLE001  (NODD drops reads occasionally)
                if attempt == 3:
                    raise
        if transpose:
            a = np.transpose(a, (1, 0, *range(2, a.ndim)))
        if sha(a) != want:
            bad.append((grid, init, name, idx))
    return bad


def native_check_task(store, product: str, grid: str, var: str) -> list:
    import zarr
    g = zarr.open_group(store, mode="r", path=grid)
    src = source_store()
    bad = []
    for name, want in iter_native_init(product, grid, src, only=var):
        got = g[name][0]
        if not np.array_equal(got, want, equal_nan=True) or not np.isnan(got[5]).all():
            bad.append((grid, name))
    return bad


class remote_cluster:
    def __init__(self, product: str):
        self.product = product

    def __enter__(self):
        import coiled
        from distributed import Client
        ensure_software_env()
        n = cfg_of(self.product)["n_workers"]
        self.cluster = coiled.Cluster(
            name=f"cefi-seasonal-{self.product}-finish", workspace=WORKSPACE, software=ENV_NAME, n_workers=n,
            region=REGION, spot_policy="spot_with_fallback", worker_cpu=2, worker_memory="8GiB",
            worker_options={"nthreads": 1}, scheduler_vm_types=["m6i.large"], idle_timeout="10 minutes",
            shutdown_on_close=True)
        print(f"  [coiled] cluster up: {self.cluster.name} ({n} workers)", flush=True)
        self.client = Client(self.cluster)
        return self.client

    def __exit__(self, *exc):
        self.cluster.close()
        print("  [coiled] cluster closed", flush=True)


def fill_task(path: str, var: str) -> list:
    """Is the NaN fill override lossless? Per array: HDF5-level fill, chunks written vs expected, stored |x| > 1e30."""
    import h5py
    import obstore as obs
    store = source_store()
    f = h5py.File(Reader(obs.open_reader(store, path, buffer_size=256 * 1024)), "r")
    out = []
    for name in (var, f"{var}_anom"):
        d = f[name]
        expected = int(np.prod([-(-n // c) for n, c in zip(d.shape, d.chunks)]))
        a = d[...].astype("float64")
        out.append((path.rsplit("/", 1)[1], name, float(np.asarray(d.fillvalue).ravel()[0]), int(d.id.get_num_chunks()),
                    expected, int((np.abs(a) > 1e30).sum()), int(np.isnan(a).sum()), int(a.size)))
    return out


def remote_native_write(repo, product: str, grids, client) -> None:
    """Forecast init 202510 as native chunks, written by workers into forked sessions, merged and committed per grid."""
    for grid in grids:
        cube_vars = sorted(BASE_VARS[grid])
        session = repo.writable_session("main")
        futs = [client.submit(native_write_task, session.fork(), product, grid, v, pure=False) for v in cube_vars]
        forks = [f.result() for f in futs]
        session.merge(*forks)
        print(f"  [{grid}] committed {session.commit(f'native chunks for init {NATIVE_INIT} (member 6 = NaN): {grid}')} "
              f"({len(forks)} variables written on workers)", flush=True)


BASE_VARS: dict = {}


def zarr_group_arrays(repo, grid: str):
    import zarr
    return [k for k, _ in zarr.open_group(repo.readonly_session(branch="main").store, mode="r", path=grid).arrays()]


def remote_validate(repo, cubes, truth, product: str, client, native: bool) -> None:
    store = repo.readonly_session(branch="main").store
    for grid, cube in cubes.items():
        times = [f"{t:%Y%m}" for t in pd.DatetimeIndex(cube.time.values)]
        tindex = {t: k for k, t in enumerate(times)}
        items = [(init, var, name, idx, h) for (init, var), tr in truth[grid].items()
                 for name, per in tr.items() for idx, h in per.items()]
        batches = [items[i:i + 40] for i in range(0, len(items), 40)]
        futs = [client.submit(window_task, store, product, grid, tindex, b, pure=False) for b in batches]
        bad = [x for f in futs for x in f.result()]
        assert not bad, f"[{grid}] {len(bad)} chunk windows differ: {bad[:5]}"
        print(f"  [{grid}] OK: {len(items)} source chunk windows match the committed store ({len(batches)} tasks)", flush=True)
        if native:
            vars_ = sorted(v for v in BASE_VARS[grid])
            bad = [x for f in [client.submit(native_check_task, store, product, grid, v, pure=False) for v in vars_]
                   for x in f.result()]
            assert not bad, f"[{grid}] native init differs: {bad[:5]}"
            print(f"  [{grid}] OK: native init {NATIVE_INIT} ({2 * len(vars_)} arrays) == source; member 6 all NaN", flush=True)


def ensure_org_setup(client) -> None:
    if SOURCE_NICKNAME not in {b.nickname for b in client.list_bucket_configs(org=ORG)}:
        raise SystemExit(f"bucket config {SOURCE_NICKNAME!r} missing")
    assert any(p.url_prefix.rstrip("/") == SOURCE_URL and p.subprefix == "" for p in
               client.list_virtual_chunk_access_policies(ORG)), "VCAP missing"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--product", choices=list(PRODUCTS), required=True)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--parse", action="store_true")
    g.add_argument("--reparse-failed", action="store_true")
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--write", action="store_true")
    g.add_argument("--fillcheck", action="store_true", help="on Coiled: HDF5 fill / unwritten chunks / stored 9.97e36 scan")
    g.add_argument("--finish", action="store_true",
                   help="resume on an existing repo: native init for grids not yet committed, then remote validation")
    ap.add_argument("--local", action="store_true", help="--parse in a local process pool, not Coiled")
    ap.add_argument("--skip-windows", action="store_true", help="--dry-run only: skip the chunk-window hashes")
    args = ap.parse_args()
    product = args.product
    c = cfg_of(product)

    if args.parse:
        phase_parse(product, args.local)
        return 0
    if args.fillcheck:
        store = source_store()
        jobs = [(p, parse_name(p)[0]) for g in GRIDS for p in list_nc(store, product, g) if parse_name(p)]
        rows = []
        with remote_cluster(product) as dclient:
            for fut in dclient.map(fill_task, *zip(*jobs), pure=False):
                rows.extend(fut.result())
        df = pd.DataFrame(rows, columns=["file", "array", "hdf_fill", "nchunks", "expected", "hits", "nans", "size"])
        df["grid"] = np.where(df.file.str.contains(".raw."), "raw", "regrid")
        for g, d in df.groupby("grid"):
            print(f"  [{g}] {len(d)} arrays | HDF5 fill NaN: {int(np.isnan(d.hdf_fill).sum())}, 9.97e36: {int((~np.isnan(d.hdf_fill)).sum())} | "
                  f"arrays with unwritten chunks: {int((d.nchunks != d.expected).sum())} | stored |x|>1e30: {int(d.hits.sum())} | NaN fraction {d.nans.sum() / d['size'].sum():.3f}")
        return 0
    with open(cache_path(product), "rb") as fh:
        results = pickle.load(fh)
    if args.reparse_failed:
        bad = [r for r in results if "error" in r]
        print(f"re-parsing {len(bad)} failed file(s) locally", flush=True)
        fixed = {r["path"]: parse_task(r["path"], product, r["grid"], r["init"], r["var"]) for r in bad}
        save_cache(product, [fixed.get(r["path"], r) for r in results])
        return 0

    import icechunk as ic
    print(f"loaded {len(results)} cached parse results", flush=True)
    assert not [r for r in results if "error" in r], "cache contains failed files; rerun --parse / --reparse-failed"
    cubes, truth = build_cubes(product, results)
    src = source_store()
    for grid, cube in cubes.items():  # base variables (with a time, member, step axis) of each grid
        BASE_VARS[grid] = sorted(v for v in cube.data_vars if not v.endswith("_anom")
                                 and {"time", "member", "step"} <= set(cube[v].dims))

    if args.dry_run:
        cfg = ic.RepositoryConfig.default()
        cfg.set_virtual_chunk_container(ic.VirtualChunkContainer(f"{SOURCE_URL}/", ic.s3_store(region=REGION, anonymous=True)))
        mem = ic.Repository.create(
            ic.in_memory_storage(), config=cfg,
            authorize_virtual_chunk_access=ic.containers_credentials({f"{SOURCE_URL}/": ic.s3_anonymous_credentials()}))
        s = mem.writable_session("main")
        write(s.store, cubes, product, src)
        s.commit("virtual part")
        if product == "forecast":
            for grid in GRIDS:  # one commit per grid: a failure loses at most one grid's native write
                s = mem.writable_session("main")
                write_native_init(s.store, product, [grid], src)
                s.commit(f"native init {NATIVE_INIT}: {grid}")
        validate(mem.readonly_session("main").store, cubes, truth, product, src, args.skip_windows)
        print("\n[dry-run] nothing written to Arraylake")
        return 0

    if args.finish:
        from arraylake import Client
        client = Client()
        ensure_org_setup(client)
        repo = client.get_repo(c["repo"])
        done = {a.message.rsplit(": ", 1)[-1] for a in repo.ancestry(branch="main") if a.message.startswith("native chunks")}
        todo = [g for g in GRIDS if g not in done]
        print(f"native init already committed for: {sorted(done)}; to do: {todo}", flush=True)
        with remote_cluster(product) as dclient:
            if product == "forecast" and todo:
                remote_native_write(repo, product, todo, dclient)
            remote_validate(repo, cubes, truth, product, dclient, native=product == "forecast")
        return 0

    from arraylake import Client
    client = Client()
    ensure_org_setup(client)
    name = c["repo"].split("/")[1]
    if name in {r.name for r in client.list_repos(ORG)}:
        print(f"repo {c['repo']} already exists; refusing to overwrite", file=sys.stderr)
        return 1
    repo = client.create_repo(c["repo"], bucket_config_nickname=NICKNAME, description=c["desc"],
                              authorize_virtual_chunk_access={SOURCE_URL: SOURCE_NICKNAME})
    print(f"created repo {c['repo']}", flush=True)
    session = repo.writable_session("main")
    write(session.store, cubes, product, src)
    snap = session.commit(f"FMRC layout (raw, regrid): (time, member, step, ...) virtual cubes from the {c['release']} "
                          "NetCDFs incl. *_anom; integer lead-month step + 2-D valid_time; fill unified to NaN")
    print(f"committed {snap} to main", flush=True)
    client.modify_repo(c["repo"], update_metadata={
        "model": "MOM6-COBALT", "region": "northwest_atlantic", "experiment": c["exp"],
        "experiment_kind": "forecast", "frequency": "monthly", "release": c["release"], "license": "CC-BY-4.0"})
    with remote_cluster(product) as dclient:
        if product == "forecast":
            remote_native_write(repo, product, GRIDS, dclient)
        remote_validate(repo, cubes, truth, product, dclient, native=product == "forecast")
    return 0


if __name__ == "__main__":
    sys.exit(main())
