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
"""NEP hindcast daily (r20260701), 16 full-period two-dimensional variables -> `noaa-cefi/cefi-nep-hindcast-daily`.

Virtual, parsed directly from the NetCDF4 files. One file per variable covers the whole period (12,053 days,
1993-01-01..2025-12-31): no concatenation. Flat groups (user rule 2026-10-23: `main` only where siblings exist):

    raw     (time 12053, jh 816, ih 342)   16 variables + native geolon/geolat/_c, time_bnds, average_T1/T2/DT + statics
    regrid  (time 12053, lat 815, lon 341) 16 variables

  NOT ingested (user decision 2026-10-20): all per-year 4-D variables (P2: needs variable-length chunks virtually, or a
  ~6.5 TB rewrite natively). Only files named `*.199301-202512.nc` of the r20260701 release directory are used.
  bundled coordinates: raw files carry geolon/geolat (jh, ih) and geolon_c/geolat_c (jq, iq): read natively from the
              files, verified identical across all 16 files and, where unmasked, equal to the product's ocean_static.nc.
  time extras (raw): time_bnds, average_T1/T2 (decoded datetimes), average_DT: native, once, after verifying identical.
  fill value: set per ARRAY to the sentinel the data really stores; one sentinel per array is asserted.
  statics (raw): deptho, areacello, wet from this release's own netCDF3 ocean_static.nc (read with scipy; same dim
              names/shapes as the data), 1e20 -> NaN; their land/sea mask is asserted equal to where the data has values.

Phases: --parse (Coiled, resumable per-file cache) | --dry-run | --write | --validate. Source files are never modified.
Run long phases under `caffeinate -i`.
"""
import argparse
import concurrent.futures as cf
import hashlib
import io
import itertools
import pickle
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ORG = "noaa-cefi"
REPO = f"{ORG}/cefi-nep-hindcast-daily"
BUCKET = "noaa-oar-cefi-regional-mom6-pds"
SOURCE_URL = f"s3://{BUCKET}"
NICKNAME = BUCKET
SOURCE_NICKNAME = f"{BUCKET}-source"
REGION = "us-east-1"
RELEASE = "r20260701"
PREFIX = "northeast_pacific/full_domain/hindcast/daily"
GRIDS = ("raw", "regrid")
CACHE = Path(__file__).resolve().parent / ".cache" / "nep_hindcast_daily"  # one pickle per file: incremental + resumable
PARSE_DEADLINE_S = 45 * 60
BUNDLED_COORDS = ("geolon", "geolat", "geolon_c", "geolat_c")  # in every raw file
OWN_STATICS = ("deptho", "areacello", "wet")  # from this release's ocean_static.nc
TIME_EXTRAS = ("time_bnds", "average_T1", "average_T2", "average_DT")
LOADABLE = ["time", "nv", *TIME_EXTRAS, "ih", "jh", "iq", "jq", "lat", "lon", *BUNDLED_COORDS]
N_WINDOWS = 150  # random chunk windows hashed per file (plus corner chunks)
WORKSPACE = "earthmover-devs"
ENV_NAME = "cefi-ingest-2"
N_WORKERS = 12
DROP_GROUP_ATTRS = ("coordinates", "title", "cefi_filename", "cefi_ori_filename", "cefi_variable", "associated_files")
LONLAT = {"geolon": "longitude", "geolat": "latitude"}
DEGREE_UNITS = {"degrees_east", "degree_east", "degrees_north", "degree_north"}
DESC = ("NOAA CEFI MOM6 Northeast Pacific hindcast (daily, r20260701): virtual references over the NODD NetCDF files; "
        "raw + regrid, 16 two-dimensional variables over the full period (per-year 4-D variables not included).")
assert len(DESC) <= 255


def source_store():
    import obstore as obs
    return obs.store.S3Store(BUCKET, region=REGION, skip_signature=True)


def list_nc(store, grid: str) -> list[str]:
    pre = f"{PREFIX}/{grid}/{RELEASE}/"
    return sorted(o["path"] for b in store.list(prefix=pre) for o in b
                  if o["path"].endswith(".199301-202512.nc") and not o["path"].endswith("_static.nc"))


def var_of(path: str) -> str:
    return path.rsplit("/", 1)[1].split(".")[0]


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


def sha(a) -> str:
    return hashlib.sha1(np.ascontiguousarray(a, dtype="<f4").tobytes()).hexdigest()


def hash_f8(a) -> str:
    return hashlib.sha1(np.ascontiguousarray(a, dtype="<f8").tobytes()).hexdigest()


def parse_task(path: str, grid: str, var: str) -> dict:
    import traceback
    try:
        return _parse_file(path, grid, var)
    except Exception as exc:  # noqa: BLE001
        return {"path": path, "grid": grid, "var": var, "error": f"{type(exc).__name__}: {str(exc)[:300]}",
                "trace": traceback.format_exc()[-1500:]}


def _parse_file(path: str, grid: str, var: str) -> dict:
    import h5py
    import obstore as obs
    import virtualizarr as vz
    from obspec_utils.registry import ObjectStoreRegistry
    from virtualizarr.parsers import HDFParser

    store = source_store()
    reg = ObjectStoreRegistry({SOURCE_URL: store})
    f = h5py.File(Reader(obs.open_reader(store, path, buffer_size=1024 * 1024)), "r")
    d = f[var]
    assert d.chunks is not None and d.shape[0] > 0, f"{path}: unchunked or empty"
    bundled = sorted(k for k, v in f.items() if k != var and getattr(v, "ndim", 0) >= 3)
    assert not bundled, f"{path}: unexpected bundled data arrays {bundled}"
    ds = vz.open_virtual_dataset(f"{SOURCE_URL}/{path}", registry=reg, parser=HDFParser(),
                                 loadable_variables=LOADABLE, decode_times=False)
    attr_fill = d.attrs.get("_FillValue")
    attr_fill = None if attr_fill is None else float(np.asarray(attr_fill).ravel()[0])
    hdf_fill = float(np.asarray(d.fillvalue).ravel()[0])
    grid_shape = tuple(-(-n // c) for n, c in zip(d.shape, d.chunks))
    expected = int(np.prod(grid_shape))
    chk = {"shape": d.shape, "chunks": d.chunks, "dims": ds[var].dims, "attr_fill": attr_fill, "hdf_fill": hdf_fill,
           "nchunks": len(ds[var].data.manifest.dict()), "expected_chunks": expected,
           "time_units": ds["time"].attrs.get("units"), "time_calendar": ds["time"].attrs.get("calendar"),
           "time_hash": hash_f8(ds["time"].values), "n_time": int(ds["time"].size)}
    for c in ("ih", "jh", "iq", "jq", "lat", "lon", "nv", *BUNDLED_COORDS):
        if c in ds.variables:
            chk[f"coord_{c}"] = hash_f8(ds[c].values)
    chk["extras"] = {n: {"hash": hash_f8(ds[n].values), "units": ds[n].attrs.get("units"), "shape": ds[n].shape}
                     for n in TIME_EXTRAS if n in ds}
    rng = np.random.default_rng(int(hashlib.sha1(f"{grid}/{var}".encode()).hexdigest()[:8], 16))
    idxs = {tuple(0 for _ in grid_shape), tuple(n - 1 for n in grid_shape)}
    while len(idxs) < min(N_WINDOWS + 2, expected):
        idxs.add(tuple(int(rng.integers(n)) for n in grid_shape))
    truth, cls = {}, {"nan": 0, "attr": 0, "hdf": 0, "other": 0, "valid": 0}
    for idx in sorted(idxs):
        a = d[chunk_slices(d.shape, d.chunks, idx)]
        truth[idx] = sha(a)
        nan = np.isnan(a)
        at = (a == np.float32(attr_fill)) if attr_fill is not None and not np.isnan(attr_fill) else np.zeros(a.shape, bool)
        hd = (a == np.float32(hdf_fill)) if not np.isnan(hdf_fill) else np.zeros(a.shape, bool)
        big = np.abs(np.nan_to_num(a, nan=0.0)) > 1e15
        cls["nan"] += int(nan.sum())
        cls["attr"] += int(at.sum())
        cls["hdf"] += int((hd & ~at).sum())
        cls["other"] += int((big & ~at & ~hd).sum())
        cls["valid"] += int((~nan & ~big).sum())
    chk["classes"] = cls
    return {"path": path, "grid": grid, "var": var, "ds": ds, "chk": chk, "truth": truth}


# ------------------------------- Coiled ------------------------------------ #

def ensure_software_env() -> None:
    import coiled
    import dask
    import distributed
    if ENV_NAME in coiled.list_software_environments(workspace=WORKSPACE):
        print(f"  [coiled] software env {ENV_NAME!r} exists in {WORKSPACE}", flush=True)
        return
    py = f"{sys.version_info.major}.{sys.version_info.minor}"
    pip = ["virtualizarr[hdf] @ git+https://github.com/zarr-developers/VirtualiZarr.git@de4e669f18e323b2814f0625785dbd5766d65652",
           "icechunk>=1.2", "obstore>=0.5", "obspec-utils>=0.2", "xarray", "zarr>=3", "numpy", "pandas", "h5py",
           "h5netcdf", "scipy", "arraylake>=0.16", f"distributed=={distributed.__version__}", f"dask=={dask.__version__}"]
    coiled.create_software_environment(
        name=ENV_NAME, workspace=WORKSPACE,
        conda={"channels": ["conda-forge"], "dependencies": [f"python={py}", "pip", {"pip": pip}]})


class remote_cluster:
    def __init__(self, label: str, n: int = N_WORKERS):
        self.label, self.n = label, n

    def __enter__(self):
        import coiled
        from distributed import Client
        ensure_software_env()
        self.cluster = coiled.Cluster(
            name=f"cefi-nep-daily-{self.label}", workspace=WORKSPACE, software=ENV_NAME, n_workers=self.n,
            region=REGION, spot_policy="spot_with_fallback", worker_cpu=2, worker_memory="8GiB",
            worker_options={"nthreads": 1}, scheduler_vm_types=["m6i.xlarge"], idle_timeout="10 minutes",
            shutdown_on_close=True)
        print(f"  [coiled] cluster up: {self.cluster.name} ({self.n} workers)", flush=True)
        self.client = Client(self.cluster)
        return self.client

    def __exit__(self, *exc):
        self.cluster.close()
        print("  [coiled] cluster closed", flush=True)


def _cache_file(grid: str, var: str) -> Path:
    return CACHE / f"{grid}__{var}.pkl"


def load_results() -> list[dict]:
    out = []
    for f in sorted(CACHE.glob("*.pkl")):
        with open(f, "rb") as fh:
            out.append(pickle.load(fh))
    return out


def phase_parse(local: bool) -> None:
    """Resumable: files already cached (without error) are skipped; every result is saved the moment it arrives; a failed
    or cancelled task is recorded as an error instead of aborting the run."""
    store = source_store()
    CACHE.mkdir(parents=True, exist_ok=True)
    all_jobs = [(p, g, var_of(p)) for g in GRIDS for p in list_nc(store, g)]
    done = set()
    for f in CACHE.glob("*.pkl"):
        with open(f, "rb") as fh:
            r = pickle.load(fh)
        if "error" not in r:
            done.add((r["grid"], r["var"]))
    jobs = [j for j in all_jobs if (j[1], j[2]) not in done]
    print(f"[parse] {len(all_jobs)} files, {len(done)} already cached, {len(jobs)} to do ({'local' if local else 'Coiled'})", flush=True)
    t = time.time()
    n_ok = n_bad = 0

    def save(r: dict) -> None:
        nonlocal n_ok, n_bad
        with open(_cache_file(r["grid"], r["var"]), "wb") as fh:
            pickle.dump(r, fh)
        if "error" in r:
            n_bad += 1
            print(f"  FAILED {r['path']}: {r['error']}", flush=True)
        else:
            n_ok += 1
        if (n_ok + n_bad) % 10 == 0 or (n_ok + n_bad) == len(jobs):
            print(f"  [parse] {n_ok + n_bad}/{len(jobs)} ({n_bad} failed) ({time.time() - t:.0f}s)", flush=True)

    if jobs and local:
        with cf.ProcessPoolExecutor(4) as pool:
            futs = {pool.submit(parse_task, *j): j for j in jobs}
            for fut in cf.as_completed(futs):
                save(fut.result())
    elif jobs:
        from distributed import as_completed
        with remote_cluster("parse") as client:
            futs = {client.submit(parse_task, *j, retries=1, pure=False): j for j in jobs}
            try:
                for fut in as_completed(list(futs), timeout=PARSE_DEADLINE_S):
                    try:
                        save(fut.result())
                    except Exception as exc:  # noqa: BLE001  (e.g. a cancelled task)
                        p, g, v = futs[fut]
                        save({"path": p, "grid": g, "var": v, "error": f"{type(exc).__name__}: {str(exc)[:300]}"})
            except Exception as exc:  # noqa: BLE001  (deadline: stop, keep what we have, close the cluster)
                print(f"  DEADLINE ({PARSE_DEADLINE_S}s) reached or connection lost: {type(exc).__name__}; "
                      f"{n_ok + n_bad}/{len(jobs)} done. Re-run --parse to resume.", flush=True)
    bad = [r for r in load_results() if "error" in r]
    print(f"[parse] {len(all_jobs) - len(bad)} cached OK, {len(bad)} failed -> {CACHE} ({time.time() - t:.0f}s)", flush=True)
    if bad:
        sys.exit(2)


# ------------------------------- build ------------------------------------- #

def with_fill(ma, value: float):
    from virtualizarr.manifests import ManifestArray
    from zarr.core.metadata.v3 import ArrayV3Metadata
    d = ma.metadata.to_dict()
    d["fill_value"] = "NaN" if np.isnan(value) else float(np.float32(value))
    return ManifestArray(metadata=ArrayV3Metadata.from_dict(d), chunkmanifest=ma.manifest)


def array_fill(r: dict) -> tuple[str, float]:
    """The sentinel this array really stores for 'no data' (exactly one), and its value."""
    c, k = r["chk"], r["chk"]["classes"]
    assert c["nchunks"] == c["expected_chunks"], f"{r['path']}: unwritten chunks ({c['nchunks']} of {c['expected_chunks']})"
    assert k["other"] == 0, f"{r['path']}: stored values that are neither valid nor the fill: {k}"
    kinds = [kk for kk in ("nan", "attr", "hdf") if k[kk]]
    assert len(kinds) <= 1, f"{r['path']}: more than one 'no data' sentinel stored: {k}"
    kind = kinds[0] if kinds else ("nan" if np.isnan(c["attr_fill"] if c["attr_fill"] is not None else np.nan) else "attr")
    value = {"attr": c["attr_fill"], "hdf": c["hdf_fill"], "nan": float("nan")}[kind]
    return kind, float(value)


def decode_days(base_units: str, values) -> np.ndarray:
    base = pd.Timestamp(base_units.split("since ")[1].strip().split()[0])
    v = np.asarray(values, "float64")
    return (base + pd.to_timedelta(v.ravel(), unit="D")).values.reshape(v.shape)


def load_statics(store):
    """deptho/areacello/wet from this release's own netCDF3 ocean_static.nc (same dim names as the data); 1e20 -> NaN."""
    import obstore as obs
    import xarray as xr
    from scipy.io import netcdf_file
    own = netcdf_file(io.BytesIO(bytes(obs.get(store, f"{PREFIX}/raw/{RELEASE}/ocean_static.nc").bytes())), mmap=False)
    out = xr.Dataset()
    report = {}
    for n in OWN_STATICS:
        v = own.variables[n]
        a = np.array(v[:], dtype="float32")
        report[n] = int((np.abs(a) > 1e19).sum() + np.isnan(a).sum())
        a = np.where(np.abs(a) > 1e19, np.float32(np.nan), a)
        attrs = {k: (getattr(v, k).decode() if isinstance(getattr(v, k), bytes) else getattr(v, k))
                 for k in ("long_name", "units") if hasattr(v, k)}
        attrs["comment"] = "from this release's ocean_static.nc (netCDF3); 1e20 land fill stored as NaN"
        out[n] = (tuple(v.dimensions), a, attrs)
    geo = {n: np.array(own.variables[n][:], dtype="float32") for n in ("geolon", "geolat")}
    print(f"  [statics] masked/NaN cells in this release's ocean_static.nc: {report}; dims {dict(own.dimensions)}", flush=True)
    return out, geo


def mask_check(st, results: list[dict]) -> None:
    """Where does the data have values, compared with the static's land/sea mask? Must agree exactly."""
    import h5py
    import obstore as obs
    r = next(x for x in results if x["grid"] == "raw" and x["var"] == "tos")
    f = h5py.File(Reader(obs.open_reader(source_store(), r["path"], buffer_size=1024 * 1024)), "r")
    a = f["tos"][0]
    valid = np.isfinite(a) & (np.abs(np.nan_to_num(a)) < 1e19)
    wet = st["wet"].values
    ocean = np.isfinite(wet) & (wet == 1)
    dep = st["deptho"].values
    ocean_d = np.isfinite(dep) & (dep > 0)
    bad = {"wet: data but not ocean": int((valid & ~ocean).sum()), "wet: ocean but no data": int((ocean & ~valid).sum()),
           "deptho: data but no depth": int((valid & ~ocean_d).sum()), "deptho: depth but no data": int((ocean_d & ~valid).sum())}
    print(f"  [mask] tos day 1: {int(valid.sum())} cells with data; static wet==1 on {int(ocean.sum())}, deptho valid on "
          f"{int(ocean_d.sum())}; mismatches {bad}", flush=True)
    assert not any(bad.values()), f"the static's land/sea mask does not match the data: {bad}"


def build_cubes(results: list[dict]):
    import virtualizarr  # noqa: F401
    import xarray as xr
    from virtualizarr.manifests import ManifestArray
    store = source_store()
    cubes, truth = {}, {}
    st, static_geo = load_statics(store)  # fail fast
    mask_check(st, results)
    for grid in GRIDS:
        rs = sorted((r for r in results if r["grid"] == grid), key=lambda r: r["var"])
        assert len(rs) == 16, f"[{grid}] expected 16 variables, got {len(rs)}"
        keys = ["time_hash", "time_units", "time_calendar", "n_time", *sorted({k for r in rs for k in r["chk"] if k.startswith("coord_")})]
        for key in keys:
            vals = {r["chk"].get(key) for r in rs}
            assert len(vals) == 1, f"[{grid}] {key} differs between files: {len(vals)} distinct"
        assert rs[0]["chk"]["n_time"] == 12053 and rs[0]["chk"]["time_calendar"] == "gregorian"
        fills = {r["var"]: array_fill(r) for r in rs}
        kinds = {k: sum(1 for kk, _ in fills.values() if kk == k) for k in ("nan", "attr", "hdf")}
        tot = {k: sum(r["chk"]["classes"][k] for r in rs) for k in ("nan", "attr", "hdf", "other", "valid")}
        print(f"  [{grid}] {len(rs)} variables | sentinel per array {kinds} (non-NaN values {sorted({v for k, v in fills.values() if k != 'nan'})}) | "
              f"sampled values {tot} | every chunk written in every file | coordinates, time axis identical across files", flush=True)
        extras = {}
        if grid == "raw":
            for n in TIME_EXTRAS:
                hs = {(r["chk"]["extras"][n]["hash"], r["chk"]["extras"][n]["units"]) for r in rs}
                assert len(hs) == 1, f"raw {n} differs between files"
            extras = rs[0]["chk"]["extras"]
        merged = xr.merge([r["ds"] for r in rs], compat="override", join="exact", combine_attrs="drop_conflicts")
        cube = merged.copy()
        names = {r["var"] for r in rs}
        for n in [v for v in merged.data_vars if v in names]:
            v = merged[n]
            cube[n] = xr.Variable(v.dims, with_fill(v.data, fills[n][1]), dict(v.attrs), dict(v.encoding))
        virtual_left = [n for n in cube.variables if n not in names and isinstance(cube[n].variable._data, ManifestArray)]
        assert not virtual_left, f"[{grid}] non-data variables left virtual: {virtual_left}"
        t = decode_days(rs[0]["chk"]["time_units"], merged["time"].values)
        cube = cube.assign_coords(time=("time", t, {"standard_name": "time", "long_name": "time"}))
        cube["time"].encoding = {"units": "days since 1993-01-01 00:00:00", "calendar": "gregorian", "dtype": "float64"}
        for n in TIME_EXTRAS:
            if n in cube:
                v = cube[n]
                vals = v.values if n == "average_DT" else decode_days(extras[n]["units"], v.values)
                attrs = {k: a for k, a in v.attrs.items() if k not in ("units", "calendar", "calendar_type")}
                if n == "average_DT":
                    attrs["units"] = "days"
                cube[n] = (v.dims, vals, attrs)
                if n != "average_DT":
                    cube[n].encoding = {"units": "days since 1993-01-01 00:00:00", "calendar": "gregorian", "dtype": "float64"}
        if "time_bnds" in cube:
            cube["time"].attrs["bounds"] = "time_bnds"
        else:
            cube["time"].attrs.pop("bounds", None)
        cube.attrs = {k: v for k, v in cube.attrs.items() if k not in DROP_GROUP_ATTRS}
        if grid == "raw":
            # bundled geolon/geolat must agree with the static's wherever the static is unmasked
            for n in ("geolon", "geolat"):
                s_, d_ = static_geo[n], cube[n].values
                ok = np.abs(s_) < 1e19
                assert s_.shape == d_.shape and np.allclose(s_[ok], d_[ok], rtol=0, atol=1e-4), f"{n}: bundled != ocean_static"
                print(f"  [statics] bundled {n} == ocean_static {n} on {int(ok.sum())} of {ok.size} cells (rest masked in the static)", flush=True)
            cube = cube.assign({n: st[n] for n in OWN_STATICS})
        used = {d for n in cube.data_vars for d in cube[n].dims}
        unused = [c for c in cube.coords if c in cube.dims and c not in used]
        if unused:
            cube = cube.drop_vars(unused)
            print(f"    dropped unused dimension coordinates: {unused}", flush=True)
        cubes[grid] = cube
        truth[grid] = {r["var"]: r["truth"] for r in rs}
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


def write(store, cubes) -> None:
    for grid, cube in cubes.items():
        (getattr(cube, "vz", None) or cube.virtualize).to_icechunk(store, group=grid)
        patch_group_metadata(store, grid)


def window_task(store, grid: str, var: str, windows: dict) -> list:
    import zarr
    arr = zarr.open_group(store, mode="r", path=grid)[var]
    bad = []
    for idx, want in windows.items():
        sl = chunk_slices(arr.shape, arr.chunks, idx)
        for attempt in range(4):
            try:
                a = arr[sl]
                break
            except Exception:  # noqa: BLE001  (NODD drops reads occasionally)
                if attempt == 3:
                    raise
        if sha(a) != want:
            bad.append((grid, var, idx))
    return bad


def remote_validate(repo, truth, client) -> None:
    """Hash the sampled source windows from the committed store on the workers. Small batches, retries, a deadline per
    round and a resubmit of stragglers (one hung NODD read must not stall or kill the whole validation)."""
    from distributed import wait
    store = repo.readonly_session(branch="main").store
    pending = []
    for group, per in truth.items():
        for var, w in per.items():
            items = sorted(w.items())
            for i in range(0, len(items), 40):
                pending.append((group, var, dict(items[i:i + 40])))
    total = len(pending)
    bad: list = []
    for rnd in range(3):
        if not pending:
            break
        futs = {client.submit(window_task, store, g, v, w, pure=False, retries=2): (g, v, w) for g, v, w in pending}
        done, not_done = wait(list(futs), timeout=600)
        pending = []
        for f in done:
            if f.status == "error":
                pending.append(futs[f])  # retry in the next round
            else:
                bad.extend(f.result())
        for f in not_done:
            pending.append(futs[f])
            f.cancel()
        print(f"  [validate] round {rnd + 1}: {len(done) - sum(1 for f in done if f.status == 'error')}/{len(futs)} batches done, "
              f"{len(pending)} to retry", flush=True)
    assert not pending, f"{len(pending)} validation batches never completed"
    assert not bad, f"{len(bad)} chunk windows differ: {bad[:5]}"
    n = sum(len(w) for per in truth.values() for w in per.values())
    print(f"  OK: {n} source chunk windows ({total} batches, {sum(len(per) for per in truth.values())} arrays) match the "
          "committed store", flush=True)


def ensure_org_setup(client) -> None:
    if SOURCE_NICKNAME not in {b.nickname for b in client.list_bucket_configs(org=ORG)}:
        raise SystemExit(f"bucket config {SOURCE_NICKNAME!r} missing")
    assert any(p.url_prefix.rstrip("/") == SOURCE_URL and p.subprefix == "" for p in
               client.list_virtual_chunk_access_policies(ORG)), "VCAP missing"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--parse", action="store_true")
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--write", action="store_true")
    g.add_argument("--validate", action="store_true", help="re-validate the existing repo on Coiled")
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()
    if args.parse:
        phase_parse(args.local)
        return 0
    results = load_results()
    assert results and not [r for r in results if "error" in r], "cache empty or contains failed files; rerun --parse (resumable)"
    print(f"loaded {len(results)} cached parse results", flush=True)
    cubes, truth = build_cubes(results)
    import icechunk as ic
    if args.dry_run:
        cfg = ic.RepositoryConfig.default()
        cfg.set_virtual_chunk_container(ic.VirtualChunkContainer(f"{SOURCE_URL}/", ic.s3_store(region=REGION, anonymous=True)))
        mem = ic.Repository.create(
            ic.in_memory_storage(), config=cfg,
            authorize_virtual_chunk_access=ic.containers_credentials({f"{SOURCE_URL}/": ic.s3_anonymous_credentials()}))
        s = mem.writable_session("main")
        write(s.store, cubes)
        s.commit("dry run")
        import xarray as xr
        for grid in GRIDS:
            ds = xr.open_zarr(mem.readonly_session("main").store, group=grid, consolidated=False, zarr_format=3)
            print(f"\n--- {grid}\n{ds}", flush=True)
        print("\n[dry-run] nothing written to Arraylake")
        return 0
    if args.validate:
        from arraylake import Client
        repo = Client().get_repo(REPO)
        with remote_cluster("validate") as dclient:
            remote_validate(repo, truth, dclient)
        return 0
    from arraylake import Client
    client = Client()
    ensure_org_setup(client)
    if REPO.split("/")[1] in {r.name for r in client.list_repos(ORG)}:
        print(f"repo {REPO} already exists; refusing to overwrite", file=sys.stderr)
        return 1
    repo = client.create_repo(REPO, bucket_config_nickname=NICKNAME, description=DESC,
                              authorize_virtual_chunk_access={SOURCE_URL: SOURCE_NICKNAME})
    print(f"created repo {REPO}", flush=True)
    session = repo.writable_session("main")
    write(session.store, cubes)
    snap = session.commit(f"Virtual references (raw, regrid) over the {RELEASE} NetCDFs: one array per variable over the "
                          "full period; zarr fill_value set to the stored sentinel; time extras native; raw statics")
    print(f"committed {snap} to main", flush=True)
    client.modify_repo(REPO, update_metadata={
        "model": "MOM6-COBALT", "region": "northeast_pacific", "experiment": "hindcast", "experiment_kind": "hindcast",
        "frequency": "daily", "release": RELEASE, "license": "CC-BY-4.0"})
    with remote_cluster("validate") as dclient:
        remote_validate(repo, truth, dclient)
    return 0


if __name__ == "__main__":
    sys.exit(main())
