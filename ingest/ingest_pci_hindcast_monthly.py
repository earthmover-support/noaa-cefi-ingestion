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
"""Pacific Islands hindcast monthly (r20260427; draft product) -> `noaa-cefi/cefi-pci-hindcast-monthly`.

Virtual, parsed directly from the NetCDF4 files. One file per variable covers the whole period (396 months,
1993-01..2025-12), so there is no concatenation. Groups (user decisions 2026-10-23):

    raw/main    (time, [z_l 52 | zi 76], yh 539, xh 725)   401 h-grid variables + native time extras + statics
    raw/u_grid  (time, [z_l], yh 539, xq 726)               8 variables
    raw/v_grid  (time, [z_l], yq 539, xh 725)               9 variables
    regrid      (time, [z_l], lat 538, lon 724)             336 variables (flat: one group)

  excluded (source files with no data): raw `T_adx_2d` and `ffedet_btm` (zero time steps), raw `speed` (no `speed`
              array in the file at all: coordinates and time fields only) and the 66 `volcello` copies bundled
              inside other variables' files (user decision 2026-10-22; the standalone `volcello` file is ingested).
  time extras (raw/main): time_bnds, average_T1/T2 (decoded datetimes), average_DT: native, taken once after
              verifying every raw file that carries them has identical values.
  fill value: set per ARRAY to the sentinel it really stores (VirtualiZarr takes the HDF5-level fill); each array is
              checked to store exactly one 'no data' sentinel.
  coordinates: always native (z_l, zi, xq, yq, ... read from the files); statics: this product's own netCDF4
              ocean_static.nc, asserted identical to the daily product's.

Phases: --parse (Coiled, workspace `earthmover-devs`) | --dry-run | --write (commits per group) | --validate
Source files are never modified.
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
REPO = f"{ORG}/cefi-pci-hindcast-monthly"
BUCKET = "noaa-oar-cefi-regional-mom6-pds"
SOURCE_URL = f"s3://{BUCKET}"
NICKNAME = BUCKET
SOURCE_NICKNAME = f"{BUCKET}-source"
REGION = "us-east-1"
RELEASE = "r20260427"
PREFIX = "pacific_islands/full_domain/hindcast/monthly"
DAILY_STATIC = "pacific_islands/full_domain/hindcast/daily/raw/r20260427/ocean_static.nc"
EXCLUDED = {("raw", "T_adx_2d"), ("raw", "ffedet_btm"), ("raw", "speed")}  # source files with no data (see docstring)
GRIDS = ("raw", "regrid")
CACHE = Path(__file__).resolve().parent / ".cache" / "pci_hindcast_monthly"  # one pickle per file: incremental + resumable
CORE_STATICS = ("geolon", "geolat", "deptho", "areacello", "wet", "geolon_c", "geolat_c")
COORD_STATICS = ("geolon", "geolat", "geolon_c", "geolat_c")
TIME_EXTRAS = ("time_bnds", "average_T1", "average_T2", "average_DT")
LOADABLE = ["time", "nv", *TIME_EXTRAS, "xh", "yh", "xq", "yq", "lat", "lon", "z_l", "z_i"]
N_WINDOWS_2D, N_WINDOWS_4D = 40, 60  # random chunk windows hashed per file (plus corner chunks)
WORKSPACE = "earthmover-devs"
ENV_NAME = "cefi-ingest-2"
N_WORKERS = 24
PARSE_DEADLINE_S = 45 * 60  # hard limit for one parse run (resumable)
DROP_GROUP_ATTRS = ("coordinates", "title", "cefi_filename", "cefi_ori_filename", "cefi_variable", "associated_files")
LONLAT = {"geolon": "longitude", "geolat": "latitude"}
DEGREE_UNITS = {"degrees_east", "degree_east", "degrees_north", "degree_north"}
DESC = ("NOAA CEFI MOM6 Pacific Islands hindcast (monthly, r20260427; draft product): virtual references over "
        "the NODD NetCDF files; raw (main/u_grid/v_grid) + regrid, one array per variable over the full period.")


def source_store():
    import obstore as obs
    return obs.store.S3Store(BUCKET, region=REGION, skip_signature=True)


def list_nc(store, grid: str) -> list[str]:
    pre = f"{PREFIX}/{grid}/{RELEASE}/"
    return sorted(o["path"] for b in store.list(prefix=pre) for o in b
              if o["path"].endswith(".nc") and not o["path"].endswith("_static.nc")
              and (grid, o["path"].rsplit("/", 1)[1].split(".")[0]) not in EXCLUDED)


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
    # other data arrays bundled in this file (e.g. a full 4-D `volcello` copy) are not ingested
    bundled = sorted(k for k, v in f.items() if k != var and getattr(v, "ndim", 0) >= 3)
    ds = vz.open_virtual_dataset(f"{SOURCE_URL}/{path}", registry=reg, parser=HDFParser(drop_variables=bundled or None),
                                 loadable_variables=LOADABLE, decode_times=False)
    assert d.shape[0] > 0 and d.chunks is not None, f"{path}: no time steps / unchunked"
    attr_fill = d.attrs.get("_FillValue")
    attr_fill = None if attr_fill is None else float(np.asarray(attr_fill).ravel()[0])
    hdf_fill = float(np.asarray(d.fillvalue).ravel()[0])
    grid_shape = tuple(-(-n // c) for n, c in zip(d.shape, d.chunks))
    expected = int(np.prod(grid_shape))
    nwritten = len(ds[var].data.manifest.dict())  # chunks VirtualiZarr found (b-tree walk done once, by the parser)
    chk = {"shape": d.shape, "chunks": d.chunks, "dims": ds[var].dims, "attr_fill": attr_fill, "hdf_fill": hdf_fill,
           "nchunks": nwritten, "expected_chunks": expected, "bundled_dropped": bundled,
           "time_units": ds["time"].attrs.get("units"), "time_calendar": ds["time"].attrs.get("calendar"),
           "time_hash": hash_f8(ds["time"].values), "n_time": int(ds["time"].size)}
    for c in ("xh", "yh", "xq", "yq", "lat", "lon", "z_l", "z_i"):
        if c in ds:
            chk[f"coord_{c}"] = hash_f8(ds[c].values)
    chk["extras"] = {n: {"hash": hash_f8(ds[n].values), "units": ds[n].attrs.get("units"), "shape": ds[n].shape}
                     for n in TIME_EXTRAS if n in ds}
    nwin = N_WINDOWS_4D if d.ndim == 4 else N_WINDOWS_2D
    rng = np.random.default_rng(int(hashlib.sha1(f"{grid}/{var}".encode()).hexdigest()[:8], 16))
    idxs = {tuple(0 for _ in grid_shape), tuple(n - 1 for n in grid_shape)}
    while len(idxs) < min(nwin + 2, expected):
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
            name=f"cefi-pci-monthly-{self.label}", workspace=WORKSPACE, software=ENV_NAME, n_workers=self.n,
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


def group_of(grid: str, dims: tuple) -> str:
    if grid == "regrid":
        return "regrid"
    h = tuple(d for d in dims if d in ("yh", "yq", "xh", "xq"))
    return {("yh", "xh"): "raw/main", ("yh", "xq"): "raw/u_grid", ("yq", "xh"): "raw/v_grid"}[h]


def load_statics(reg):
    """Core 5 + geolon_c/geolat_c from this product's own netCDF4 ocean_static.nc (HDF5 parser)."""
    import virtualizarr as vz
    from virtualizarr.parsers import HDFParser
    want = list(CORE_STATICS)
    ds = vz.open_virtual_dataset(f"{SOURCE_URL}/{PREFIX}/raw/{RELEASE}/ocean_static.nc", registry=reg, parser=HDFParser(),
                                 loadable_variables=[*want, "xh", "yh", "xq", "yq"], decode_times=False)[want].load()
    # NB: loading decodes the 1e20 fill to NaN, so "masked" = NaN or 1e20
    masked = {n: int((np.isnan(ds[n].values) | (np.abs(np.nan_to_num(ds[n].values)) > 1e19)).sum()) for n in want}
    print(f"  [statics] masked (1e20) cells per field in this release's own ocean_static.nc: {masked}", flush=True)
    for n in COORD_STATICS:
        assert masked[n] == 0 and np.isfinite(ds[n].values).all(), f"{n} is masked/non-finite in the product's static"
    daily = vz.open_virtual_dataset(f"{SOURCE_URL}/{DAILY_STATIC}", registry=reg, parser=HDFParser(),
                                    loadable_variables=[*want, "xh", "yh", "xq", "yq"], decode_times=False)[want].load()
    same = {n: bool(np.array_equal(ds[n].values, daily[n].values, equal_nan=True)) for n in want}
    assert all(same.values()), f"monthly static differs from the daily static: {same}"
    print("  [statics] identical to the daily product's ocean_static.nc for all 7 fields", flush=True)
    lon, lat = ds["geolon"].values, ds["geolat"].values
    print(f"  [statics] geolon {lon.min():.2f}..{lon.max():.2f}, geolat {lat.min():.2f}..{lat.max():.2f} "
          "(Pacific Islands expected: ~171..220.5 E, 8.9..38.1 N)", flush=True)
    return ds


def nativize_coords(cube, rs: list[dict], verify: int = 3):
    """Coordinates must never be virtual (opening a group would need source reads): any coordinate left as a
    ManifestArray is replaced by its values read straight from a data file that carries it, and cross-checked against
    up to `verify` other files that carry it (they are merged with compat='override', i.e. assumed identical)."""
    import h5py
    import obstore as obs
    from virtualizarr.manifests import ManifestArray
    store = source_store()
    for name in list(cube.coords):
        if not isinstance(cube[name].variable._data, ManifestArray):
            continue
        carriers = [r["path"] for r in rs if name in r["ds"].variables]
        assert carriers, f"no file carries coordinate {name}"
        vals = None
        for k, path in enumerate(carriers[: verify + 1]):
            f = h5py.File(Reader(obs.open_reader(store, path, buffer_size=256 * 1024)), "r")
            v = np.asarray(f[name][...], "float64")
            if vals is None:
                vals = v
                assert vals.shape == cube[name].shape, (name, vals.shape, cube[name].shape)
            else:
                assert np.array_equal(vals, v), f"coordinate {name} differs between {carriers[0]} and {path}"
        cube = cube.assign_coords({name: (cube[name].dims, vals, dict(cube[name].attrs))})
        print(f"    native coordinate {name} ({vals.size} values) read from {carriers[0].rsplit('/', 1)[1]} "
              f"(identical in {min(verify, len(carriers) - 1)} other files)", flush=True)
    return cube


def build_cubes(results: list[dict]):
    import virtualizarr  # noqa: F401
    import xarray as xr
    from obspec_utils.registry import ObjectStoreRegistry
    reg = ObjectStoreRegistry({SOURCE_URL: source_store()})
    cubes, truth = {}, {}
    # per-grid consistency of the shared axes
    for grid in GRIDS:
        rs = [r for r in results if r["grid"] == grid]
        for key in ("time_hash", "time_units", "time_calendar", "n_time"):
            vals = {r["chk"][key] for r in rs}
            assert len(vals) == 1, f"[{grid}] {key} differs between files: {vals}"
        print(f"  [{grid}] {len(rs)} variables, {rs[0]['chk']['n_time']} time steps ({rs[0]['chk']['time_units']}, "
              f"{rs[0]['chk']['time_calendar']})", flush=True)
    time_units = results[0]["chk"]["time_units"]
    by_group: dict[str, list[dict]] = {}
    for r in results:
        by_group.setdefault(group_of(r["grid"], r["chk"]["dims"]), []).append(r)
    # coordinate identity within each coordinate name across all files that carry it
    for key in ("coord_xh", "coord_yh", "coord_xq", "coord_yq", "coord_lat", "coord_lon", "coord_z_l", "coord_z_i"):
        for grid in GRIDS:
            vals = {r["chk"][key] for r in results if r["grid"] == grid and key in r["chk"]}
            assert len(vals) <= 1, f"[{grid}] {key} differs between files"
    all_extras = [r for r in results if r["grid"] == "raw" and r["chk"]["extras"]]
    for n in TIME_EXTRAS:
        hs = {(r["chk"]["extras"][n]["hash"], r["chk"]["extras"][n]["units"]) for r in all_extras if n in r["chk"]["extras"]}
        assert len(hs) == 1, f"raw {n} differs between files that carry it: {len(hs)} distinct"
    print(f"  [raw] time extras {sorted(all_extras[0]['chk']['extras'])} identical across {len(all_extras)} raw files", flush=True)
    extras = all_extras[0]["chk"]["extras"]
    st = load_statics(reg)  # fail fast, before anything slow
    for group in ("raw/main", "raw/u_grid", "raw/v_grid", "regrid"):
        rs = sorted(by_group[group], key=lambda r: r["var"])
        fills = {r["var"]: array_fill(r) for r in rs}
        kinds = {k: sum(1 for kk, _ in fills.values() if kk == k) for k in ("nan", "attr", "hdf")}
        vals = sorted({(kk, v) for kk, v in fills.values() if kk != "nan"})
        tot = {k: sum(r["chk"]["classes"][k] for r in rs) for k in ("nan", "attr", "hdf", "other", "valid")}
        dropped = sum(len(r["chk"]["bundled_dropped"]) for r in rs)
        print(f"  [{group}] {len(rs)} variables | stored 'no data' sentinel per array: {kinds} (non-NaN values {vals}) | "
              f"sampled values {tot} | every chunk written in every file | bundled arrays dropped from these files: {dropped}",
              flush=True)
        merged = xr.merge([r["ds"] for r in rs], compat="override", join="exact", combine_attrs="drop_conflicts")
        cube = merged.copy()
        data_names = {r["var"] for r in rs}
        for n in [v for v in merged.data_vars if v in data_names]:
            v = merged[n]
            cube[n] = xr.Variable(v.dims, with_fill(v.data, fills[n][1]), dict(v.attrs), dict(v.encoding))
        keep_extras = group == "raw/main"
        if not keep_extras and "nv" in cube.coords:
            cube = cube.drop_vars("nv")  # only meaningful with time_bnds
        for n in TIME_EXTRAS:
            if n in cube and not keep_extras:
                cube = cube.drop_vars(n)
        t = decode_days(time_units, merged["time"].values)
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
        cube.attrs = {k: v for k, v in cube.attrs.items() if k not in DROP_GROUP_ATTRS}
        cube = nativize_coords(cube, rs)
        if group == "raw/main":
            for d in ("yh", "xh"):
                assert np.array_equal(st[d].values, cube[d].values), f"static {d} != data {d}"
            cube = cube.assign({n: st[n] for n in CORE_STATICS}).assign_coords(xq=st["xq"], yq=st["yq"])
        if group in ("raw/u_grid", "raw/v_grid"):
            n = "xq" if group == "raw/u_grid" else "yq"
            assert np.array_equal(np.asarray(st[n].values, "float64"), cube[n].values), f"{group}: {n} differs from the static's"
        used = {d for n in cube.data_vars for d in cube[n].dims}
        unused = [c for c in cube.coords if c in cube.dims and c not in used]  # e.g. z_i in groups with no 4-D z_i variable
        if unused:
            cube = cube.drop_vars(unused)
            print(f"    dropped unused dimension coordinates: {unused}", flush=True)
        cubes[group] = cube
        truth[group] = {r["var"]: r["truth"] for r in rs}
        print(f"  [{group}] cube {dict(cube.sizes)}", flush=True)
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
    for group, cube in cubes.items():
        (getattr(cube, "vz", None) or cube.virtualize).to_icechunk(store, group=group)
        patch_group_metadata(store, group)


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
        for group in cubes:
            ds = xr.open_zarr(mem.readonly_session("main").store, group=group, consolidated=False, zarr_format=3)
            print(f"\n--- {group}\n{ds}", flush=True)
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
    for group, cube in cubes.items():  # one commit per group: a failure loses at most one group's write
        session = repo.writable_session("main")
        (getattr(cube, "vz", None) or cube.virtualize).to_icechunk(session.store, group=group)
        patch_group_metadata(session.store, group)
        print(f"committed {session.commit(f'Virtual references: {group} ({len(cube.data_vars)} arrays)')} to main ({group})", flush=True)
    client.modify_repo(REPO, update_metadata={
        "model": "MOM6-COBALT", "region": "pacific_islands", "experiment": "hindcast", "experiment_kind": "hindcast",
        "frequency": "monthly", "release": RELEASE, "license": "CC-BY-4.0"})
    with remote_cluster("validate") as dclient:
        remote_validate(repo, truth, dclient)
    return 0


if __name__ == "__main__":
    sys.exit(main())
