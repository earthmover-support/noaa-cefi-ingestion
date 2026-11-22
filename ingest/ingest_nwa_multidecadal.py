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
"""NWA multi-decadal outlook (r20260331) -> `noaa-cefi/cefi-nwa-multi-decadal-outlook-yearly`.

Scenario-stacked virtual cube (NOT a forecast-run collection: the SSPs are climate scenarios on a shared time axis).
One source file per (variable, scenario); the four scenarios are stacked on a new leading `scenario` axis (chunk 1).
Groups (user decisions 2026-10-22/23):

    raw/main    (scenario 4, time 131, [z_l 52], yh 845, xh 775)    8 variables + native time_bnds + statics
    raw/u_grid  (scenario 4, time 131, [z_l], yh 845, xq 776)       5 variables (T_adx included)
    raw/v_grid  (scenario 4, time 131, [z_l], yq 846, xh 775)       6 variables
    regrid      (scenario 4, time 131, [z_l], lat 844, lon 774)     8 variables (flat: one group)

  scenario   native string coordinate ["SSP126","SSP245","SSP370","SSP585"]; time = datetime64 yearly (mid-year
             1970-07-02 .. 2100-07-02, 131 steps) with native time_bnds (raw).
  P6         raw `T_adx` SSP585 has only 130 steps (year 2045 missing). Per the user's decision (2026-10-22) its slot enters
             the virtual stack as a placeholder (empty manifest) and is written as NATIVE chunks (region write by Coiled
             workers into forked sessions), the 130 real years at their correct time positions, 2045 = NaN.
  fill       per array, the sentinel the data really stores (raw: 1e20, regrid: NaN); one sentinel per array is asserted.
  statics    this release ships none: raw geolon/geolat/_c from the NWA hindcast monthly; deptho/areacello/wet from the NWA
             decadal-forecast static (see below);  (originally planned: all from the NWA hindcast monthly
             r20250715 ocean_static.nc (HYBRID, user decision 2026-10-26): coordinates from there, deptho/areacello/wet from
             the NWA decadal-forecast r20250819 static, whose land/sea mask matches this data exactly (the hindcast mask is
             wrong at 288 cells); the mask is asserted equal to where the data has values.

Phases: --parse (Coiled, resumable per-file cache) | --dry-run | --write | --finish (native T_adx SSP585 + validation on
Coiled for an existing repo) | --validate. Source files are never modified. Run long phases under `caffeinate -i`.
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
from pathlib import Path

import numpy as np
import pandas as pd

ORG = "noaa-cefi"
REPO = f"{ORG}/cefi-nwa-multi-decadal-outlook-yearly"
BUCKET = "noaa-oar-cefi-regional-mom6-pds"
SOURCE_URL = f"s3://{BUCKET}"
NICKNAME = BUCKET
SOURCE_NICKNAME = f"{BUCKET}-source"
REGION = "us-east-1"
RELEASE = "r20260331"
PREFIX = "northwest_atlantic/full_domain/multi_decadal_outlook/yearly"
HINDCAST_STATIC = "northwest_atlantic/full_domain/hindcast/monthly/raw/r20250715/ocean_static.nc"
GRIDS = ("raw", "regrid")
SCENARIOS = ["SSP126", "SSP245", "SSP370", "SSP585"]
NSTEP = 131
SPECIAL = ("raw/u_grid", "T_adx", "SSP585")  # (group, variable, scenario) written natively (P6)
SPECIAL_SLOT = SCENARIOS.index("SSP585")
CACHE = Path(__file__).resolve().parent / ".cache" / "nwa_multidecadal"
CORE_STATICS = ("geolon", "geolat", "deptho", "areacello", "wet", "geolon_c", "geolat_c")
COORD_STATICS = ("geolon", "geolat", "geolon_c", "geolat_c")
LOADABLE = ["time", "time_bnds", "bnds", "xh", "yh", "xq", "yq", "lat", "lon", "z_l"]
N_WINDOWS_4D = 100
WORKSPACE = "earthmover-devs"
ENV_NAME = "cefi-ingest-2"
N_WORKERS = 12
PARSE_DEADLINE_S = 45 * 60
DROP_GROUP_ATTRS = ("coordinates", "title", "cefi_filename", "cefi_ori_filename", "cefi_variable", "associated_files")
LONLAT = {"geolon": "longitude", "geolat": "latitude"}
DEGREE_UNITS = {"degrees_east", "degree_east", "degrees_north", "degree_north"}
DESC = ("NOAA CEFI MOM6 NW Atlantic multi-decadal outlook (yearly, r20260331): virtual references stacked on a `scenario` "
        "axis (SSP126/245/370/585); raw + regrid. T_adx SSP585 is native chunks (year 2045 = NaN).")
assert len(DESC) <= 255, len(DESC)


def source_store():
    import obstore as obs
    return obs.store.S3Store(BUCKET, region=REGION, skip_signature=True)


def list_nc(store, grid: str) -> list[str]:
    pre = f"{PREFIX}/{grid}/{RELEASE}/"
    return sorted(o["path"] for b in store.list(prefix=pre) for o in b if o["path"].endswith(".nc"))


def var_of(path: str) -> str:
    return path.rsplit("/", 1)[1].split(".")[0]


def scenario_of(path: str) -> str:
    return path.rsplit("/", 1)[1].split(".")[-3]


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


def group_of(grid: str, dims: tuple) -> str:
    if grid == "regrid":
        return "regrid"
    h = tuple(d for d in dims if d in ("yh", "yq", "xh", "xq"))
    return {("yh", "xh"): "raw/main", ("yh", "xq"): "raw/u_grid", ("yq", "xh"): "raw/v_grid"}[h]


# ------------------------------- parse (workers) ------------------------------- #

def parse_task(path: str, grid: str, var: str, scenario: str) -> dict:
    import traceback
    try:
        return _parse_file(path, grid, var, scenario)
    except Exception as exc:  # noqa: BLE001
        return {"path": path, "grid": grid, "var": var, "scenario": scenario,
                "error": f"{type(exc).__name__}: {str(exc)[:300]}", "trace": traceback.format_exc()[-1500:]}


def _parse_file(path: str, grid: str, var: str, scenario: str) -> dict:
    import h5py
    import obstore as obs
    import virtualizarr as vz
    from obspec_utils.registry import ObjectStoreRegistry
    from virtualizarr.parsers import HDFParser

    store = source_store()
    reg = ObjectStoreRegistry({SOURCE_URL: store})
    f = h5py.File(Reader(obs.open_reader(store, path, buffer_size=1024 * 1024)), "r")
    d = f[var]
    assert d.chunks is not None, f"{path}: unchunked"
    bundled = sorted(k for k, v in f.items() if k != var and getattr(v, "ndim", 0) >= 3)
    ds = vz.open_virtual_dataset(f"{SOURCE_URL}/{path}", registry=reg, parser=HDFParser(drop_variables=bundled or None),
                                 loadable_variables=LOADABLE, decode_times=False)
    attr_fill = d.attrs.get("_FillValue")
    attr_fill = None if attr_fill is None else float(np.asarray(attr_fill).ravel()[0])
    hdf_fill = float(np.asarray(d.fillvalue).ravel()[0])
    grid_shape = tuple(-(-n // c) for n, c in zip(d.shape, d.chunks))
    expected = int(np.prod(grid_shape))
    special = (var, scenario) == (SPECIAL[1], SPECIAL[2]) and grid == "raw"
    chk = {"shape": d.shape, "chunks": d.chunks, "dims": ds[var].dims, "attr_fill": attr_fill, "hdf_fill": hdf_fill,
           "nchunks": len(ds[var].data.manifest.dict()), "expected_chunks": expected, "bundled_dropped": bundled,
           "time_units": ds["time"].attrs.get("units"), "time_calendar": ds["time"].attrs.get("calendar"),
           "time_hash": hash_f8(ds["time"].values), "n_time": int(ds["time"].size), "special": special,
           "time_values": np.asarray(ds["time"].values, "float64") if special else None}
    for c in ("xh", "yh", "xq", "yq", "lat", "lon", "z_l"):
        if c in ds:
            chk[f"coord_{c}"] = hash_f8(ds[c].values)
    if "time_bnds" in ds:
        chk["bnds_hash"] = hash_f8(ds["time_bnds"].values)
        chk["bnds_units"] = ds["time_bnds"].attrs.get("units")
        chk["bnds_values"] = np.asarray(ds["time_bnds"].values, "float64") if special else None
    # windows: every chunk for small grids, else corners + random; hash + classify every stored value
    rng = np.random.default_rng(int(hashlib.sha1(f"{grid}/{var}/{scenario}".encode()).hexdigest()[:8], 16))
    if expected <= 60:
        idxs = set(itertools.product(*[range(n) for n in grid_shape]))
    else:
        idxs = {tuple(0 for _ in grid_shape), tuple(n - 1 for n in grid_shape)}
        while len(idxs) < N_WINDOWS_4D + 2:
            idxs.add(tuple(int(rng.integers(n)) for n in grid_shape))
    truth, cls = {}, {"nan": 0, "attr": 0, "hdf": 0, "other": 0, "valid": 0}
    for idx in sorted(idxs):
        a = d[chunk_slices(d.shape, d.chunks, idx)]
        if not special:
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
    return {"path": path, "grid": grid, "var": var, "scenario": scenario, "ds": ds, "chk": chk, "truth": truth}


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
            name=f"cefi-multidecadal-{self.label}", workspace=WORKSPACE, software=ENV_NAME, n_workers=self.n,
            region=REGION, spot_policy="spot_with_fallback", worker_cpu=2, worker_memory="8GiB",
            worker_options={"nthreads": 1}, scheduler_vm_types=["m6i.xlarge"], idle_timeout="10 minutes",
            shutdown_on_close=True)
        print(f"  [coiled] cluster up: {self.cluster.name} ({self.n} workers)", flush=True)
        self.client = Client(self.cluster)
        return self.client

    def __exit__(self, *exc):
        self.cluster.close()
        print("  [coiled] cluster closed", flush=True)


def _cache_file(grid: str, var: str, scenario: str) -> Path:
    return CACHE / f"{grid}__{var}__{scenario}.pkl"


def load_results() -> list[dict]:
    out = []
    for f in sorted(CACHE.glob("*.pkl")):
        with open(f, "rb") as fh:
            out.append(pickle.load(fh))
    return out


def phase_parse(local: bool) -> None:
    """Resumable; each result is saved the moment it arrives; failures become error records; hard deadline."""
    store = source_store()
    CACHE.mkdir(parents=True, exist_ok=True)
    all_jobs = [(p, g, var_of(p), scenario_of(p)) for g in GRIDS for p in list_nc(store, g)]
    done = set()
    for f in CACHE.glob("*.pkl"):
        with open(f, "rb") as fh:
            r = pickle.load(fh)
        if "error" not in r:
            done.add((r["grid"], r["var"], r["scenario"]))
    jobs = [j for j in all_jobs if (j[1], j[2], j[3]) not in done]
    print(f"[parse] {len(all_jobs)} files, {len(done)} already cached, {len(jobs)} to do ({'local' if local else 'Coiled'})", flush=True)
    t = time.time()
    n_ok = n_bad = 0

    def save(r: dict) -> None:
        nonlocal n_ok, n_bad
        with open(_cache_file(r["grid"], r["var"], r["scenario"]), "wb") as fh:
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
            for fut in cf.as_completed([pool.submit(parse_task, *j) for j in jobs]):
                save(fut.result())
    elif jobs:
        from distributed import as_completed
        with remote_cluster("parse") as client:
            futs = {client.submit(parse_task, *j, retries=1, pure=False): j for j in jobs}
            try:
                for fut in as_completed(list(futs), timeout=PARSE_DEADLINE_S):
                    try:
                        save(fut.result())
                    except Exception as exc:  # noqa: BLE001
                        p, g, v, s = futs[fut]
                        save({"path": p, "grid": g, "var": v, "scenario": s, "error": f"{type(exc).__name__}: {str(exc)[:300]}"})
            except Exception as exc:  # noqa: BLE001
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


def empty_like(ma):
    from virtualizarr.manifests import ChunkManifest, ManifestArray
    return ManifestArray(metadata=ma.metadata, chunkmanifest=ChunkManifest({}, shape=ma.manifest.shape_chunk_grid))


def array_fill(r: dict) -> tuple[str, float]:
    c, k = r["chk"], r["chk"]["classes"]
    assert c["nchunks"] == c["expected_chunks"], f"{r['path']}: unwritten chunks ({c['nchunks']} of {c['expected_chunks']})"
    assert k["other"] == 0, f"{r['path']}: stored values that are neither valid nor the fill: {k}"
    kinds = [kk for kk in ("nan", "attr", "hdf") if k[kk]]
    assert len(kinds) <= 1, f"{r['path']}: more than one 'no data' sentinel stored: {k}"
    kind = kinds[0] if kinds else ("nan" if c["attr_fill"] is None or np.isnan(c["attr_fill"]) else "attr")
    value = {"attr": c["attr_fill"], "hdf": c["hdf_fill"], "nan": float("nan")}[kind]
    return kind, float(value)


def decode_days(base_units: str, values) -> np.ndarray:
    base = pd.Timestamp(base_units.split("since ")[1].strip().split()[0])
    v = np.asarray(values, "float64")
    return (base + pd.to_timedelta(v.ravel(), unit="D")).values.reshape(v.shape)


def nativize_coords(cube, rs: list[dict], verify: int = 3):
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
        for path in carriers[: verify + 1]:
            f = h5py.File(Reader(obs.open_reader(store, path, buffer_size=256 * 1024)), "r")
            v = np.asarray(f[name][...], "float64")
            if vals is None:
                vals = v
                assert vals.shape == cube[name].shape, (name, vals.shape, cube[name].shape)
            else:
                assert np.array_equal(vals, v), f"coordinate {name} differs between {carriers[0]} and {path}"
        cube = cube.assign_coords({name: (cube[name].dims, vals, dict(cube[name].attrs))})
        print(f"    native coordinate {name} ({vals.size} values) read from {carriers[0].rsplit('/', 1)[1]}", flush=True)
    return cube


DECADAL_STATIC = "northwest_atlantic/full_domain/decadal_forecast/yearly/raw/r20250819/ocean_static.nc"
OWN_FROM_DECADAL = ("deptho", "areacello", "wet")  # the decadal model's land/sea mask = this release's (see mask_check)


def load_statics(store):
    """This release ships no static file (user decision 2026-10-26, hybrid like the decadal repos):
    geolon/geolat/geolon_c/geolat_c from the NWA hindcast monthly static (gap-free); deptho/areacello/wet from the NWA
    decadal-forecast r20250819 ocean_static.nc, whose land/sea mask matches this release's data exactly."""
    import h5py
    import obstore as obs
    import xarray as xr
    from scipy.io import netcdf_file
    hc = netcdf_file(io.BytesIO(bytes(obs.get(store, HINDCAST_STATIC).bytes())), mmap=False)
    dec = h5py.File(io.BytesIO(bytes(obs.get(store, DECADAL_STATIC).bytes())), "r")
    for d in ("xh", "yh", "xq", "yq"):  # both statics describe the same grid
        assert np.array_equal(np.asarray(hc.variables[d][:], "float64"), np.asarray(dec[d][:], "float64")), d
    out = xr.Dataset()
    for n in COORD_STATICS:
        v = hc.variables[n]
        a = np.array(v[:], dtype="float32")
        assert np.isfinite(a).all() and (np.abs(a) < 1e19).all(), f"hindcast static {n} has gaps"
        o = np.array(dec[n][:], dtype="float32")
        valid = np.abs(o) < 1e19
        assert np.array_equal(o[valid], a[valid]), f"{n}: decadal static != hindcast static where the decadal one is unmasked"
        attrs = {k: (getattr(v, k).decode() if isinstance(getattr(v, k), bytes) else getattr(v, k))
                 for k in ("long_name", "units") if hasattr(v, k)}
        attrs["comment"] = ("taken from the NWA hindcast monthly r20250715 ocean_static.nc (this release ships no static file; "
                            "gap-free, identical to the decadal-forecast static wherever that is unmasked)")
        out[n] = (v.dimensions, a, attrs)
    for n in OWN_FROM_DECADAL:
        d = dec[n]
        a = np.array(d[:], dtype="float32")
        a = np.where(np.abs(a) > 1e19, np.float32(np.nan), a)  # 1e20 'no data' -> NaN, as in the other repos' statics
        dims = ("yh", "xh")
        attrs = {k: (d.attrs[k].decode() if isinstance(d.attrs[k], bytes) else d.attrs[k]) for k in ("long_name", "units") if k in d.attrs}
        attrs["comment"] = ("taken from the NWA decadal-forecast r20250819 ocean_static.nc: this release ships no static file and "
                            "that model's land/sea mask matches this release's data exactly (the hindcast mask differs at 288 cells)")
        out[n] = (dims, a, attrs)
    for d in ("xh", "yh", "xq", "yq"):
        out = out.assign_coords({d: (d, np.asarray(hc.variables[d][:], "float64"), {})})
    return out


def mask_check(st, results: list[dict]) -> None:
    """Where does the data have values, compared with the borrowed hindcast land/sea mask (wet)? Report, never hide."""
    import h5py
    import obstore as obs
    r = next(x for x in results if x["grid"] == "raw" and x["var"] == "tos" and x["scenario"] == "SSP126")
    f = h5py.File(Reader(obs.open_reader(source_store(), r["path"], buffer_size=1024 * 1024)), "r")
    a = f["tos"][0]  # first year
    valid = np.isfinite(a) & (np.abs(np.nan_to_num(a)) < 1e19)
    wet = st["wet"].values
    ocean = np.isfinite(wet) & (np.abs(np.nan_to_num(wet)) < 1e19) & (wet == 1)
    dep = st["deptho"].values
    ocean_d = np.isfinite(dep) & (dep > 0)
    bad = {"wet: data but not ocean": int((valid & ~ocean).sum()), "wet: ocean but no data": int((ocean & ~valid).sum()),
           "deptho: data but no depth": int((valid & ~ocean_d).sum()), "deptho: depth but no data": int((ocean_d & ~valid).sum())}
    print(f"  [mask] tos SSP126 year 1: {int(valid.sum())} cells with data; borrowed statics: wet==1 on {int(ocean.sum())} cells, "
          f"deptho valid on {int(ocean_d.sum())}; mismatches {bad}", flush=True)
    assert not any(bad.values()), f"the statics' land/sea mask does not match the data: {bad}"


def build_cubes(results: list[dict]):
    import virtualizarr  # noqa: F401
    import xarray as xr
    store = source_store()
    by_key = {(r["grid"], r["var"], r["scenario"]): r for r in results}
    # ---- consistency of the shared axes (step 5)
    normal = [r for r in results if not r["chk"]["special"]]
    for key in ("time_hash", "time_units", "time_calendar", "n_time", "bnds_hash", "bnds_units"):
        for grid in GRIDS:
            vals = {r["chk"].get(key) for r in normal if r["grid"] == grid and r["chk"].get(key) is not None}
            assert len(vals) <= 1, f"[{grid}] {key} differs between files: {vals}"
    assert {r["chk"]["n_time"] for r in normal} == {NSTEP}, "unexpected time length in a non-special file"
    for key in ("coord_xh", "coord_yh", "coord_xq", "coord_yq", "coord_lat", "coord_lon", "coord_z_l"):
        for grid in GRIDS:
            vals = {r["chk"][key] for r in results if r["grid"] == grid and key in r["chk"]}
            assert len(vals) <= 1, f"[{grid}] {key} differs between files: {len(vals)} distinct"
    sp = by_key[("raw", SPECIAL[1], SPECIAL[2])]["chk"]
    ref_time = np.asarray(by_key[("raw", "tos", "SSP126")]["ds"]["time"].values, "float64")
    miss = sorted(set(range(NSTEP)) - {int(np.argmin(np.abs(ref_time - v))) for v in sp["time_values"]})
    assert sp["n_time"] == NSTEP - 1 and len(miss) == 1, f"T_adx SSP585 is not '131 minus one step': {sp['n_time']}, missing {miss}"
    assert all(np.isin(sp["time_values"], ref_time)), "T_adx SSP585 time values are not a subset of the common axis"
    missing_year = pd.Timestamp(decode_days(sp["time_units"], ref_time[miss])[0]).year
    print(f"  [P6] T_adx SSP585: {sp['n_time']} steps; the only missing step is index {miss[0]} (year {missing_year}); "
          "all other time values equal the common axis", flush=True)
    assert miss == [75] and missing_year == 2045, "unexpected missing step"
    print(f"  common time axis: {NSTEP} steps, identical in {len(normal)} files per the hash checks; "
          f"time_bnds identical across files that carry them", flush=True)
    st = load_statics(store)
    mask_check(st, results)

    # ---- group files
    groups: dict[str, dict[str, dict[str, dict]]] = {}
    for r in results:
        groups.setdefault(group_of(r["grid"], r["chk"]["dims"]), {}).setdefault(r["var"], {})[r["scenario"]] = r
    cubes, truth = {}, {}
    t_axis = decode_days(by_key[("raw", "tos", "SSP126")]["chk"]["time_units"], ref_time)
    for group in ("raw/main", "raw/u_grid", "raw/v_grid", "regrid"):
        vars_ = groups[group]
        for v, per in vars_.items():
            assert sorted(per) == sorted(SCENARIOS), f"[{group}] {v} lacks scenarios: {sorted(per)}"
        fills, kinds = {}, {"nan": 0, "attr": 0, "hdf": 0}
        for v, per in vars_.items():
            f4 = {s: array_fill(r) for s, r in per.items()}
            assert len({(k, (None if np.isnan(x) else x)) for k, x in f4.values()}) == 1, f"[{group}] {v}: scenarios disagree on the sentinel: {f4}"
            fills[v] = f4[SCENARIOS[0]]
            kinds[fills[v][0]] += 1
        print(f"  [{group}] {len(vars_)} variables x {len(SCENARIOS)} scenarios | sentinel per variable {kinds} "
              f"(values {sorted({x for k, x in fills.values() if k != 'nan'})}) | every chunk written in every file", flush=True)
        per_scen = []
        for s in SCENARIOS:
            parts = []
            for v, per in vars_.items():
                r = per[s]
                if (group, v, s) == SPECIAL:  # placeholder for the native slot: SSP126's metadata, EMPTY manifest
                    r = per["SSP126"]
                ds = r["ds"]
                x = ds[v]
                ma = x.data
                if (group, v, s) == SPECIAL:
                    ma = empty_like(ma)
                parts.append(ds.assign({v: xr.Variable(x.dims, with_fill(ma, fills[v][1]), dict(x.attrs), dict(x.encoding))}).drop_vars(
                    [n for n in ds.data_vars if n != v and n != "time_bnds"]))
            per_scen.append(xr.merge(parts, compat="override", join="exact", combine_attrs="drop_conflicts"))
        cube = xr.concat(per_scen, dim=pd.Index(SCENARIOS, name="scenario"), coords="minimal", compat="override",
                         combine_attrs="drop_conflicts", join="exact")
        if group != "raw/main" and "time_bnds" in cube:
            cube = cube.drop_vars("time_bnds")
        if group != "raw/main" and "bnds" in cube.coords:
            cube = cube.drop_vars("bnds")
        cube = cube.assign_coords(time=("time", t_axis, {"standard_name": "time", "long_name": "time"}))
        cube["time"].encoding = {"units": "days since 1900-01-01 00:00:00", "calendar": "gregorian", "dtype": "float64"}
        cube["scenario"].attrs = {"long_name": "SSP scenario"}
        if "time_bnds" in cube:
            tb = cube["time_bnds"]
            vals = decode_days(by_key[("raw", "tos", "SSP126")]["chk"]["time_units"], tb.isel(scenario=0).values) if tb.ndim == 3 else decode_days(
                by_key[("raw", "tos", "SSP126")]["chk"]["time_units"], tb.values)
            cube["time_bnds"] = (tb.dims[1:] if tb.ndim == 3 else tb.dims, vals,
                                 {k: a for k, a in tb.attrs.items() if k not in ("units", "calendar")})
            cube["time_bnds"].encoding = {"units": "days since 1900-01-01 00:00:00", "calendar": "gregorian", "dtype": "float64"}
            cube["time"].attrs["bounds"] = "time_bnds"
        else:
            cube["time"].attrs.pop("bounds", None)
        cube.attrs = {k: v for k, v in cube.attrs.items() if k not in DROP_GROUP_ATTRS}
        some = [r for per in vars_.values() for r in per.values()]
        cube = nativize_coords(cube, some)
        if group == "raw/main":
            for d in ("yh", "xh"):
                assert np.array_equal(st[d].values, cube[d].values), f"static {d} != data {d}"
            cube = cube.assign({n: st[n] for n in CORE_STATICS}).assign_coords(xq=st["xq"], yq=st["yq"])
        if group == "raw/u_grid":
            assert np.array_equal(st["xq"].values, cube["xq"].values) and np.array_equal(st["yh"].values, cube["yh"].values)
        if group == "raw/v_grid":
            assert np.array_equal(st["yq"].values, cube["yq"].values) and np.array_equal(st["xh"].values, cube["xh"].values)
        used = {d for n in cube.data_vars for d in cube[n].dims}
        unused = [c for c in cube.coords if c in cube.dims and c not in used]
        if unused:
            cube = cube.drop_vars(unused)
            print(f"    dropped unused dimension coordinates: {unused}", flush=True)
        cubes[group] = cube
        truth[group] = {v: {s: r["truth"] for s, r in per.items() if r["truth"]} for v, per in vars_.items()}
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


def write_groups(repo_or_mem, cubes, commit_each: bool):
    for group, cube in cubes.items():
        session = repo_or_mem.writable_session("main")
        (getattr(cube, "vz", None) or cube.virtualize).to_icechunk(session.store, group=group)
        patch_group_metadata(session.store, group)
        print(f"committed {session.commit(f'Virtual references: {group} ({len(cube.data_vars)} arrays)')} to main ({group})", flush=True)


# ------------------------------- native T_adx SSP585 + validation (workers) ------------------------------- #

def special_path() -> str:
    return f"{PREFIX}/raw/{RELEASE}/T_adx.nwa.full.multi_decade.yearly.raw.r20260331.SSP585.1970-2100.nc"


def native_blocks(shape_t: int = NSTEP, shape_z: int = 52, tb: int = 22, zb: int = 8):
    return [(t0, min(t0 + tb, shape_t), z0, min(z0 + zb, shape_z)) for t0 in range(0, shape_t, tb) for z0 in range(0, shape_z, zb)]


def _source_rows(ref_time: np.ndarray, src_time: np.ndarray, t0: int, t1: int):
    """For global time positions t0..t1-1, the source row (or -1 when the source has no such step, i.e. 2045)."""
    pos = {int(np.argmin(np.abs(ref_time - v))): j for j, v in enumerate(src_time)}
    return [pos.get(g, -1) for g in range(t0, t1)]


def native_block_task(fork, block: tuple, ref_time: np.ndarray, src_time: np.ndarray):
    """Read one (time x z) block of the SSP585 T_adx source and region-write it at its true time positions (2045 = NaN)."""
    import h5py
    import obstore as obs
    import xarray as xr
    t0, t1, z0, z1 = block
    store = source_store()
    f = h5py.File(Reader(obs.open_reader(store, special_path(), buffer_size=4 * 1024 * 1024)), "r")
    d = f["T_adx"]
    rows = _source_rows(ref_time, src_time, t0, t1)
    out = np.full((t1 - t0, z1 - z0, d.shape[2], d.shape[3]), np.nan, dtype="float32")
    present = [(i, j) for i, j in enumerate(rows) if j >= 0]
    lo, hi = min(j for _, j in present), max(j for _, j in present) + 1
    slab = d[lo:hi, z0:z1]
    for i, j in present:
        out[i] = slab[j - lo]
    g = xr.open_zarr(fork.store, group=SPECIAL[0], consolidated=False, zarr_format=3, decode_times=False)
    ds = xr.Dataset({"T_adx": (g["T_adx"].dims, out[None])})  # (scenario=1, time, z_l, yh, xq)
    ds.to_zarr(fork.store, group=SPECIAL[0], mode="r+",
               region={"scenario": slice(SPECIAL_SLOT, SPECIAL_SLOT + 1), "time": slice(t0, t1), "z_l": slice(z0, z1)},
               consolidated=False, zarr_format=3)
    return fork


def native_check_task(store, block: tuple, ref_time: np.ndarray, src_time: np.ndarray) -> list:
    """Compare one block of the committed store with the source; the missing year must be all NaN."""
    import h5py
    import obstore as obs
    import zarr
    t0, t1, z0, z1 = block
    s = source_store()
    d = h5py.File(Reader(obs.open_reader(s, special_path(), buffer_size=4 * 1024 * 1024)), "r")["T_adx"]
    arr = zarr.open_group(store, mode="r", path=SPECIAL[0])["T_adx"]
    fillv = np.float32(arr.fill_value)
    got = arr[SPECIAL_SLOT, t0:t1, z0:z1]
    rows = _source_rows(ref_time, src_time, t0, t1)
    bad = []
    for i, j in enumerate(rows):
        if j < 0:
            # xarray stores NaN as the array's own `_FillValue` (1e20, the same marker land uses); readers decode it to NaN
            if not (np.isnan(got[i]) | (got[i] == fillv)).all():
                bad.append(("missing year has data values", t0 + i, z0))
        else:
            want = d[j, z0:z1]
            if not np.array_equal(got[i], want, equal_nan=True):
                bad.append(("differs from source", t0 + i, z0))
    return bad


def window_task(store, group: str, var: str, s_idx: int, windows: dict) -> list:
    import zarr
    arr = zarr.open_group(store, mode="r", path=group)[var]
    bad = []
    for idx, want in windows.items():
        sl = chunk_slices(arr.shape[1:], arr.chunks[1:], idx)
        for attempt in range(4):
            try:
                a = arr[(s_idx, *sl)]
                break
            except Exception:  # noqa: BLE001
                if attempt == 3:
                    raise
        if sha(a) != want:
            bad.append((group, var, SCENARIOS[s_idx], idx))
    return bad


def remote_native_write(repo, client, ref_time, src_time) -> None:
    session = repo.writable_session("main")
    blocks = native_blocks()
    futs = [client.submit(native_block_task, session.fork(), b, ref_time, src_time, pure=False) for b in blocks]
    forks = [f.result() for f in futs]
    session.merge(*forks)
    print(f"committed {session.commit('Native chunks for T_adx SSP585 (raw/u_grid): 130 source years at their time positions, 2045 = NaN')} "
          f"to main ({len(blocks)} blocks written on workers)", flush=True)


def remote_validate(repo, truth, client, ref_time, src_time) -> None:
    from distributed import wait
    store = repo.readonly_session(branch="main").store
    pending = []
    for group, per_var in truth.items():
        for var, per_s in per_var.items():
            for s, w in per_s.items():
                items = sorted(w.items())
                for i in range(0, len(items), 40):
                    pending.append(("w", group, var, SCENARIOS.index(s), dict(items[i:i + 40])))
    for b in native_blocks():
        pending.append(("n", b))
    total = len(pending)
    bad: list = []
    for rnd in range(3):
        if not pending:
            break
        futs = {}
        for p in pending:
            if p[0] == "w":
                futs[client.submit(window_task, store, p[1], p[2], p[3], p[4], pure=False, retries=2)] = p
            else:
                futs[client.submit(native_check_task, store, p[1], ref_time, src_time, pure=False, retries=2)] = p
        done, not_done = wait(list(futs), timeout=900)
        pending = []
        for f in done:
            if f.status == "error":
                pending.append(futs[f])
            else:
                bad.extend(f.result())
        for f in not_done:
            pending.append(futs[f])
            f.cancel()
        print(f"  [validate] round {rnd + 1}: {len(done) - sum(1 for f in done if f.status == 'error')}/{len(futs)} done, "
              f"{len(pending)} to retry", flush=True)
    assert not pending, f"{len(pending)} validation tasks never completed"
    assert not bad, f"{len(bad)} mismatches: {bad[:5]}"
    nwin = sum(len(w) for pv in truth.values() for ps in pv.values() for w in ps.values())
    print(f"  OK: {nwin} source chunk windows over {sum(len(ps) for pv in truth.values() for ps in pv.values())} (variable, scenario) "
          f"arrays match the committed store; native T_adx SSP585: {len(native_blocks())} blocks == source, year 2045 all NaN", flush=True)


def ensure_org_setup(client) -> None:
    if SOURCE_NICKNAME not in {b.nickname for b in client.list_bucket_configs(org=ORG)}:
        raise SystemExit(f"bucket config {SOURCE_NICKNAME!r} missing")
    assert any(p.url_prefix.rstrip("/") == SOURCE_URL and p.subprefix == "" for p in
               client.list_virtual_chunk_access_policies(ORG)), "VCAP missing"


def axes(results):
    by = {(r["grid"], r["var"], r["scenario"]): r for r in results}
    ref_time = np.asarray(by[("raw", "tos", "SSP126")]["ds"]["time"].values, "float64")
    src_time = by[("raw", SPECIAL[1], SPECIAL[2])]["chk"]["time_values"]
    return ref_time, src_time


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    for n in ("parse", "dry-run", "write", "finish", "validate"):
        g.add_argument(f"--{n}", action="store_true")
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()
    if args.parse:
        phase_parse(args.local)
        return 0
    results = load_results()
    assert results and not [r for r in results if "error" in r], "cache empty or contains failed files; rerun --parse (resumable)"
    print(f"loaded {len(results)} cached parse results", flush=True)
    cubes, truth = build_cubes(results)
    ref_time, src_time = axes(results)
    import icechunk as ic
    if args.dry_run:
        cfg = ic.RepositoryConfig.default()
        cfg.set_virtual_chunk_container(ic.VirtualChunkContainer(f"{SOURCE_URL}/", ic.s3_store(region=REGION, anonymous=True)))
        mem = ic.Repository.create(
            ic.in_memory_storage(), config=cfg,
            authorize_virtual_chunk_access=ic.containers_credentials({f"{SOURCE_URL}/": ic.s3_anonymous_credentials()}))
        write_groups(mem, cubes, True)
        import xarray as xr
        for group in cubes:
            ds = xr.open_zarr(mem.readonly_session("main").store, group=group, consolidated=False, zarr_format=3)
            print(f"\n--- {group}\n{ds}", flush=True)
        print("\n[dry-run] nothing written to Arraylake")
        return 0
    from arraylake import Client
    client = Client()
    ensure_org_setup(client)
    if args.write:
        if REPO.split("/")[1] in {r.name for r in client.list_repos(ORG)}:
            print(f"repo {REPO} already exists; refusing to overwrite", file=sys.stderr)
            return 1
        repo = client.create_repo(REPO, bucket_config_nickname=NICKNAME, description=DESC,
                                  authorize_virtual_chunk_access={SOURCE_URL: SOURCE_NICKNAME})
        print(f"created repo {REPO}", flush=True)
        write_groups(repo, cubes, True)
        client.modify_repo(REPO, update_metadata={
            "model": "MOM6-COBALT", "region": "northwest_atlantic", "experiment": "multi_decadal_outlook",
            "experiment_kind": "projection", "frequency": "yearly", "release": RELEASE, "license": "CC-BY-4.0"})
        args.finish = True
    repo = client.get_repo(REPO)
    done = any(a.message.startswith("Native chunks for T_adx SSP585") for a in repo.ancestry(branch="main"))
    with remote_cluster("finish") as dclient:
        if not args.validate and not done:
            remote_native_write(repo, dclient, ref_time, src_time)
        remote_validate(repo, truth, dclient, ref_time, src_time)
    return 0


if __name__ == "__main__":
    sys.exit(main())
