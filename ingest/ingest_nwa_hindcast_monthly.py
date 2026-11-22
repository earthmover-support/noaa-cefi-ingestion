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
"""NWA hindcast monthly (r20250715) -> `noaa-cefi/cefi-nwa-hindcast-monthly`.

Virtual, parsed directly from the NetCDF4 files. One file per variable covers the whole period (372 months,
1993-01..2023-12): no concatenation. Groups (confirmed 2026-10-22; group naming rules 2026-10-23):

    raw/main     (time, [z_l 52 | zi 76], yh 845, xh 775)   427 ocean variables + native time extras + statics
    raw/u_grid   (time, [z_l], yh 845, xq 776)              9 variables
    raw/v_grid   (time, [z_l], yq 846, xh 775)              9 variables
    raw/ice      (time, [ct 5], yT 845, xT 775)             37 sea-ice variables (+ CELL_AREA from ice_static.nc)
    raw/ice_xB   (time, yT 845, xB 776)                     3 variables
    raw/ice_yB   (time, yB 846, xT 775)                     3 variables
    regrid/main  (time, [z_l | zi], lat 844, lon 774)       427 variables, lon grid A (start -98.4423, step 0.0807)
    regrid/ice   (time, [ct], lat 844, lon 774)             37 sea-ice variables, lon grid B (start -98.0, step 0.0801) (P5)

  The 62 bundled `volcello` copies (full 4-D copies inside other variables' files, P3) are not ingested (user decision
  2026-10-22); the standalone `volcello` is.
  fill value: per ARRAY, the sentinel the data really stores (one per array is asserted).
  statics (raw/main): geolon/geolat/geolon_c/geolat_c/deptho/areacello/wet from this release's own netCDF3 ocean_static.nc
              (the data files carry no geolon/geolat); the land/sea mask is asserted equal to where the data has values.
  raw/ice: CELL_AREA from ice_static.nc; geolon/geolat copied from ocean_static ONLY if the data files' yT/xT equal yh/xh.

Phases: --parse (Coiled, resumable per-file cache) | --dry-run | --write (commits per group) | --validate
Source files are never modified. Run long phases under `caffeinate -i`.
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
REPO = f"{ORG}/cefi-nwa-hindcast-monthly"
BUCKET = "noaa-oar-cefi-regional-mom6-pds"
SOURCE_URL = f"s3://{BUCKET}"
NICKNAME = BUCKET
SOURCE_NICKNAME = f"{BUCKET}-source"
REGION = "us-east-1"
RELEASE = "r20250715"
PREFIX = "northwest_atlantic/full_domain/hindcast/monthly"
# Source files whose only data array has an EMPTY chunk index (HDF5 reports 0 bytes of storage and 0 of 480 chunks, so any
# read returns all-fill) although the files are 3-14 GB: an interrupted / unflushed write on the producer's side. Verified
# 2026-10-26 with h5py (get_num_chunks() == 0, get_storage_size() == 0, a data block read is 100% NaN). Excluded (like the
# Pacific Islands empty files); regrid has no volcello/dissic/talk/psm/q_p_2_n_Sm as a result. See SOURCE-FIXES.md.
EXCLUDED = {("raw", "vmo_rotate"), ("regrid", "dissic"), ("regrid", "psm"), ("regrid", "q_p_2_n_Sm"),
            ("regrid", "talk"), ("regrid", "volcello")}
GRIDS = ("raw", "regrid")
CACHE = Path(__file__).resolve().parent / ".cache" / "nwa_hindcast_monthly"  # one pickle per file: incremental + resumable
BUNDLED_COORDS: tuple = ()  # this release's data files carry no geolon/geolat
OWN_STATICS = ("geolon", "geolat", "deptho", "areacello", "wet", "geolon_c", "geolat_c")  # from this release's ocean_static.nc
TIME_EXTRAS = ("time_bnds", "average_T1", "average_T2", "average_DT")
LOADABLE = ["time", "nv", *TIME_EXTRAS, "xh", "yh", "xq", "yq", "xT", "yT", "xB", "yB", "lat", "lon", "z_l", "zi", "ct"]
N_WINDOWS_2D, N_WINDOWS_4D = 40, 60  # random chunk windows hashed per file (plus corner chunks)
WORKSPACE = "earthmover-devs"
ENV_NAME = "cefi-ingest-2"
N_WORKERS = 24
PARSE_DEADLINE_S = 45 * 60
# Source defect (user decision 2026-10-26: ingest as published and flag): these four dimensionless limitation factors
# hold a few hundred garbage values (|x| up to ~2e23; valid range ~0.05-1) in both grids. Every other array must have none.
ALLOW_OTHER: tuple = ()  # no known garbage-value variables in this release (the build lists every offender at once)
BAD_SCAN = Path(__file__).resolve().parent / ".cache" / "nwa_monthly_badvalues.json"  # hard limit for one parse run (resumable)
DROP_GROUP_ATTRS = ("coordinates", "title", "cefi_filename", "cefi_ori_filename", "cefi_variable", "associated_files")
LONLAT = {"geolon": "longitude", "geolat": "latitude"}
DEGREE_UNITS = {"degrees_east", "degree_east", "degrees_north", "degree_north"}
DESC = ("NOAA CEFI MOM6 Northwest Atlantic hindcast (monthly, r20250715): virtual references over the NODD NetCDF files; "
        "raw (ocean/u/v + sea-ice groups) and regrid (ocean + ice on its own lon axis); one array per variable.")
assert len(DESC) <= 255, len(DESC)


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
           "time_units": str(pd.Timestamp(ds["time"].attrs.get("units").split("since ")[1].strip().split()[0]).date()), "time_calendar": ds["time"].attrs.get("calendar"),
           "time_hash": hash_f8(ds["time"].values), "n_time": int(ds["time"].size)}
    for c in ("xh", "yh", "xq", "yq", "xT", "yT", "xB", "yB", "lat", "lon", "z_l", "zi", "ct", "nv"):
        if c in ds.variables:
            chk[f"coord_{c}"] = hash_f8(ds[c].values)
    if "lon" in ds.variables:
        chk["lon0"] = float(ds["lon"].values[0])
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
            name=f"cefi-nwa-monthly-{self.label}", workspace=WORKSPACE, software=ENV_NAME, n_workers=self.n,
            region=REGION, spot_policy="spot_with_fallback", worker_cpu=2, worker_memory="8GiB",
            worker_options={"nthreads": 1}, scheduler_vm_types=["m6i.xlarge"], idle_timeout="10 minutes",
            shutdown_on_close=True)
        print(f"  [coiled] cluster up: {self.cluster.name} ({self.n} workers)", flush=True)
        self.client = Client(self.cluster)
        return self.client

    def __exit__(self, *exc):
        self.cluster.close()
        print("  [coiled] cluster closed", flush=True)


def bad_scan_task(path: str, grid: str, var: str) -> dict:
    """Exact counts of out-of-range values in one whole array. These are dimensionless limitation factors that live in
    [0, 1]: 'garbage' = outside [-1, 2] (not the array's fill); 'slightly outside' = outside [0, 1] but within [-1, 2]."""
    import h5py
    import obstore as obs
    store = source_store()
    f = h5py.File(Reader(obs.open_reader(store, path, buffer_size=4 * 1024 * 1024)), "r")
    d = f[var]
    fill = float(np.asarray(d.attrs.get("_FillValue", np.nan)).ravel()[0])
    out = {"grid": grid, "var": var, "n_total": int(np.prod(d.shape)), "n_bad": 0, "n_slightly_outside": 0, "bad_min": None,
           "bad_max": None, "valid_min": None, "valid_max": None, "times": set(), "bbox": None,
           "long_name": str(d.attrs.get("long_name", "")), "units": str(d.attrs.get("units", ""))}
    for t0 in range(0, d.shape[0], d.chunks[0]):
        a = d[t0:t0 + d.chunks[0]]
        sentinel = (a == np.float32(fill)) if not np.isnan(fill) else np.zeros(a.shape, bool)
        fin = np.isfinite(a) & ~sentinel
        bad = fin & ((a < -1) | (a > 2))
        ok = fin & ~bad
        if ok.any():
            lo, hi = float(a[ok].min()), float(a[ok].max())
            out["valid_min"] = lo if out["valid_min"] is None else min(out["valid_min"], lo)
            out["valid_max"] = hi if out["valid_max"] is None else max(out["valid_max"], hi)
            out["n_slightly_outside"] += int((ok & ((a < 0) | (a > 1))).sum())
        if bad.any():
            out["n_bad"] += int(bad.sum())
            b = a[bad]
            out["bad_min"] = float(b.min()) if out["bad_min"] is None else min(out["bad_min"], float(b.min()))
            out["bad_max"] = float(b.max()) if out["bad_max"] is None else max(out["bad_max"], float(b.max()))
            idx = np.argwhere(bad)
            out["times"].update((idx[:, 0] + t0).tolist())
            bb = [int(idx[:, k].min()) for k in (1, 2)] + [int(idx[:, k].max()) for k in (1, 2)]
            out["bbox"] = bb if out["bbox"] is None else [min(out["bbox"][0], bb[0]), min(out["bbox"][1], bb[1]),
                                                           max(out["bbox"][2], bb[2]), max(out["bbox"][3], bb[3])]
    out["times"] = sorted(out["times"])
    return out


def phase_scan_bad() -> None:
    import json
    store = source_store()
    jobs = [(p, g, var_of(p)) for g in GRIDS for p in list_nc(store, g) if var_of(p) in ALLOW_OTHER]
    assert len(jobs) == 2 * len(ALLOW_OTHER), jobs
    res = []
    with remote_cluster("scanbad", n=len(jobs)) as client:
        for fut in client.map(bad_scan_task, *zip(*jobs), pure=False):
            res.append(fut.result())
    BAD_SCAN.parent.mkdir(exist_ok=True)
    json.dump(sorted(res, key=lambda r: (r["grid"], r["var"])), open(BAD_SCAN, "w"), indent=1)
    for r in sorted(res, key=lambda r: (r["grid"], r["var"])):
        print(f"  {r['grid']:6s} {r['var']:11s} {r['n_bad']:5d} garbage of {r['n_total']} values ({r['n_bad'] / r['n_total']:.2e}); "
              f"garbage range {r['bad_min']:.3g}..{r['bad_max']:.3g}; valid range {r['valid_min']:.3g}..{r['valid_max']:.3g}; "
              f"{len(r['times'])} time steps affected; bbox (y0,x0,y1,x1) {r['bbox']}", flush=True)


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
    assert k["other"] == 0 or r["var"] in ALLOW_OTHER, f"{r['path']}: stored values that are neither valid nor the fill: {k}"
    kinds = [kk for kk in ("nan", "attr", "hdf") if k[kk]]
    assert len(kinds) <= 1, f"{r['path']}: more than one 'no data' sentinel stored: {k}"
    kind = kinds[0] if kinds else ("nan" if np.isnan(c["attr_fill"] if c["attr_fill"] is not None else np.nan) else "attr")
    value = {"attr": c["attr_fill"], "hdf": c["hdf_fill"], "nan": float("nan")}[kind]
    return kind, float(value)


def decode_days(base_units: str, values) -> np.ndarray:
    base = pd.Timestamp(base_units.split("since ")[1].strip().split()[0])
    v = np.asarray(values, "float64")
    return (base + pd.to_timedelta(v.ravel(), unit="D")).values.reshape(v.shape)


def group_of(grid: str, dims: tuple, lon0: float | None = None) -> str:
    if grid == "regrid":
        return "regrid/ice" if abs(lon0 + 98.0) < 1e-6 else "regrid/main"
    h = tuple(d for d in dims if d in ("yh", "yq", "yT", "yB", "xh", "xq", "xT", "xB"))
    return {("yh", "xh"): "raw/main", ("yh", "xq"): "raw/u_grid", ("yq", "xh"): "raw/v_grid", ("yT", "xT"): "raw/ice",
            ("yT", "xB"): "raw/ice_xB", ("yB", "xT"): "raw/ice_yB"}[h]


DECADAL_STATIC = "northwest_atlantic/full_domain/decadal_forecast/yearly/raw/r20250819/ocean_static.nc"
COORD_STATICS = ("geolon", "geolat", "geolon_c", "geolat_c")  # this release's own static (gap-free)
MASK_STATICS = ("deptho", "areacello", "wet")  # from the NWA decadal-yearly static (user decision 2026-10-26)


def load_statics(store):
    """HYBRID statics (user decision 2026-10-26, same as the other NWA repos): this release's ocean_static.nc is gap-free in
    geolon/geolat/_c but its land/sea mask (wet/deptho) marks 288 cells as land where the data has values; the NWA
    decadal-yearly r20250819 static's mask matches the data exactly. So: geolon/geolat/_c from this release's static
    (netCDF3, scipy), deptho/areacello/wet from the decadal static (netCDF4); 'no data' fill stored as NaN. Also ice_static.nc
    (netCDF4): CELL_AREA."""
    import h5py
    import obstore as obs
    import xarray as xr
    from scipy.io import netcdf_file
    own = netcdf_file(io.BytesIO(bytes(obs.get(store, f"{PREFIX}/raw/{RELEASE}/ocean_static.nc").bytes())), mmap=False)
    dec = h5py.File(io.BytesIO(bytes(obs.get(store, DECADAL_STATIC).bytes())), "r")
    for d in ("xh", "yh", "xq", "yq"):  # both statics describe the same grid
        assert np.array_equal(np.asarray(own.variables[d][:], "float64"), np.asarray(dec[d][:], "float64")), d
    out, report = xr.Dataset(), {}
    for n in COORD_STATICS:
        v = own.variables[n]
        a = np.array(v[:], dtype="float32")
        assert np.isfinite(a).all() and (np.abs(a) < 1e19).all(), f"ocean_static {n} has gaps (the plan relies on it being gap-free)"
        attrs = {k: (getattr(v, k).decode() if isinstance(getattr(v, k), bytes) else getattr(v, k))
                 for k in ("long_name", "units") if hasattr(v, k)}
        attrs["comment"] = "from this release's ocean_static.nc (netCDF3)"
        out[n] = (tuple(v.dimensions), a, attrs)
    for n in MASK_STATICS:
        d = dec[n]
        a = np.array(d[:], dtype="float32")
        report[n] = int((np.abs(a) > 1e19).sum() + np.isnan(a).sum())
        a = np.where(np.abs(a) > 1e19, np.float32(np.nan), a)
        attrs = {k: (d.attrs[k].decode() if isinstance(d.attrs[k], bytes) else d.attrs[k]) for k in ("long_name", "units") if k in d.attrs}
        attrs["comment"] = ("from the NWA decadal-forecast r20250819 ocean_static.nc: this release's own ocean_static.nc marks 288 cells "
                            "as land where the data has values; that model's land/sea mask matches the data exactly. "
                            "'no data' fill stored as NaN")
        out[n] = (("yh", "xh"), a, attrs)
    print(f"  [statics] hybrid: geolon/geolat/_c from this release; deptho/areacello/wet from the decadal static "
          f"(masked/NaN cells {report}); dims {dict(own.dimensions)}", flush=True)
    ice = h5py.File(io.BytesIO(bytes(obs.get(store, f"{PREFIX}/raw/{RELEASE}/ice_static.nc").bytes())), "r")
    ca = np.array(ice["CELL_AREA"][:], dtype="float32")
    nmask = int((np.abs(ca) > 1e19).sum() + np.isnan(ca).sum())
    ca = np.where(np.abs(ca) > 1e19, np.float32(np.nan), ca)
    ice_static = {"CELL_AREA": ca, "xT": np.asarray(ice["xT"][:], "float64"), "yT": np.asarray(ice["yT"][:], "float64")}
    print(f"  [statics] ice_static.nc CELL_AREA {ca.shape}, masked/NaN cells {nmask}", flush=True)
    coords = {d: np.asarray(own.variables[d][:], "float64") for d in ("xh", "yh", "xq", "yq")}
    return out, coords, ice_static


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


GROUP_ORDER = ("raw/main", "raw/u_grid", "raw/v_grid", "raw/b_x", "raw/b_y", "regrid")


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


GROUP_ORDER = ("raw/main", "raw/u_grid", "raw/v_grid", "raw/ice", "raw/ice_xB", "raw/ice_yB", "regrid/main", "regrid/ice")


def build_cubes(results: list[dict]):
    import virtualizarr  # noqa: F401
    import xarray as xr
    from virtualizarr.manifests import ManifestArray
    store = source_store()
    st, scoords, ice_static = load_statics(store)  # fail fast, before anything slow
    mask_check(st, results)
    by_group: dict[str, list[dict]] = {}
    for r in results:
        by_group.setdefault(group_of(r["grid"], r["chk"]["dims"], r["chk"].get("lon0")), []).append(r)
    assert sorted(by_group) == sorted(GROUP_ORDER), sorted(by_group)
    print("  groups: " + ", ".join(f"{g} {len(by_group[g])}" for g in GROUP_ORDER), flush=True)
    # ---- arrays with out-of-range stored values other than the fill: list ALL of them at once
    suspects = sorted((r["grid"], r["var"], r["chk"]["classes"]["other"]) for r in results
                      if r["chk"]["classes"]["other"] and r["var"] not in ALLOW_OTHER)
    assert not suspects, f"{len(suspects)} arrays store values that are neither valid nor their fill (sampled windows): {suspects}"
    # ---- consistency of the shared axes / coordinates across files
    for grid in GRIDS:
        rs = [r for r in results if r["grid"] == grid]
        for key in ("time_hash", "time_units", "time_calendar", "n_time"):
            vals = {r["chk"][key] for r in rs}
            assert len(vals) == 1, f"[{grid}] {key} differs between files: {vals}"
        print(f"  [{grid}] {len(rs)} variables, {rs[0]['chk']['n_time']} time steps (base {rs[0]['chk']['time_units']}, "
              f"{rs[0]['chk']['time_calendar']}); time axis identical across files", flush=True)
    for g, rs in by_group.items():
        for key in sorted({k for r in rs for k in r["chk"] if k.startswith("coord_")}):
            vals = {r["chk"][key] for r in rs if key in r["chk"]}
            assert len(vals) == 1, f"[{g}] {key} differs between files: {len(vals)} distinct"
    time_base = f"days since {results[0]['chk']['time_units']}"
    extras_src = [r for r in results if r["grid"] == "raw" and r["chk"]["extras"]]
    for n in TIME_EXTRAS:
        hs = {(r["chk"]["extras"][n]["hash"], r["chk"]["extras"][n]["units"].split("since ")[1].split()[0] if n != "average_DT" else "d")
              for r in extras_src if n in r["chk"]["extras"]}
        assert len(hs) == 1, f"raw {n} differs between the files that carry it: {len(hs)} distinct"
    extras = extras_src[0]["chk"]["extras"]
    print(f"  [raw] time extras {sorted(extras)} identical across the {len(extras_src)} raw files that carry them", flush=True)
    cubes, truth = {}, {}
    for group in GROUP_ORDER:
        rs = sorted(by_group[group], key=lambda r: r["var"])
        fills = {r["var"]: array_fill(r) for r in rs}
        kinds = {k: sum(1 for kk, _ in fills.values() if kk == k) for k in ("nan", "attr", "hdf")}
        vals = sorted({(kk, v) for kk, v in fills.values() if kk != "nan"})
        tot = {k: sum(r["chk"]["classes"][k] for r in rs) for k in ("nan", "attr", "hdf", "other", "valid")}
        dropped = sum(len(r["chk"]["bundled_dropped"]) for r in rs)
        print(f"  [{group}] {len(rs)} variables | stored 'no data' sentinel per array: {kinds} (non-NaN values {vals}) | "
              f"sampled values {tot} | every chunk written in every file | bundled arrays dropped: {dropped}", flush=True)
        merged = xr.merge([r["ds"] for r in rs], compat="override", join="exact", combine_attrs="drop_conflicts")
        cube = merged.copy()
        names = {r["var"] for r in rs}
        for n in [v for v in merged.data_vars if v in names]:
            v = merged[n]
            cube[n] = xr.Variable(v.dims, with_fill(v.data, fills[n][1]), dict(v.attrs), dict(v.encoding))
        keep = group == "raw/main"
        if not keep:  # bundled coordinates / time fields are kept once, in raw/main
            cube = cube.drop_vars([n for n in (*BUNDLED_COORDS, *TIME_EXTRAS, "nv") if n in cube.variables])
        cube = nativize_coords(cube, rs)
        virtual_left = [n for n in cube.variables if n not in names and isinstance(cube[n].variable._data, ManifestArray)]
        assert not virtual_left, f"[{group}] non-data variables left virtual: {virtual_left}"
        t = decode_days(time_base, merged["time"].values)
        cube = cube.assign_coords(time=("time", t, {"standard_name": "time", "long_name": "time"}))
        cube["time"].encoding = {"units": "days since 1993-01-01 00:00:00", "calendar": "gregorian", "dtype": "float64"}
        for n in TIME_EXTRAS:
            if n in cube:
                v = cube[n]
                vals_ = v.values if n == "average_DT" else decode_days(extras[n]["units"], v.values)
                attrs = {k: a for k, a in v.attrs.items() if k not in ("units", "calendar", "calendar_type")}
                if n == "average_DT":
                    attrs["units"] = "days"
                cube[n] = (v.dims, vals_, attrs)
                if n != "average_DT":
                    cube[n].encoding = {"units": "days since 1993-01-01 00:00:00", "calendar": "gregorian", "dtype": "float64"}
        if "time_bnds" in cube:
            cube["time"].attrs["bounds"] = "time_bnds"
        else:
            cube["time"].attrs.pop("bounds", None)
        cube.attrs = {k: v for k, v in cube.attrs.items() if k not in DROP_GROUP_ATTRS}
        for n in cube.data_vars:
            if n in ALLOW_OTHER:
                import json
                info = next(x for x in json.load(open(BAD_SCAN)) if x["var"] == n and x["grid"] == ("regrid" if group == "regrid" else "raw"))
                cube[n].attrs["cefi_data_quality"] = (
                    f"This is a dimensionless limitation factor that should lie in [0, 1]. The NOAA source file contains "
                    f"{info['n_bad']} garbage values out of {info['n_total']} (outside [-1, 2]; from {info['bad_min']:.3g} to "
                    f"{info['bad_max']:.3g}) in {len(info['times'])} of 396 months, and {info['n_slightly_outside']} further values "
                    f"slightly outside [0, 1] (the in-range values span {info['valid_min']:.3g} to {info['valid_max']:.3g}). "
                    "All values are published unchanged; mask values outside [0, 1] before computing statistics.")
        if group == "raw/main":
            for d in ("xh", "yh", "xq", "yq"):
                got = cube[d].values if d in cube.variables else None
                if got is not None:
                    assert np.array_equal(np.asarray(got, "float64"), scoords[d]), f"data {d} != ocean_static {d}"
            cube = cube.assign({n: st[n] for n in OWN_STATICS}).assign_coords(
                xq=("xq", scoords["xq"], {"units": "degrees_east"}), yq=("yq", scoords["yq"], {"units": "degrees_north"}))
        if group == "raw/ice":
            assert np.array_equal(ice_static["xT"], cube["xT"].values) and np.array_equal(ice_static["yT"], cube["yT"].values), \
                "ice_static xT/yT != the data files' xT/yT"
            cube = cube.assign(CELL_AREA=(("yT", "xT"), ice_static["CELL_AREA"], {
                "long_name": "ice-grid cell area", "comment": "from this release's ice_static.nc; 'no data' fill stored as NaN"}))
            same = np.array_equal(cube["xT"].values, scoords["xh"]) and np.array_equal(cube["yT"].values, scoords["yh"])
            print(f"  [raw/ice] data xT/yT numerically equal to yh/xh: {same}", flush=True)
            assert same, ("yT/xT differ from yh/xh: per the plan, do NOT copy ocean_static geolon/geolat to raw/ice; "
                          "carry no ice lon/lat and re-ask the user")
            for n in ("geolon", "geolat"):  # plan: copy only if yT/xT == yh/xh
                v = st[n]
                cube[n] = (("yT", "xT"), v.values, {**dict(v.attrs), "comment": "ocean_static.nc geolon/geolat (the ice grid has the "
                           "same cell centres: yT/xT are numerically equal to yh/xh)"})
        if group == "regrid/ice":
            cube.attrs["comment"] = ("The sea-ice variables are regridded onto their own longitude axis (start -98.0, step 0.0801), "
                                     "which differs from the ocean variables' axis in regrid/main (start -98.4423, step 0.0807).")
        used = {d for n in cube.data_vars for d in cube[n].dims}
        unused = [c for c in cube.coords if c in cube.dims and c not in used]
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
        try:
            done, not_done = wait(list(futs), timeout=1800)
        except Exception:  # noqa: BLE001  (distributed raises TimeoutError instead of returning the unfinished futures)
            done = {f for f in futs if f.done()}
            not_done = set(futs) - done
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
    g.add_argument("--scan-bad", action="store_true", help="exact garbage-value counts for the 8 flagged arrays (Coiled)")
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()
    if args.parse:
        phase_parse(args.local)
        return 0
    if args.scan_bad:
        phase_scan_bad()
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
        "model": "MOM6-COBALT", "region": "northwest_atlantic", "experiment": "hindcast", "experiment_kind": "hindcast",
        "frequency": "monthly", "release": RELEASE, "license": "CC-BY-4.0",
        "excluded": "6 arrays whose source files have an empty chunk index (no readable data): regrid dissic, talk, volcello, "
                    "psm, q_p_2_n_Sm and raw vmo_rotate"})
    with remote_cluster("validate") as dclient:
        remote_validate(repo, truth, dclient)
    return 0


if __name__ == "__main__":
    sys.exit(main())
