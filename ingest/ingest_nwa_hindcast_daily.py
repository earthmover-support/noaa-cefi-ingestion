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
"""NWA hindcast daily (r20250715) -> `noaa-cefi/cefi-nwa-hindcast-daily`.

Virtual, parsed directly from the NetCDF4 files. One file per variable covers the whole period, so there is no
concatenation: each data array is one virtual reference set.

    raw     (time: 11322, yh: 845, xh: 775)   11 variables + native time extras + statics
    regrid  (time: 11322, lat: 844, lon: 774) 11 variables

  time extras (raw): time_bnds (time, nv), average_T1/T2 (decoded datetimes), average_DT: native, taken once after
                     verifying all 11 files carry identical values
  fill value: the zarr fill_value is set to the sentinel the data really stores. VirtualiZarr takes the HDF5-level
              fill (9.97e36); the raw files actually store 1e20 on land (= their `_FillValue` attribute), the regrid
              files store NaN. Each array is checked: its stored non-valid values must all equal ONE sentinel.
  statics (raw): core 5 + geolon_c/geolat_c from this release's netCDF3 ocean_static.nc; coordinates from the NWA hindcast
                 monthly static if this release's are masked (hybrid rule, user 2026-10-22).

Phases: --parse [--local]  (Coiled, workspace `earthmover-devs`)  |  --dry-run [--skip-windows]  |  --write
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
from pathlib import Path

import numpy as np
import pandas as pd

ORG = "noaa-cefi"
REPO = f"{ORG}/cefi-nwa-hindcast-daily"
BUCKET = "noaa-oar-cefi-regional-mom6-pds"
SOURCE_URL = f"s3://{BUCKET}"
NICKNAME = BUCKET
SOURCE_NICKNAME = f"{BUCKET}-source"
REGION = "us-east-1"
RELEASE = "r20250715"
PREFIX = "northwest_atlantic/full_domain/hindcast/daily"
HINDCAST_STATIC = "northwest_atlantic/full_domain/hindcast/monthly/raw/r20250715/ocean_static.nc"
GRIDS = ("raw", "regrid")
CACHE = Path(__file__).resolve().parent / ".cache" / "nwa_hindcast_daily.pkl"
CORE_STATICS = ("geolon", "geolat", "deptho", "areacello", "wet", "geolon_c", "geolat_c")
COORD_STATICS = ("geolon", "geolat", "geolon_c", "geolat_c")
TIME_EXTRAS = ("time_bnds", "average_T1", "average_T2", "average_DT")
LOADABLE = ["time", "nv", *TIME_EXTRAS, "xh", "yh", "lat", "lon"]
N_WINDOWS = 120  # random chunk windows hashed per file (plus corner chunks)
WORKSPACE = "earthmover-devs"
ENV_NAME = "cefi-ingest-2"
N_WORKERS = 11
DROP_GROUP_ATTRS = ("coordinates", "title", "cefi_filename", "cefi_ori_filename", "cefi_variable", "associated_files")
LONLAT = {"geolon": "longitude", "geolat": "latitude"}
DEGREE_UNITS = {"degrees_east", "degree_east", "degrees_north", "degree_north"}
DESC = ("NOAA CEFI MOM6 Northwest Atlantic hindcast (daily, r20250715): virtual references over the NODD "
        "NetCDF files; raw + regrid grids, one array per variable over the full period.")


def source_store():
    import obstore as obs
    return obs.store.S3Store(BUCKET, region=REGION, skip_signature=True)


def list_nc(store, grid: str) -> list[str]:
    pre = f"{PREFIX}/{grid}/{RELEASE}/"
    return sorted(o["path"] for b in store.list(prefix=pre) for o in b
                  if o["path"].endswith(".nc") and not o["path"].endswith("_static.nc"))


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
    ds = vz.open_virtual_dataset(f"{SOURCE_URL}/{path}", registry=reg, parser=HDFParser(),
                                 loadable_variables=LOADABLE, decode_times=False)
    f = h5py.File(Reader(obs.open_reader(store, path, buffer_size=1024 * 1024)), "r")
    d = f[var]
    attr_fill = d.attrs.get("_FillValue")
    attr_fill = None if attr_fill is None else float(np.asarray(attr_fill).ravel()[0])
    hdf_fill = float(np.asarray(d.fillvalue).ravel()[0])
    grid_shape = tuple(-(-n // c) for n, c in zip(d.shape, d.chunks))
    expected = int(np.prod(grid_shape))
    chk = {"shape": d.shape, "chunks": d.chunks, "dims": ds[var].dims, "attr_fill": attr_fill, "hdf_fill": hdf_fill,
           "nchunks": int(d.id.get_num_chunks()), "expected_chunks": expected,
           "time_units": ds["time"].attrs.get("units"), "time_calendar": ds["time"].attrs.get("calendar"),
           "time_hash": hash_f8(ds["time"].values), "n_time": int(ds["time"].size)}
    for c in ("xh", "yh", "lat", "lon"):
        if c in ds:
            chk[f"coord_{c}"] = hash_f8(ds[c].values)
    chk["extras"] = {n: {"hash": hash_f8(ds[n].values), "units": ds[n].attrs.get("units"), "shape": ds[n].shape}
                     for n in TIME_EXTRAS if n in ds}
    # windows: corners + random chunks; hash + classify every stored value (one sentinel only?)
    rng = np.random.default_rng(int(hashlib.sha1(f"{grid}/{var}".encode()).hexdigest()[:8], 16))
    idxs = {tuple(0 for _ in grid_shape), tuple(n - 1 for n in grid_shape)}
    while len(idxs) < N_WINDOWS + 2:
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
            name=f"cefi-hindcast-daily-{self.label}", workspace=WORKSPACE, software=ENV_NAME, n_workers=self.n,
            region=REGION, spot_policy="spot_with_fallback", worker_cpu=2, worker_memory="8GiB",
            worker_options={"nthreads": 1}, scheduler_vm_types=["m6i.large"], idle_timeout="10 minutes",
            shutdown_on_close=True)
        print(f"  [coiled] cluster up: {self.cluster.name} ({self.n} workers)", flush=True)
        self.client = Client(self.cluster)
        return self.client

    def __exit__(self, *exc):
        self.cluster.close()
        print("  [coiled] cluster closed", flush=True)


def phase_parse(local: bool) -> None:
    store = source_store()
    jobs = [(p, g, var_of(p)) for g in GRIDS for p in list_nc(store, g)]
    print(f"[parse] {len(jobs)} files ({'local' if local else 'Coiled'})", flush=True)
    t = time.time()
    results = []
    if local:
        with cf.ProcessPoolExecutor(4) as pool:
            for fut in cf.as_completed([pool.submit(parse_task, *j) for j in jobs]):
                results.append(fut.result())
                print(f"  [parse] {len(results)}/{len(jobs)} ({time.time() - t:.0f}s)", flush=True)
    else:
        with remote_cluster("parse") as client:
            for fut in client.map(parse_task, *zip(*jobs), retries=1, pure=False):
                results.append(fut.result())
    CACHE.parent.mkdir(exist_ok=True)
    with open(CACHE, "wb") as fh:
        pickle.dump(results, fh)
    bad = [r for r in results if "error" in r]
    print(f"[parse] cached {len(results)} results ({len(bad)} failed) -> {CACHE} ({time.time() - t:.0f}s)", flush=True)
    for r in bad:
        print(f"  FAILED {r['path']}\n    {r['error']}", flush=True)
    if bad:
        sys.exit(2)


# ------------------------------- build ------------------------------------- #

def with_fill(ma, value: float):
    from virtualizarr.manifests import ManifestArray
    from zarr.core.metadata.v3 import ArrayV3Metadata
    d = ma.metadata.to_dict()
    d["fill_value"] = "NaN" if np.isnan(value) else float(np.float32(value))
    return ManifestArray(metadata=ArrayV3Metadata.from_dict(d), chunkmanifest=ma.manifest)


def decide_fill(grid: str, rs: list[dict]) -> float:
    """The sentinel each array really stores for 'no data': exactly one value, the same for every file of the grid."""
    sentinels = set()
    for r in rs:
        c, k = r["chk"], r["chk"]["classes"]
        assert c["nchunks"] == c["expected_chunks"], f"{r['path']}: unwritten chunks"
        assert k["other"] == 0, f"{r['path']}: stored values that are neither valid nor the file's fill: {k}"
        if k["attr"] and k["nan"]:
            raise AssertionError(f"{r['path']}: mixes NaN and {c['attr_fill']} as 'no data': {k}")
        if k["hdf"] and k["attr"]:
            raise AssertionError(f"{r['path']}: two different fill sentinels stored: {k}")
        sentinels.add("attr" if k["attr"] else "hdf" if k["hdf"] else "nan")
    assert len(sentinels) == 1, f"[{grid}] files disagree on the stored sentinel: {sentinels}"
    kind = sentinels.pop()
    first = rs[0]["chk"]
    value = {"attr": first["attr_fill"], "hdf": first["hdf_fill"], "nan": float("nan")}[kind]
    assert all({"attr": x["chk"]["attr_fill"], "hdf": x["chk"]["hdf_fill"], "nan": float("nan")}[kind] == value
               or (np.isnan(value) and np.isnan({"attr": x["chk"]["attr_fill"], "hdf": x["chk"]["hdf_fill"], "nan": float("nan")}[kind]))
               for x in rs), f"[{grid}] sentinel value differs between files"
    tot = {k: sum(r["chk"]["classes"][k] for r in rs) for k in ("nan", "attr", "hdf", "other", "valid")}
    print(f"  [{grid}] stored 'no data' sentinel = {kind} ({value!r}); sampled values: {tot}; every chunk written in every file",
          flush=True)
    return float(value)


def decode_days(base_units: str, values) -> np.ndarray:
    base = pd.Timestamp(base_units.split("since ")[1].strip().split()[0])
    v = np.asarray(values, "float64")
    return (base + pd.to_timedelta(v.ravel(), unit="D")).values.reshape(v.shape)


def load_statics(store):
    import obstore as obs
    import xarray as xr
    from scipy.io import netcdf_file
    nc = lambda p: netcdf_file(io.BytesIO(bytes(obs.get(store, p).bytes())), mmap=False)  # noqa: E731
    own = nc(f"{PREFIX}/raw/{RELEASE}/ocean_static.nc")
    hc = nc(HINDCAST_STATIC)
    same = {n: bool(np.array_equal(np.array(own.variables[n][:]), np.array(hc.variables[n][:]))) for n in CORE_STATICS}

    def var(n, src):
        v = src.variables[n]
        return v.dimensions, np.array(v[:]).astype("float32"), {k: (getattr(v, k).decode() if isinstance(getattr(v, k), bytes)
                                                              else getattr(v, k)) for k in ("long_name", "units") if hasattr(v, k)}
    out, report = xr.Dataset(), {}
    for n in CORE_STATICS:
        dims, a, attrs = var(n, own)
        masked = int((np.abs(a) > 1e19).sum())
        report[n] = masked
        if n in COORD_STATICS and masked:
            _, h, hattrs = var(n, hc)
            assert np.isfinite(h).all() and (np.abs(h) < 1e19).all(), f"hindcast monthly {n} has gaps"
            valid = np.abs(a) < 1e19
            assert a.shape == h.shape and np.array_equal(a[valid], h[valid]), f"{n}: monthly static != this release where valid"
            hattrs["comment"] = ("taken from the NWA hindcast monthly r20250715 ocean_static.nc: this release's own "
                                 "ocean_static.nc masks land with 1e20; identical wherever that file is unmasked")
            out[n] = (dims, h, hattrs)
        else:
            out[n] = (dims, a, attrs)
    for d in ("xh", "yh", "xq", "yq"):
        if d in own.variables:
            out = out.assign_coords({d: (d, np.asarray(own.variables[d][:], "float64"), {})})
    print(f"  [statics] masked (1e20) cells per field in this release: {report}; identical to the monthly hindcast static: "
          f"{[n for n, s in same.items() if s]}", flush=True)
    return out


def build_cubes(results: list[dict]):
    import virtualizarr  # noqa: F401
    import xarray as xr
    store = source_store()
    cubes, truth = {}, {}
    for grid in GRIDS:
        rs = sorted((r for r in results if r["grid"] == grid), key=lambda r: r["var"])
        print(f"  [{grid}] {len(rs)} variables: {[r['var'] for r in rs]}", flush=True)
        for key in ("time_hash", "time_units", "time_calendar", "n_time", "coord_xh", "coord_yh", "coord_lat", "coord_lon"):
            vals = {r["chk"].get(key) for r in rs}
            assert len(vals) == 1, f"[{grid}] {key} differs between files: {vals}"
        assert rs[0]["chk"]["n_time"] == 11322 and rs[0]["chk"]["time_calendar"] == "gregorian"
        fill = decide_fill(grid, rs)
        extras = {}
        if grid == "raw":
            for n in TIME_EXTRAS:
                hs = {(r["chk"]["extras"][n]["hash"], r["chk"]["extras"][n]["units"]) for r in rs}
                assert len(hs) == 1, f"raw {n} differs between files"
            extras = rs[0]["chk"]["extras"]
        merged = xr.merge([r["ds"] for r in rs], compat="override", join="exact", combine_attrs="drop_conflicts")
        cube = merged.copy()
        for n in [v for v in merged.data_vars if v in {r["var"] for r in rs}]:
            v = merged[n]
            cube[n] = xr.Variable(v.dims, with_fill(v.data, fill), dict(v.attrs), dict(v.encoding))
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
        cube.attrs = {k: v for k, v in cube.attrs.items() if k not in DROP_GROUP_ATTRS}
        if grid == "raw":
            st = load_statics(store)
            for d in ("yh", "xh"):
                assert np.array_equal(st[d].values, cube[d].values), f"static {d} != data {d}"
            cube = cube.assign(st)
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
    store = repo.readonly_session(branch="main").store
    for grid, per in truth.items():
        futs = [client.submit(window_task, store, grid, var, w, pure=False) for var, w in per.items()]
        bad = [x for f in futs for x in f.result()]
        assert not bad, f"[{grid}] {len(bad)} chunk windows differ: {bad[:5]}"
        n = sum(len(w) for w in per.values())
        print(f"  [{grid}] OK: {n} source chunk windows ({len(per)} variables) match the committed store", flush=True)


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
    ap.add_argument("--local", action="store_true")
    args = ap.parse_args()
    if args.parse:
        phase_parse(args.local)
        return 0
    with open(CACHE, "rb") as fh:
        results = pickle.load(fh)
    assert not [r for r in results if "error" in r], "cache contains failed files; rerun --parse"
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
        "model": "MOM6-COBALT", "region": "northwest_atlantic", "experiment": "hindcast", "experiment_kind": "hindcast",
        "frequency": "daily", "release": RELEASE, "license": "CC-BY-4.0"})
    with remote_cluster("validate") as dclient:
        remote_validate(repo, truth, dclient)
    return 0


if __name__ == "__main__":
    sys.exit(main())
