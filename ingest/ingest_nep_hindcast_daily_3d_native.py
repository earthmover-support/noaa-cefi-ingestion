#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "arraylake>=0.16", "coiled", "distributed", "icechunk>=1.2", "obstore>=0.5", "xarray", "zarr>=3", "numpy", "pandas",
#   "h5py", "h5netcdf",
# ]
# ///
"""NEP hindcast daily (r20260701): the per-year 3-D (time, z_l, y, x) variables as NATIVE chunks, added to
`noaa-cefi/cefi-nep-hindcast-daily` (groups `raw` and `regrid`, next to the virtual 2-D variables).

Why native (user decision 2026-10-26): these variables are stored as one NetCDF per year with a time chunk of 100, which
does not divide 365/366, so the years cannot be one virtual array (needs variable-length chunks). They are read with Xarray
on Coiled and written with `to_zarr(region=...)` into the same Icechunk repo. Source files are never modified.

  raw     dissic no3 o2 po4 si so talk thetao uo(iq) vo(jq) volcello (vollcello if it differs from volcello)
  regrid  dissic no3 o2 po4 si so talk thetao volcello (vollcello if it differs)
  The `volcello`/`vollcello` copies bundled inside other variables' files are not read (user decision 2026-10-22).

Layout: time = the repo's existing 12,053-day axis (year files are placed by date and checked against it); zarr chunks
(30, 10, 200, 200) float32, Blosc(zstd, shuffle); zarr fill_value = the sentinel the data really stores (one per array,
asserted). One Coiled task = 60 days x 10 levels, written into a forked Icechunk session; one commit per variable.

Phases (run long ones under `caffeinate -i`):
  --probe     Coiled: header + 12 random source chunk windows hashed per file (ground truth), sentinels, time axes, mask check
  --create    local, metadata only: native z_l + empty arrays for every variable in both groups, one commit
  --write     Coiled: fill the arrays (resumable: variables with a commit are skipped); --only grid/var restricts
  --fix-fill-attrs  local, metadata only: add _FillValue/missing_value attrs to arrays created without them (done 2026-10-27)
  --validate  Coiled: hash the sampled source windows from the committed store; element counts from the write stats
"""
import argparse
import base64
import hashlib
import json
import pickle
import re
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ORG = "noaa-cefi"
REPO = f"{ORG}/cefi-nep-hindcast-daily"
BUCKET = "noaa-oar-cefi-regional-mom6-pds"
REGION = "us-east-1"
RELEASE = "r20260701"
PREFIX = "northeast_pacific/full_domain/hindcast/daily"
GRIDS = ("raw", "regrid")
CACHE = Path(__file__).resolve().parent / ".cache" / "nep_daily_3d"
PROBE_DIR = CACHE / "probe"
STATS_DIR = CACHE / "stats"
WORKSPACE = "earthmover-devs"
ENV_NAME = "cefi-ingest-2"
N_WINDOWS = 12            # random source chunk windows hashed per file (ground truth)
ZCHUNKS = (30, 10, 200, 200)
TASK_DAYS = 60            # 2 zarr time chunks per task (90 days ran workers out of memory on raw/thetao)
TASK_LEVELS = 10          # one zarr level chunk per task
FILE_RX = re.compile(r"/([^/.]+)\.nep\.full\.hcast\.daily\.(raw|regrid)\." + RELEASE + r"\.(\d{4})01-(\d{4})12\.nc$")
DROP_ATTRS = ("_FillValue", "missing_value", "coordinates", "_Netcdf4Coordinates", "_Netcdf4Dimid", "DIMENSION_LIST",
              "REFERENCE_LIST", "CLASS", "NAME")
MSG = "native 3-D: {grid}/{var}"
BIG = 1e15


def source_store():
    import obstore as obs
    return obs.store.S3Store(BUCKET, region=REGION, skip_signature=True)


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


def open_file(path: str):
    import obstore as obs
    return Reader(obs.open_reader(source_store(), path, buffer_size=1024 * 1024))


def chunk_slices(shape, chunks, idx):
    return tuple(slice(i * c, min((i + 1) * c, n)) for i, c, n in zip(idx, chunks, shape))


def sha(a) -> str:
    return hashlib.sha1(np.ascontiguousarray(a, dtype="<f4").tobytes()).hexdigest()


def hash_f8(a) -> str:
    return hashlib.sha1(np.ascontiguousarray(a, dtype="<f8").tobytes()).hexdigest()


def list_files() -> dict:
    """{grid: {var: {year: path}}} for the per-year files of the release (full-period files are the 2-D variables)."""
    out: dict = {g: {} for g in GRIDS}
    for b in source_store().list(prefix=f"{PREFIX}/"):
        for o in b:
            m = FILE_RX.search(o["path"])
            if m and m.group(3) == m.group(4) and f"/{m.group(2)}/{RELEASE}/" in o["path"]:
                out[m.group(2)].setdefault(m.group(1), {})[int(m.group(3))] = o["path"]
    return out


# ------------------------------- Coiled ------------------------------------ #

class remote_cluster:
    def __init__(self, label: str, n: int):
        self.label, self.n = label, n

    def __enter__(self):
        import coiled
        from distributed import Client
        self.cluster = coiled.Cluster(
            name=f"cefi-nep-3d-{self.label}", workspace=WORKSPACE, software=ENV_NAME, n_workers=self.n,
            region=REGION, spot_policy="spot_with_fallback", worker_cpu=2, worker_memory="8GiB",
            worker_options={"nthreads": 1}, scheduler_vm_types=["m6i.2xlarge"], idle_timeout="15 minutes",
            shutdown_on_close=True)
        print(f"  [coiled] cluster up: {self.cluster.name} ({self.n} workers)", flush=True)
        self.client = Client(self.cluster)
        return self.client

    def __exit__(self, *exc):
        self.cluster.close()
        print("  [coiled] cluster closed", flush=True)


# ------------------------------- probe ------------------------------------- #

def _s(v):
    return v.decode() if isinstance(v, bytes) else (v.tolist() if hasattr(v, "tolist") else v)


def probe_task(grid: str, var: str, year: int, path: str) -> dict:
    import traceback
    try:
        return _probe_file(grid, var, year, path)
    except Exception as exc:  # noqa: BLE001
        return {"grid": grid, "var": var, "year": year, "path": path, "error": f"{type(exc).__name__}: {str(exc)[:300]}",
                "trace": traceback.format_exc()[-1500:]}


def _probe_file(grid: str, var: str, year: int, path: str) -> dict:
    import h5py
    f = h5py.File(open_file(path), "r")
    d = f[var]
    assert d.chunks is not None and d.ndim == 4 and d.dtype == np.float32, f"{path}: {d.shape} {d.chunks} {d.dtype}"
    dims = tuple(f[var].dims[i][0].name.lstrip("/") for i in range(4))
    attr_fill = d.attrs.get("_FillValue")
    attr_fill = None if attr_fill is None else float(np.asarray(attr_fill).ravel()[0])
    hdf_fill = float(np.asarray(d.fillvalue).ravel()[0])
    grid_shape = tuple(-(-n // c) for n, c in zip(d.shape, d.chunks))
    expected = int(np.prod(grid_shape))
    time_ds = f["time"]
    out = {"grid": grid, "var": var, "year": year, "path": path, "shape": d.shape, "chunks": d.chunks, "dims": dims,
           "attr_fill": attr_fill, "hdf_fill": hdf_fill, "nchunks": int(d.id.get_num_chunks()), "expected_chunks": expected,
           "time": np.asarray(time_ds[:], "float64"), "time_units": str(_s(time_ds.attrs["units"])),
           "time_calendar": str(_s(time_ds.attrs["calendar"])),
           "attrs": {k: _s(v) for k, v in d.attrs.items() if k not in DROP_ATTRS},
           "bundled": sorted(k for k, v in f.items() if k != var and getattr(v, "ndim", 0) >= 3)}
    out["coords"] = {c: hash_f8(f[c][:]) for c in ("jh", "ih", "jq", "iq", "lat", "lon", "z_l") if c in f and c in dims}
    out["z_l"] = np.asarray(f["z_l"][:], "float64")
    out["z_l_attrs"] = {k: _s(v) for k, v in f["z_l"].attrs.items() if k not in DROP_ATTRS}
    # same random chunk indices for every variable of a (grid, year): lets volcello / vollcello be compared
    rng = np.random.default_rng(int(hashlib.sha1(f"{grid}/{year}".encode()).hexdigest()[:8], 16))
    idxs = {tuple(0 for _ in grid_shape), tuple(n - 1 for n in grid_shape)}
    while len(idxs) < min(N_WINDOWS + 2, expected):
        idxs.add(tuple(int(rng.integers(n)) for n in grid_shape))
    truth, cls = {}, {"nan": 0, "attr": 0, "hdf": 0, "other": 0, "valid": 0}
    for idx in sorted(idxs):
        a = d[chunk_slices(d.shape, d.chunks, idx)]
        truth[idx] = sha(a)
        classify(a, attr_fill, hdf_fill, cls)
    out["truth"], out["classes"] = truth, cls
    if var == "thetao" and year == 1993:
        a = d[0]
        out["mask_day0"] = (np.isfinite(a[0]) & (np.abs(np.nan_to_num(a[0])) < 1e19),
                            (np.isfinite(a) & (np.abs(np.nan_to_num(a)) < 1e19)).any(axis=0))
    return out


def classify(a, attr_fill, hdf_fill, cls) -> None:
    nan = np.isnan(a)
    at = (a == np.float32(attr_fill)) if attr_fill is not None and not np.isnan(attr_fill) else np.zeros(a.shape, bool)
    hd = (a == np.float32(hdf_fill)) if not np.isnan(hdf_fill) else np.zeros(a.shape, bool)
    big = np.abs(np.nan_to_num(a, nan=0.0)) > BIG
    cls["nan"] += int(nan.sum())
    cls["attr"] += int(at.sum())
    cls["hdf"] += int((hd & ~at).sum())
    cls["other"] += int((big & ~at & ~hd).sum())
    cls["valid"] += int((~nan & ~big).sum())


def phase_probe(n_workers: int) -> None:
    from distributed import as_completed
    files = list_files()
    PROBE_DIR.mkdir(parents=True, exist_ok=True)
    jobs = [(g, v, y, p) for g in GRIDS for v, ys in sorted(files[g].items()) for y, p in sorted(ys.items())]
    done = set()
    for f in PROBE_DIR.glob("*.pkl"):
        r = pickle.loads(f.read_bytes())
        if "error" not in r:
            done.add((r["grid"], r["var"], r["year"]))
    todo = [j for j in jobs if j[:3] not in done]
    print(f"[probe] {len(jobs)} files, {len(done)} cached, {len(todo)} to do", flush=True)
    t0, n = time.time(), 0
    if todo:
        with remote_cluster("probe", n_workers) as client:
            futs = {client.submit(probe_task, *j, retries=1, pure=False): j for j in todo}
            for fut in as_completed(list(futs), timeout=45 * 60):
                r = fut.result()
                (PROBE_DIR / f"{r['grid']}__{r['var']}__{r['year']}.pkl").write_bytes(pickle.dumps(r))
                n += 1
                if "error" in r:
                    print(f"  FAILED {r['path']}: {r['error']}", flush=True)
                if n % 50 == 0 or n == len(todo):
                    print(f"  [probe] {n}/{len(todo)} ({time.time() - t0:.0f}s)", flush=True)
    bad = [r for r in load_probe(allow_errors=True) if "error" in r]
    print(f"[probe] done; {len(bad)} failed", flush=True)
    if bad:
        sys.exit(2)


def load_probe(allow_errors: bool = False) -> list[dict]:
    out = [pickle.loads(f.read_bytes()) for f in sorted(PROBE_DIR.glob("*.pkl"))]
    assert allow_errors or not [r for r in out if "error" in r], "probe cache has failed files; rerun --probe"
    return out


# ------------------------------- plan -------------------------------------- #

def decode_days(units: str, values) -> np.ndarray:
    base = pd.Timestamp(units.split("since ")[1].strip().split()[0])
    v = np.asarray(values, "float64")
    return (base + pd.to_timedelta(v.ravel(), unit="D")).values.reshape(v.shape)


def sentinel(rs: list[dict]) -> tuple[str, float]:
    """The one 'no data' value this array really stores, from the sampled chunks of every year file."""
    for r in rs:
        assert r["nchunks"] == r["expected_chunks"], f"{r['path']}: unwritten chunks ({r['nchunks']} of {r['expected_chunks']})"
    k = {x: sum(r["classes"][x] for r in rs) for x in ("nan", "attr", "hdf", "other", "valid")}
    assert k["other"] == 0, f"{rs[0]['grid']}/{rs[0]['var']}: stored values that are neither valid nor the fill: {k}"
    attr = {repr(r["attr_fill"]) for r in rs}
    hdf = {r["hdf_fill"] for r in rs if not np.isnan(r["hdf_fill"])}
    assert len(attr) == 1, f"attr fill differs between files: {attr}"
    kinds = [kk for kk in ("nan", "attr", "hdf") if k[kk]]
    assert len(kinds) <= 1, f"{rs[0]['grid']}/{rs[0]['var']}: more than one 'no data' sentinel stored: {k}"
    a = rs[0]["attr_fill"]
    kind = kinds[0] if kinds else ("nan" if a is None or np.isnan(a) else "attr")
    value = {"attr": a, "hdf": next(iter(hdf), None), "nan": float("nan")}[kind]
    return kind, float(value)


def build_plan() -> dict:
    """Checks every cross-file / cross-repo invariant; returns the write plan per (grid, var)."""
    import xarray as xr
    from arraylake import Client
    results = load_probe()
    by = {}
    for r in results:
        by.setdefault((r["grid"], r["var"]), {})[r["year"]] = r
    repo = Client().get_repo(REPO)
    store = repo.readonly_session(branch="main").store
    plan = {}
    for grid in GRIDS:
        ds = xr.open_zarr(store, group=grid, consolidated=False, zarr_format=3)
        repo_time = ds["time"].values
        assert repo_time.size == 12053
        offsets = {y: int(((pd.Timestamp(f"{y}-01-01") - pd.Timestamp(repo_time[0]).normalize()).days)) for y in range(1993, 2026)}
        zl = None
        for (g, var), per in sorted(by.items()):
            if g != grid:
                continue
            assert sorted(per) == list(range(1993, 2026)), f"{g}/{var}: years {sorted(per)}"
            rs = [per[y] for y in sorted(per)]
            for key in ("shape", "chunks", "dims", "attrs", "time_units", "time_calendar"):
                if key == "shape":
                    vals = {r["shape"][1:] for r in rs}
                else:
                    vals = {json.dumps(r[key], sort_keys=True, default=str) for r in rs}
                assert len(vals) == 1, f"{g}/{var}: {key} differs between years: {vals}"
            assert rs[0]["dims"][:2] == ("time", "z_l"), rs[0]["dims"]
            for r in rs:
                n = r["shape"][0]
                assert n == (366 if r["year"] % 4 == 0 else 365), f"{r['path']}: {n} steps"
                got = decode_days(r["time_units"], r["time"])
                want = repo_time[offsets[r["year"]]:offsets[r["year"]] + n]
                assert got.shape == want.shape and (got == want).all(), f"{r['path']}: time axis != the repo's"
            for c in set().union(*(r["coords"] for r in rs)):
                hs = {r["coords"].get(c) for r in rs}
                assert len(hs) == 1, f"{g}/{var}: coordinate {c} differs between years"
                if c != "z_l":
                    assert hash_f8(ds[c].values) in hs, f"{g}/{var}: coordinate {c} differs from the repo's"
            zs = {hash_f8(r["z_l"]) for r in rs}
            assert len(zs) == 1
            zl = zl or (rs[0]["z_l"], rs[0]["z_l_attrs"])
            assert hash_f8(zl[0]) in zs, "z_l differs between variables"
            kind, value = sentinel(rs)
            plan[(grid, var)] = {"grid": grid, "var": var, "dims": rs[0]["dims"], "shape": (12053, *rs[0]["shape"][1:]),
                                 "attrs": rs[0]["attrs"], "kind": kind, "fill": value, "offsets": offsets,
                                 "files": {y: r["path"] for y, r in per.items()}, "year_len": {y: r["shape"][0] for y, r in per.items()},
                                 "src_chunks": rs[0]["chunks"], "z_l": zl[0], "z_l_attrs": zl[1],
                                 "truth": {y: r["truth"] for y, r in per.items()}, "bundled": sorted({b for r in rs for b in r["bundled"]})}
            print(f"  [{g}/{var}] dims {rs[0]['dims']}, 33 files, sentinel {kind} ({value:g}), "
                  f"sampled {sum(r['classes']['valid'] for r in rs):,} valid / "
                  f"{sum(r['classes']['nan'] + r['classes']['attr'] + r['classes']['hdf'] for r in rs):,} no-data values", flush=True)
    mask_check(plan, by, repo)
    drop = vollcello_decision(plan)
    for k in drop:
        plan.pop(k)
    return plan


def mask_check(plan, by, repo) -> None:
    """Raw: where the 3-D data has values must agree with the repo's static land/sea mask (thetao, 1993-01-01)."""
    import xarray as xr
    r = by[("raw", "thetao")][1993]
    surf, anylev = r["mask_day0"]
    ds = xr.open_zarr(repo.readonly_session(branch="main").store, group="raw", consolidated=False, zarr_format=3)
    wet = ds["wet"].values
    ocean = np.isfinite(wet) & (wet == 1)
    bad = {"surface data but not wet": int((surf & ~ocean).sum()), "wet but no surface data": int((ocean & ~surf).sum()),
           "data at some level but not wet": int((anylev & ~ocean).sum())}
    print(f"  [mask] raw thetao 1993-01-01: surface valid {int(surf.sum())}, wet==1 {int(ocean.sum())}; mismatches {bad}", flush=True)
    assert not any(bad.values()), f"the repo's static mask does not match the 3-D data: {bad}"


def vollcello_decision(plan) -> list:
    drop = []
    for grid in GRIDS:
        a, b = plan.get((grid, "volcello")), plan.get((grid, "vollcello"))
        if not (a and b):
            continue
        n = same = 0
        for y in a["truth"]:
            for idx, h in a["truth"][y].items():
                n += 1
                same += b["truth"][y].get(idx) == h
        print(f"  [{grid}] volcello vs vollcello: {same}/{n} sampled source chunk windows are identical", flush=True)
        if same == n and a["attrs"].get("long_name") == b["attrs"].get("long_name"):
            drop.append((grid, "vollcello"))
        else:
            print(f"  [{grid}] !! vollcello differs from volcello: both are kept under their source names", flush=True)
    if drop:
        print(f"  vollcello is a byte-identical copy of volcello in every sampled window: not ingested again ({drop})", flush=True)
    return drop


# ------------------------------- create ------------------------------------ #

def patch_coordinates(store, group: str) -> None:
    """Same `coordinates` attribute rule as the 2-D arrays of the group (see ingest_nep_hindcast_daily.py)."""
    import zarr
    g = zarr.open_group(store, mode="r+", path=group)
    arrays = dict(g.arrays())
    dims_of = lambda a: tuple(a.metadata.dimension_names or ())  # noqa: E731
    if "geolon" in arrays and "geolat" in arrays:
        geo = set(dims_of(arrays["geolon"]))
        for name, a in arrays.items():
            if name not in ("geolon", "geolat") and geo <= set(dims_of(a)):
                a.attrs["coordinates"] = "geolon geolat"
    else:
        coord_names = {n for n, a in arrays.items() if len(dims_of(a)) == 1 and dims_of(a)[0] == n}
        for name, a in arrays.items():
            cs = [d for d in dims_of(a) if d in coord_names]
            if name not in coord_names and cs:
                a.attrs["coordinates"] = " ".join(cs)


def fill_json(kind: str, value: float):
    return float("nan") if kind == "nan" else float(np.float32(value))


def fill_attrs(p: dict) -> dict:
    """Source attrs plus, for a non-NaN sentinel, `_FillValue`/`missing_value` as the virtual 2-D arrays carry them: xarray
    does NOT mask from a zarr v3 `fill_value` alone (found by the consumer check: 1e20 came back as data)."""
    out = dict(p["attrs"])
    if p["kind"] != "nan":
        v = float(np.float32(p["fill"]))
        # same encoding as the virtual 2-D arrays: `_FillValue` as base64 of the little-endian float64 (zarr v3 convention
        # xarray decodes), `missing_value` as a plain number
        out["_FillValue"] = base64.b64encode(np.float64(v).tobytes()).decode()
        out["missing_value"] = v
    return out


def create_arrays(store, plan: dict) -> None:
    import zarr
    from zarr.codecs import BloscCodec
    codec = BloscCodec(cname="zstd", clevel=3, shuffle="shuffle")
    for grid in GRIDS:
        g = zarr.open_group(store, mode="r+", path=grid)
        items = [p for (gg, _), p in sorted(plan.items()) if gg == grid]
        z = items[0]
        if "z_l" not in g:
            a = g.create_array("z_l", shape=z["z_l"].shape, chunks=z["z_l"].shape, dtype="float64", dimension_names=("z_l",),
                               attributes=dict(z["z_l_attrs"]), fill_value=float("nan"))
            a[:] = z["z_l"]
        for p in items:
            assert p["var"] not in g, f"{grid}/{p['var']} already exists"
            g.create_array(p["var"], shape=p["shape"], chunks=ZCHUNKS, dtype="float32", dimension_names=p["dims"],
                           fill_value=fill_json(p["kind"], p["fill"]), compressors=codec, attributes=fill_attrs(p))
        patch_coordinates(store, grid)


def phase_create() -> None:
    from arraylake import Client
    plan = build_plan()
    repo = Client().get_repo(REPO)
    session = repo.writable_session("main")
    create_arrays(session.store, plan)
    snap = session.commit(f"Add native placeholder arrays for the per-year 3-D variables ({len(plan)} arrays, raw + regrid); "
                          "data written variable by variable")
    print(f"committed {snap}", flush=True)


# ------------------------------- write ------------------------------------- #

def windows(p: dict):
    """(time0, time1, z0, z1) tasks: 90 days x 10 levels, aligned to the zarr chunk grid."""
    nz = p["shape"][1]
    for t0 in range(0, p["shape"][0], TASK_DAYS):
        for z0 in range(0, nz, TASK_LEVELS):
            yield t0, min(t0 + TASK_DAYS, p["shape"][0]), z0, min(z0 + TASK_LEVELS, nz)


def pieces(p: dict, t0: int, t1: int) -> list:
    out = []
    for y, off in sorted(p["offsets"].items()):
        a, b = max(t0, off), min(t1, off + p["year_len"][y])
        if a < b:
            out.append((p["files"][y], a - off, b - off))
    return out


def write_task(fork, p: dict, t0: int, t1: int, z0: int, z1: int):
    import xarray as xr
    parts = []
    for path, l0, l1 in pieces(p, t0, t1):
        with xr.open_dataset(open_file(path), engine="h5netcdf", decode_cf=False, mask_and_scale=False,
                             decode_times=False, chunks=None) as ds:
            parts.append(ds[p["var"]].isel(time=slice(l0, l1), z_l=slice(z0, z1)).values)
    a = np.concatenate(parts, axis=0) if len(parts) > 1 else parts[0]
    del parts
    assert a.shape[:2] == (t1 - t0, z1 - z0) and a.dtype == np.float32, (a.shape, a.dtype)
    cls = {"nan": 0, "attr": 0, "hdf": 0, "other": 0, "valid": 0}
    value = None if p["kind"] == "nan" else p["fill"]
    for i in range(0, a.shape[0], 5):  # 5-day slices: the mask temporaries stay small (peak memory)
        classify(a[i:i + 5], value, float("nan"), cls)
    n_fill = cls["nan"] if p["kind"] == "nan" else cls["attr"] + cls["hdf"]
    stats = {"n": int(a.size), "no_data": n_fill, "valid": cls["valid"], "other": cls["other"]}
    assert cls["other"] == 0 and stats["no_data"] + stats["valid"] == stats["n"], (p["grid"], p["var"], t0, z0, cls)
    if p["kind"] != "nan":
        assert cls["nan"] == 0, (p["grid"], p["var"], t0, z0, "NaN in an array whose sentinel is not NaN")
    xr.Dataset({p["var"]: (p["dims"], a)}).to_zarr(
        fork.store, group=p["grid"], mode="r+", region={"time": slice(t0, t1), "z_l": slice(z0, z1)},
        consolidated=False, zarr_format=3)
    return fork, (t0, z0), stats


def done_vars(repo) -> set:
    return {c.message for c in repo.ancestry(branch="main")}


def phase_write(only: list[str] | None, n_workers: int) -> None:
    from arraylake import Client
    plan = {k: v for k, v in build_plan().items() if not only or f"{k[0]}/{k[1]}" in only}
    assert plan, "nothing selected"
    repo = Client().get_repo(REPO)
    STATS_DIR.mkdir(parents=True, exist_ok=True)
    todo = [k for k in sorted(plan) if MSG.format(grid=k[0], var=k[1]) not in done_vars(repo)]
    print(f"[write] {len(plan)} arrays selected, {len(plan) - len(todo)} already committed, {len(todo)} to write", flush=True)
    if not todo:
        return
    with remote_cluster("write", n_workers) as client:
        for grid, var in todo:
            p = plan[(grid, var)]
            t0 = time.time()
            repo = Client().get_repo(REPO)  # fresh Arraylake token per variable: workers cannot refresh the one they are sent
            session = repo.writable_session("main")
            wins = list(windows(p))
            futs = [client.submit(write_task, session.fork(), p, *w, pure=False, retries=2) for w in wins]
            res = [f.result() for f in futs]
            tot = {k: sum(r[2][k] for r in res) for k in ("n", "no_data", "valid", "other")}
            want = int(np.prod(p["shape"]))
            assert tot["n"] == want and tot["other"] == 0, f"{grid}/{var}: classified {tot['n']} of {want} values, {tot}"
            session.merge(*[r[0] for r in res])
            snap = session.commit(MSG.format(grid=grid, var=var))
            (STATS_DIR / f"{grid}__{var}.json").write_text(json.dumps({**tot, "expected": want, "snapshot": snap}))
            print(f"  [{grid}/{var}] {len(wins)} tasks, {tot['n']:,} values ({tot['no_data'] / tot['n']:.1%} no-data), "
                  f"committed {snap} ({time.time() - t0:.0f}s)", flush=True)


# ------------------------------- validate ---------------------------------- #

def window_task(store, grid: str, var: str, items: list) -> list:
    import zarr
    arr = zarr.open_group(store, mode="r", path=grid)[var]
    bad = []
    for off, idx, want, src_chunks, ny in items:
        shape = (ny, *arr.shape[1:])
        sl = chunk_slices(shape, src_chunks, idx)
        sl = (slice(sl[0].start + off, sl[0].stop + off), *sl[1:])
        for attempt in range(4):
            try:
                a = arr[sl]
                break
            except Exception:  # noqa: BLE001
                if attempt == 3:
                    raise
        if sha(a) != want:
            bad.append((grid, var, off, idx))
    return bad


def phase_validate(only: list[str] | None, n_workers: int) -> None:
    from arraylake import Client
    plan = {k: v for k, v in build_plan().items() if not only or f"{k[0]}/{k[1]}" in only}
    repo = Client().get_repo(REPO)
    missing = [k for k in plan if MSG.format(grid=k[0], var=k[1]) not in done_vars(repo)]
    assert not missing, f"not committed yet: {missing}"
    for (grid, var), p in sorted(plan.items()):
        s = json.loads((STATS_DIR / f"{grid}__{var}.json").read_text())
        assert s["n"] == s["expected"] == int(np.prod(p["shape"])) and s["other"] == 0, (grid, var, s)
    store = repo.readonly_session(branch="main").store
    batches = []
    for (grid, var), p in sorted(plan.items()):
        items = [(p["offsets"][y], idx, h, p["src_chunks"], p["year_len"][y]) for y, tr in p["truth"].items() for idx, h in tr.items()]
        batches += [(grid, var, items[i:i + 40]) for i in range(0, len(items), 40)]
    print(f"[validate] element counts match for {len(plan)} arrays; {sum(len(b[2]) for b in batches)} source windows in "
          f"{len(batches)} batches", flush=True)
    with remote_cluster("validate", n_workers) as client:
        futs = [client.submit(window_task, store, g, v, items, pure=False, retries=2) for g, v, items in batches]
        bad = [x for f in futs for x in f.result()]
    assert not bad, f"{len(bad)} windows differ: {bad[:5]}"
    print(f"  OK: all sampled source chunk windows match the committed store for {len(plan)} arrays", flush=True)


def phase_fix_fill_attrs() -> None:
    import zarr
    from arraylake import Client
    plan = build_plan()
    repo = Client().get_repo(REPO)
    session = repo.writable_session("main")
    n = 0
    for (grid, var), p in sorted(plan.items()):
        want = fill_attrs(p)
        arr = zarr.open_group(session.store, mode="r+", path=grid)[var]
        for k in ("_FillValue", "missing_value"):
            if k in want and arr.attrs.get(k) != want[k]:
                arr.attrs[k] = want[k]
                n += 1
    print(f"set {n} attributes", flush=True)
    if n:
        print(f"committed {session.commit('Add _FillValue/missing_value attributes to the raw native 3-D arrays (xarray masks from them)')}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    for n in ("probe", "plan", "create", "write", "validate", "fix-fill-attrs"):
        g.add_argument(f"--{n}", action="store_true")
    ap.add_argument("--only", nargs="*", help="--write/--validate: grid/var names, e.g. regrid/so")
    ap.add_argument("--workers", type=int, default=120)
    a = ap.parse_args()
    if a.probe:
        phase_probe(min(a.workers, 16))
    elif a.plan:
        plan = build_plan()
        print(f"{len(plan)} arrays: {sorted(plan)}")
    elif a.create:
        phase_create()
    elif a.fix_fill_attrs:
        phase_fix_fill_attrs()
    elif a.write:
        phase_write(a.only, a.workers)
    else:
        phase_validate(a.only, min(a.workers, 40))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
