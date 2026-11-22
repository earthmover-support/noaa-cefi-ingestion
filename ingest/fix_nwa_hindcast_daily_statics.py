#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["arraylake>=0.16", "icechunk>=1.2", "obstore>=0.5", "xarray", "zarr>=3", "numpy", "h5py"]
# ///
"""Replace raw deptho/areacello/wet of `noaa-cefi/cefi-nwa-hindcast-daily` (user decision 2026-10-26).

Why: that repo was written with the NWA hindcast r20250715 `ocean_static.nc`, whose land/sea mask marks 288 cells as land
where the hindcast data has values (data cells 391,722, static ocean cells 391,434). The NWA decadal-yearly r20250819 static
matches the data exactly. The three mask fields are replaced by the decadal static's (1e20 'no data' -> NaN), the same rule
as the other NWA repos. Native arrays only: no virtual data is touched. The old commit stays in history.

Dry-run by default (compares, writes nothing); pass --apply to commit.
"""
from __future__ import annotations

import argparse
import io

import h5py
import numpy as np
import obstore as obs
import xarray as xr
import zarr
from arraylake import Client

REPO = "noaa-cefi/cefi-nwa-hindcast-daily"
BUCKET = "noaa-oar-cefi-regional-mom6-pds"
DECADAL_STATIC = "northwest_atlantic/full_domain/decadal_forecast/yearly/raw/r20250819/ocean_static.nc"
FIELDS = ("deptho", "areacello", "wet")
NOTE = ("from the NWA decadal-forecast r20250819 ocean_static.nc: the hindcast's own ocean_static.nc marks 288 cells as land "
        "where the data has values; that model's land/sea mask matches the data exactly. 'no data' fill stored as NaN")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="commit the change (default: compare only)")
    args = ap.parse_args()

    store = obs.store.S3Store(BUCKET, region="us-east-1", skip_signature=True)
    dec = h5py.File(io.BytesIO(bytes(obs.get(store, DECADAL_STATIC).bytes())), "r")
    new = {}
    for n in FIELDS:
        a = np.array(dec[n][:], dtype="float32")
        new[n] = np.where(np.abs(a) > 1e19, np.float32(np.nan), a)

    repo = Client().get_repo(REPO)
    ro = xr.open_zarr(repo.readonly_session(branch="main").store, group="raw", consolidated=False, zarr_format=3)
    for d in ("xh", "yh"):
        assert np.array_equal(np.asarray(dec[d][:], "float64"), ro[d].values), f"decadal static {d} != the repo's {d}"
    var = next(n for n in ("tos", "btm_o2") if n in ro)
    data_ok = ro[var].isel(time=0).notnull().values
    print(f"data cells (first day of {var}): {int(data_ok.sum())}")
    for label, wet, dep in (("current repo", ro["wet"].values, ro["deptho"].values), ("decadal static", new["wet"], new["deptho"])):
        ocean = np.isfinite(wet) & (wet == 1)
        has_depth = np.isfinite(dep) & (dep > 0) & (dep < 1e19)
        print(f"  {label:15s}: wet==1 on {int(ocean.sum())}, deptho valid on {int(has_depth.sum())} | data-but-not-wet {int((data_ok & ~ocean).sum())}, "
              f"wet-but-no-data {int((ocean & ~data_ok).sum())}, data-but-no-depth {int((data_ok & ~has_depth).sum())}")
    ocean_new = np.isfinite(new["wet"]) & (new["wet"] == 1)
    dep_new = np.isfinite(new["deptho"]) & (new["deptho"] > 0)
    assert (data_ok == ocean_new).all() and (data_ok == dep_new).all(), "the decadal static does not match this repo's data mask"
    print("decadal static matches the data mask exactly (0 mismatches).")
    if not args.apply:
        print("dry run: nothing written (use --apply)")
        return 0

    session = repo.writable_session("main")
    g = zarr.open_group(session.store, mode="r+", path="raw")
    for n in FIELDS:
        arr = g[n]
        assert arr.shape == new[n].shape and arr.dtype == new[n].dtype, (n, arr.shape, arr.dtype)
        arr[...] = new[n]
        arr.attrs["comment"] = NOTE
    snap = session.commit("Replace raw deptho/areacello/wet with the NWA decadal-forecast static (its land/sea mask matches the data; "
                          "the hindcast static's mask is wrong at 288 cells)")
    print(f"committed {snap}")
    chk = xr.open_zarr(repo.readonly_session(branch="main").store, group="raw", consolidated=False, zarr_format=3)
    for n in FIELDS:
        assert np.array_equal(chk[n].values, new[n], equal_nan=True), n
    ocean = np.isfinite(chk["wet"].values) & (chk["wet"].values == 1)
    assert (ocean == data_ok).all()
    print("read back from the committed repo: all three fields equal the new arrays; wet==1 matches the data mask on every cell.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
