#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "virtualizarr[hdf] @ git+https://github.com/zarr-developers/VirtualiZarr.git@de4e669f18e323b2814f0625785dbd5766d65652",
#   "icechunk>=1.2", "obstore>=0.5", "obspec-utils>=0.2", "xarray", "zarr>=3", "numpy", "pandas", "h5py", "h5netcdf",
# ]
# ///
"""Prototype: open 3 inits x 6 vars of NWA decadal yearly raw as virtual datasets and inspect."""
import time
import obstore as obs, virtualizarr as vz, xarray as xr
from obspec_utils.registry import ObjectStoreRegistry
from virtualizarr.parsers import HDFParser

B = "s3://noaa-oar-cefi-regional-mom6-pds"
store = obs.store.S3Store("noaa-oar-cefi-regional-mom6-pds", region="us-east-1", skip_signature=True)
reg = ObjectStoreRegistry({B: store})
pre = "northwest_atlantic/full_domain/decadal_forecast/yearly/raw/r20250819/"
paths = sorted(o["path"] for b in store.list(prefix=pre) for o in b if o["path"].endswith(".nc"))
print(len(paths))
inits = ["i196501", "i196601", "i202501"]
sel = [p for p in paths if any(i in p for i in inits)]
LOAD = ["lead", "member", "xh", "yh", "nv", "average_T1", "average_T2", "average_DT", "time_bnds"]
t = time.time()
for p in sel[:6]:
    ds = vz.open_virtual_dataset(f"{B}/{p}", registry=reg, parser=HDFParser(), loadable_variables=LOAD, decode_times=False)
    print(p.rsplit("/", 1)[1], round(time.time() - t, 1), "s")
print(ds)
for v in ds.data_vars:
    print(v, ds[v].dims, ds[v].shape, ds[v].attrs.get("units"))
print(ds["lead"].values, ds["lead"].attrs)
