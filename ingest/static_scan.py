#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["obstore>=0.5", "scipy", "numpy"]
# ///
"""Follow-up to scan_packing_attrs.py: read the files h5py could not open (netCDF3 statics) with scipy, and list the
integer / big-endian variables found. Reads packing_scan.json written by scan_packing_attrs.py."""
import collections
import io
import json

import obstore as obs
from scipy.io import netcdf_file

R = json.load(open("packing_scan.json"))
store = obs.store.S3Store("noaa-oar-cefi-regional-mom6-pds", region="us-east-1", skip_signature=True)
KEYS = ("scale_factor", "add_offset", "scale", "offset", "valid_range", "valid_min", "valid_max", "packing")
for r in [r for r in R if "error" in r]:
    p = r["path"]
    f = netcdf_file(io.BytesIO(bytes(obs.get(store, p).bytes())), mmap=False)
    hits = {n: {k: getattr(v, k) for k in KEYS if hasattr(v, k)} for n, v in f.variables.items()}
    hits = {n: h for n, h in hits.items() if h}
    print(p.split("/")[-4:-1], p.split("/")[-1], "vars", len(f.variables), "dtypes",
          sorted({str(v.data.dtype) for v in f.variables.values()}), "packing attrs:", hits or "none")
ints = collections.defaultdict(set)
for r in R:
    for n, d in r.get("datasets", {}).items():
        if d["dtype"] in ("int8", "int32", "int64", ">f4"):
            ints[d["dtype"]].add((r["product"], n))
for k, v in ints.items():
    print(k, sorted(v)[:12], len(v))
