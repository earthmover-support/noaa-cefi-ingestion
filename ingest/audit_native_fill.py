#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["arraylake>=1.9", "icechunk>=2.0.5", "zarr>=3", "numpy", "obstore>=0.5", "scipy", "xarray", "h5py"]
# ///
"""Read-only audit: in every repo of `noaa-cefi`, find natively stored float arrays that contain the literal 1e20 'no data' fill
(values with |x| > 1e19 that are not NaN). xarray returns those as data when the array's `_FillValue` says NaN.
Usage: audit_native_fill.py [REPO ...]  (default: all repos in the org)"""
import sys
from pathlib import Path

import numpy as np
import zarr
from arraylake import Client

sys.path.insert(0, str(Path(__file__).resolve().parent))
import restructure_nwa12_seasonal as base  # noqa: E402


def main(names):
    c = Client()
    names = names or sorted(r.name for r in c.list_repos("noaa-cefi"))
    for name in names:
        full = name if "/" in name else f"noaa-cefi/{name}"
        s = c.get_repo(full).readonly_session(branch="main")
        root = zarr.open_group(s.store, mode="r")
        bad, checked = [], 0
        for path, node in root.members(max_depth=None):
            if not isinstance(node, zarr.Array) or node.dtype.kind != "f" or node.size > 5e7:
                continue
            table = base._chunk_table(s.store, path)
            if not table or any(r[0] == "virtual" for r in table.values()):
                continue
            checked += 1
            a = node[...]
            n = int((np.abs(a) > 1e19).sum())
            if n:
                bad.append((path, n, str(dict(node.attrs).get("_FillValue"))))
        print(f"{full}: {checked} native float arrays checked; with 1e20 fill values: {len(bad)}", flush=True)
        for path, n, fv in bad:
            print(f"    {path}: {n} cells, _FillValue attr {fv}", flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
