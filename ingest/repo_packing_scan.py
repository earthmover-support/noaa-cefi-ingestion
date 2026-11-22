#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["arraylake>=0.16", "icechunk>=1.2", "zarr>=3", "numpy"]
# ///
"""Read-only: list every array in the noaa-cefi repos carrying CF packing attributes (scale_factor / add_offset)."""
import collections
import zarr
from arraylake import Client

KEYS = ("scale_factor", "add_offset", "scale", "offset", "packing", "valid_range", "valid_min", "valid_max")
c = Client()
for r in sorted(x.name if hasattr(x, "name") else str(x) for x in c.list_repos("noaa-cefi")):
    name = r if "/" in r else f"noaa-cefi/{r}"
    repo = c.get_repo(name)
    root = zarr.open_group(repo.readonly_session(branch="main").store, mode="r")
    n = 0
    hits = collections.defaultdict(list)
    dtypes = collections.Counter()
    for path, node in root.members(max_depth=None):
        if isinstance(node, zarr.Array):
            n += 1
            dtypes[str(node.dtype)] += 1
            for k in KEYS:
                if k in node.attrs:
                    hits[k].append((path, node.attrs[k]))
    print(f"{name}: {n} arrays, dtypes {dict(dtypes)}")
    for k, v in hits.items():
        print(f"   {k}: {len(v)} arrays, e.g. {v[:3]}")
