#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["arraylake>=0.16", "icechunk>=1.2", "zarr>=3", "numpy"]
# ///
"""Read-only inventory of repos in `noaa-cefi`: per group, the arrays (dims, dtype), the static-like and coordinate-like names,
and how each data variable's `coordinates` attribute points at them. Usage: inventory_repos.py REPO [REPO ...]"""
import collections
import sys

import zarr
from arraylake import Client

STATIC_HINT = {"geolon", "geolat", "geolon_c", "geolat_c", "geolon_u", "geolat_u", "geolon_v", "geolat_v", "deptho", "areacello",
               "areacello_bu", "areacello_cu", "areacello_cv", "wet", "wet_c", "wet_u", "wet_v", "sftof", "Coriolis", "dxt", "dyt",
               "dxCu", "dxCv", "dyCu", "dyCv", "CELL_AREA", "COSROT", "SINROT"}


def main(names):
    c = Client()
    for name in names:
        full = name if "/" in name else f"noaa-cefi/{name}"
        repo = c.get_repo(full)
        root = zarr.open_group(repo.readonly_session(branch="main").store, mode="r")
        groups = collections.defaultdict(dict)
        for path, node in root.members(max_depth=None):
            if isinstance(node, zarr.Array):
                g, _, n = path.rpartition("/")
                groups[g][n] = node
        print(f"\n=== {full}: {sum(len(v) for v in groups.values())} arrays in {len(groups)} groups")
        for g in sorted(groups):
            arrs = groups[g]
            dims = collections.Counter(tuple(a.metadata.dimension_names or ()) for a in arrs.values())
            statics = sorted(n for n in arrs if n in STATIC_HINT)
            coordattrs = collections.Counter(str(a.attrs.get("coordinates")) for a in arrs.values() if a.ndim >= 2 and a.attrs.get("coordinates"))
            print(f"  /{g}: {len(arrs)} arrays; statics present: {statics}")
            print(f"     dim signatures: {dict(dims.most_common(6))}")
            print(f"     coordinates attrs: {dict(coordattrs.most_common(4))}")
            print(f"     1-D/scalar arrays: {sorted(n for n, a in arrs.items() if a.ndim <= 1)}")


if __name__ == "__main__":
    main(sys.argv[1:])
