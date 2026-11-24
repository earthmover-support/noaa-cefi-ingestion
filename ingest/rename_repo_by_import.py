#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["arraylake>=1.9", "icechunk>=2.0.5", "zarr>=3", "numpy", "xarray"]
# ///
"""Rename an Arraylake repo without copying its data: remove its Arraylake record (keeping the bucket bytes) and import the same Icechunk
storage under the new name.

Arraylake has no rename, and `import_repo` refuses a storage prefix that another record owns (409 prefix_overlap), hence the order:
 1. tag the old repo's main (`before-rename`) so the starting point stays addressable,
 2. delete the old record with `delete_repo(immediate=True, retain_data=True)` (bytes are left alone),
 3. `import_repo(new name, same bucket, same prefix)`; if that fails, re-import under the OLD name so nothing is lost,
 4. verify: same `main` snapshot, same history, a native array and a virtual array read through the new record.
Tested on a throwaway repo first (the snapshot and data came through unchanged). Used for cefi-nep-hindcast-daily -> nep10k-hindcast, whose
4.2 TB of native 3-D chunks would otherwise have to be downloaded and uploaded again.
Usage: rename_repo_by_import.py OLD NEW [--yes]   (without --yes it only prints what it would do)
"""
import argparse
import sys

BUCKET = "noaa-oar-cefi-regional-mom6-pds"
SOURCE_URL = f"s3://{BUCKET}"
SOURCE_NICKNAME = f"{BUCKET}-source"


def main() -> int:
    import numpy as np
    import xarray as xr
    import zarr
    from arraylake import Client

    ap = argparse.ArgumentParser()
    ap.add_argument("old")
    ap.add_argument("new")
    ap.add_argument("--yes", action="store_true")
    a = ap.parse_args()
    old, new = (n if "/" in n else f"noaa-cefi/{n}" for n in (a.old, a.new))
    c = Client()
    ro = c.get_repo_object(old)
    repo = c.get_repo(old)
    tip = repo.lookup_branch("main")
    n_snapshots = len(list(repo.ancestry(branch="main")))
    nickname, prefix = ro.bucket.nickname, ro.prefix.split("/", 1)[1]  # import_repo's prefix is relative to the bucket's configured prefix
    meta, desc = dict(ro.metadata or {}), ro.description
    print(f"{old}: bucket {nickname}, prefix {ro.prefix}, main {tip}, {n_snapshots} snapshots, {len(meta)} metadata keys")
    print(f"plan: tag main as 'before-rename', delete the record of {old} (retain_data=True), import the same prefix as {new}")
    if not a.yes:
        print("dry run: nothing changed (use --yes)")
        return 0

    repo.create_tag("before-rename", tip)
    c.delete_repo(old, imsure=True, imreallysure=True, immediate=True, retain_data=True)
    print(f"deleted the record of {old} (bucket bytes retained)", flush=True)
    try:
        c.import_repo(new, bucket_config_nickname=nickname, prefix=prefix, description=desc, metadata=meta,
                      authorize_virtual_chunk_access={SOURCE_URL: SOURCE_NICKNAME})
    except Exception as exc:  # noqa: BLE001
        print(f"IMPORT FAILED ({exc}); restoring the record under the old name", file=sys.stderr)
        c.import_repo(old, bucket_config_nickname=nickname, prefix=prefix, description=desc, metadata=meta,
                      authorize_virtual_chunk_access={SOURCE_URL: SOURCE_NICKNAME})
        raise
    print(f"imported as {new}", flush=True)

    n = c.get_repo(new)
    assert n.lookup_branch("main") == tip, "main differs after the import"
    assert len(list(n.ancestry(branch="main"))) == n_snapshots, "history differs after the import"
    store = n.readonly_session(branch="main").store
    root = zarr.open_group(store, mode="r")
    print(f"{new}: main {tip}, {n_snapshots} snapshots, tag before-rename present: {'before-rename' in n.list_tags()}", flush=True)
    ds = xr.open_zarr(store, group="raw", consolidated=False, zarr_format=3)
    v2 = next(v for v in ds.data_vars if ds[v].ndim == 3)
    print(f"read through the new record: {v2} (virtual) first-step mean {float(ds[v2].isel(time=0).mean()):.4f}", flush=True)
    v4 = next((v for v in ds.data_vars if ds[v].ndim == 4), None)
    if v4:
        print(f"read through the new record: {v4} (native) mean over a small slab {float(np.nanmean(ds[v4].isel(time=0, z_l=0).values[:100, :100])):.4f}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
