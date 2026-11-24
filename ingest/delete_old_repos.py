#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["arraylake>=1.9", "icechunk>=2.0.5", "obstore>=0.5", "zarr>=3", "numpy", "xarray"]
# ///
"""Delete the ten old `cefi-*` repos of the `noaa-cefi` org (Arraylake record + their Icechunk storage), after the seven new repos replaced them.

NEVER touches NOAA's NetCDF files: it only calls `Client.delete_repo`, which reclaims the objects under the repo's own storage prefix
(`arraylake/<id>_<name>/`). Guards, all checked before anything is deleted:
 * every target's storage prefix lies under `arraylake/` (not under a NOAA data prefix) and is not the prefix of any of the new repos
   (`nep10k-hindcast` IS the old NEP daily repo's storage, renamed; its old name no longer exists and must never be deleted);
 * no virtual chunk reference of a sampled array in the new repos points under `arraylake/` (they point at the NetCDF files);
 * the new repos open, and a virtual chunk is read through each of them.
It also counts the NetCDF objects (and their bytes) under the three NOAA region prefixes before and after, and reports any change.
Without --yes it only prints the plan. Default deletion mode: the repo goes to Arraylake's recoverable state for the grace period (7 days) and
its bucket bytes are then reclaimed by the cleanup job; --immediate skips the grace period (irreversible).
Usage: delete_old_repos.py [--yes] [--immediate]
"""
import argparse
import sys

ORG = "noaa-cefi"
BUCKET = "noaa-oar-cefi-regional-mom6-pds"
OLD = ["cefi-nep-hindcast-monthly", "cefi-nwa-decadal-forecast-monthly", "cefi-nwa-decadal-forecast-yearly", "cefi-nwa-hindcast-daily",
       "cefi-nwa-hindcast-monthly", "cefi-nwa-multi-decadal-outlook-yearly", "cefi-nwa-seasonal-forecast-monthly",
       "cefi-nwa-seasonal-reforecast-monthly", "cefi-pci-hindcast-daily", "cefi-pci-hindcast-monthly"]
NEW = ["nep10k-hindcast", "nwa12-decadal-forecast", "nwa12-hindcast", "nwa12-multi-decadal-outlook", "nwa12-seasonal-forecast",
       "nwa12-seasonal-reforecast", "hic6k-hindcast"]
NOAA_PREFIXES = ["northwest_atlantic/", "northeast_pacific/", "pacific_islands/"]


def count_netcdf(store) -> dict:
    out = {}
    for p in NOAA_PREFIXES:
        n = size = 0
        for batch in store.list(prefix=p):
            for o in batch:
                n += 1
                size += o["size"]
        out[p] = (n, size)
    return out


def main() -> int:
    import obstore as obs
    import xarray as xr
    import zarr
    from arraylake import Client

    ap = argparse.ArgumentParser()
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--immediate", action="store_true")
    a = ap.parse_args()
    c = Client()
    names = {r.name for r in c.list_repos(ORG)}
    assert not (set(OLD) & set(NEW)), "a repo is on both lists"
    missing_new = [n for n in NEW if n not in names]
    assert not missing_new, f"new repos missing: {missing_new}"
    targets = [n for n in OLD if n in names]
    print(f"{len(targets)} of {len(OLD)} old repos exist: {targets}")

    new_prefixes = {n: c.get_repo_object(f"{ORG}/{n}").prefix for n in NEW}
    for n in targets:
        ro = c.get_repo_object(f"{ORG}/{n}")
        assert ro.prefix.startswith("arraylake/"), f"{n}: prefix {ro.prefix} is not under arraylake/"
        assert not any(ro.prefix.startswith(p) or p.startswith(ro.prefix) for p in new_prefixes.values()), f"{n}: prefix overlaps a new repo"
        print(f"  {n}: prefix {ro.prefix}")
    print("new repos' prefixes:", new_prefixes)

    # new repos must not reference anything under arraylake/ and must be readable
    import icechunk as ic
    from zarr.core.sync import sync
    spatial = ("lat", "lon", "jh", "ih", "yh", "xh")
    for n in NEW:
        repo = c.get_repo(f"{ORG}/{n}")
        store = repo.readonly_session(branch="main").store
        root = zarr.open_group(store, mode="r")
        checked = 0
        for path, node in root.members(max_depth=None):
            if isinstance(node, zarr.Array) and node.ndim >= 3 and checked < 40:
                async def refs(path=path):
                    found = set()
                    async for _c, kinds, ps, _o, _l, _ in store.array_chunk_iterator(path):
                        found.update(str(p) for k, p in zip(kinds, ps) if k == ic.ChunkType.virtual)
                        if len(found) > 20:
                            break
                    return found
                bad = [p for p in sync(refs()) if "/arraylake/" in p or p.startswith("arraylake/")]
                assert not bad, f"{n}/{path}: virtual references under arraylake/: {bad[:2]}"
                checked += 1
        sub = next(k for k, v in root["native"].members() if k != "static" and isinstance(v, zarr.Group))
        ds = xr.open_zarr(store, group=f"native/{sub}", consolidated=False, zarr_format=3)
        v = next(v for v in ds.data_vars if ds[v].ndim >= 3 and "time" in ds[v].dims)
        mean = float(ds[v].isel({d: 0 for d in ds[v].dims if d not in spatial}).mean())
        print(f"  {n}: opens; {checked} arrays' virtual references checked (none under arraylake/); read native/{sub}/{v} first step, mean {mean:.4g}")

    noaa = obs.store.S3Store(BUCKET, region="us-east-1", skip_signature=True)
    before = count_netcdf(noaa)
    print("NOAA data objects before:", before)
    if not a.yes:
        print("dry run: nothing deleted (use --yes)")
        return 0

    for n in targets:
        c.delete_repo(f"{ORG}/{n}", imsure=True, imreallysure=True, immediate=a.immediate)
        print(f"deleted {n}", flush=True)
    remaining = sorted(r.name for r in c.list_repos(ORG))
    print("repos now:", remaining)
    assert set(remaining) == set(NEW), f"unexpected repo list: {remaining}"
    after = count_netcdf(noaa)
    print("NOAA data objects after: ", after)
    if before != after:
        print("WARNING: the NOAA data objects changed!", file=sys.stderr)
        return 1
    print("NOAA data prefixes unchanged (object counts and bytes identical).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
