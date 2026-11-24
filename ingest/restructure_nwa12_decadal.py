#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["arraylake>=1.9", "icechunk>=2.0.5", "obstore>=0.5", "zarr>=3", "numpy", "scipy", "h5py", "xarray"]
# ///
"""NOAA revisions (REVISIONS.md): merge the NWA decadal forecast monthly and yearly repos into one, without re-reading data.

    cefi-nwa-decadal-forecast-monthly + cefi-nwa-decadal-forecast-yearly -> noaa-cefi/nwa12-decadal-forecast

New layout: native/{monthly,yearly}, native/static, regrid/{monthly,yearly}   (each old repo had raw + regrid)
 * `raw` of each old repo is copied to native/<freq>, `regrid` to regrid/<freq> with utilities.copy_node (virtual references stay virtual,
   native chunks are copied).
 * native/static holds the full static set once, serving both frequencies: fields from the decadal-forecast yearly r20250819 static (its mask
   matches the data), geolon/geolat family from the NWA hindcast monthly r20250715 static (no land gaps). The monthly release's own static
   is checked to be identical on the fields that matter. Land is NaN.
 * areacello/deptho/wet and geolon_c/geolat_c are removed from the data groups (they live only in native/static); geolon/geolat stay there
   because the data variables name them in `coordinates`.
 * no regrid/static: NOAA ships no static file for the regridded grid.
Work happens on branch `restructure`; main is moved to it only after the checks pass. The old repos are not touched.
Usage: restructure_nwa12_decadal.py [--build] [--validate] [--promote]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import restructure_nwa12_multidecadal as md  # noqa: E402
import restructure_nwa12_seasonal as base  # noqa: E402

ORG, BRANCH = base.ORG, base.BRANCH
NEW = "nwa12-decadal-forecast"
SOURCES = {"monthly": "cefi-nwa-decadal-forecast-monthly", "yearly": "cefi-nwa-decadal-forecast-yearly"}
MONTHLY_STATIC = "northwest_atlantic/full_domain/decadal_forecast/monthly/raw/r20250925/ocean_static.nc"
DESC = ("NOAA CEFI MOM6 NW Atlantic (nwa12) decadal forecast, monthly (r20250925) and yearly (r20250819). Forecast layout (time = init, "
        "member, step, valid_time). Groups: native/{monthly,yearly,static}, regrid/{monthly,yearly}. Virtual references.")
MOVED_TO_STATIC = ("areacello", "deptho", "wet", "geolon_c", "geolat_c")
STATIC_SOURCE = (f"{md.DECADAL_STATIC} (all fields; its mask matches the data), geolon/geolat family from the NWA hindcast monthly "
                 "r20250715 ocean_static.nc; the monthly release's own static was checked identical on the mask fields")


def build_static() -> tuple[dict, list[str]]:
    """The yearly r20250819 static (as in the other NWA repos). The monthly release's static agrees on every ocean cell and on deptho; they
    differ only in how LAND cells are filled (areacello / wet defined on some land cells in one and masked in the other), so one set serves both."""
    static, notes = md.build_static()
    own_m = base.read_static(MONTHLY_STATIC)
    for n in ("areacello", "deptho", "wet"):
        y, m = static[n][1], base.masked(own_m[n][1].astype("float32"))
        both = ~np.isnan(y) & ~np.isnan(m)
        assert np.array_equal(y[both], m[both]), f"{n}: the monthly and yearly decadal statics differ on cells valid in both"
        only_y, only_m = int((~np.isnan(y) & np.isnan(m)).sum()), int((np.isnan(y) & ~np.isnan(m)).sum())
        notes.append(f"{n}: monthly static equals the yearly one on the {int(both.sum())} cells valid in both; valid only in yearly {only_y}, "
                     f"only in monthly {only_m} (land cells)")
    return static, notes


def build() -> None:
    import zarr
    from arraylake import Client
    from utilities import copy_node

    assert len(DESC) <= 255, len(DESC)
    new = f"{ORG}/{NEW}"
    client = Client()
    names = {r.name for r in client.list_repos(ORG)}
    static, notes = build_static()
    print(f"static: {len(static)} arrays; " + "; ".join(n for n in notes if "monthly static" in n), flush=True)
    if NEW in names or new in names:
        repo = client.get_repo(new)
        if len(list(repo.ancestry(branch="main"))) > 2:
            raise SystemExit(f"{new} exists and main has more than the root commit; refusing to touch it")
        print(f"{new} exists with an untouched main: resuming on branch {BRANCH!r}", flush=True)
    else:
        repo = client.create_repo(new, bucket_config_nickname=base.NICKNAME, description=DESC,
                                  authorize_virtual_chunk_access={base.SOURCE_URL: base.SOURCE_NICKNAME})
        print(f"created {new}", flush=True)
        s = repo.writable_session("main")
        zarr.open_group(s.store, mode="a")
        repo.create_branch(BRANCH, s.commit("Create root group"))

    for freq, old in SOURCES.items():
        src = client.get_repo(f"{ORG}/{old}").readonly_session(branch="main")
        for dst_group, src_group in ((f"native/{freq}", "raw"), (f"regrid/{freq}", "regrid")):
            s = repo.writable_session(BRANCH)
            if base._exists(s.store, dst_group):
                print(f"{dst_group} already copied", flush=True)
                continue
            zarr.open_group(s.store, path=dst_group.split("/")[0], mode="a")
            copy_node(src, src_group, s, dst_group)
            print(f"copied {old}/{src_group} -> {dst_group}: {s.commit(f'Copy {old}/{src_group} to /{dst_group}')}", flush=True)

    s = repo.writable_session(BRANCH)
    if base._exists(s.store, "native/static"):
        print("native/static already written", flush=True)
    else:
        for freq in SOURCES:
            ng = zarr.open_group(s.store, path=f"native/{freq}", mode="r+")
            for n in MOVED_TO_STATIC:
                o, new_a = ng[n][...], static[n][1]
                o_valid, s_valid = np.isfinite(o) & (np.abs(o) < 1e19), ~np.isnan(new_a)
                both = o_valid & s_valid
                assert np.array_equal(o[both], new_a[both]), f"{freq}/{n}: old repo differs from the source on cells valid in both"
                fill = "1e20" if (o[~o_valid] == np.float32(1e20)).all() else ("NaN" if np.isnan(o[~o_valid]).all() else "mixed")
                print(f"  {freq}/{n}: equal on {int(both.sum())} cells valid in both; valid only in the old repo {int((o_valid & ~s_valid).sum())}, "
                      f"only in the static {int((s_valid & ~o_valid).sum())}; old fill cells were {fill}; all become NaN", flush=True)
        base.write_static(s, {"applies_to": [f"native/{f}" for f in SOURCES], "static_source": STATIC_SOURCE}, static)
        for freq in SOURCES:
            ng = zarr.open_group(s.store, path=f"native/{freq}", mode="r+")
            for n in MOVED_TO_STATIC:
                del ng[n]
            ng.update_attributes({"static_group": "native/static"})
        print(f"statics: {s.commit('Add native/static (full static set, land as NaN) and remove areacello/deptho/wet/geolon_c/geolat_c from the data groups')}",
              flush=True)

    old_meta = client.get_repo_object(f"{ORG}/{SOURCES['yearly']}").metadata or {}
    keep = {k: v for k, v in old_meta.items() if k not in ("excluded", "frequency", "release")}
    client.modify_repo(new, update_metadata={**keep, "domain": "nwa12", "frequency": ["monthly", "yearly"],
                                             "release": ["r20250925 (monthly)", "r20250819 (yearly)"]})
    print(f"build done; work is on branch {BRANCH!r}; main is still the empty root", flush=True)


def validate() -> None:
    import xarray as xr
    import zarr
    from arraylake import Client

    client = Client()
    new = client.get_repo(f"{ORG}/{NEW}").readonly_session(branch=BRANCH)
    ng = zarr.open_group(new.store, mode="r")
    problems, n_arrays, nv, nn = [], 0, 0, 0
    sig = lambda x: (x.shape, str(x.dtype), x.chunks, repr(sorted(dict(x.attrs).items())), x.metadata.dimension_names,  # noqa: E731
                     repr(x.metadata.fill_value), repr(x.metadata.codecs))
    for freq, old_name in SOURCES.items():
        old = client.get_repo(f"{ORG}/{old_name}").readonly_session(branch="main")
        og = zarr.open_group(old.store, mode="r")
        for old_group, new_group in (("raw", f"native/{freq}"), ("regrid", f"regrid/{freq}")):
            oarr, narr = dict(og[old_group].arrays()), dict(ng[new_group].arrays())
            expected = {k for k in oarr if not (old_group == "raw" and k in MOVED_TO_STATIC)}
            if set(narr) != expected:
                problems.append(f"{new_group}: names differ: missing {sorted(expected - set(narr))}, extra {sorted(set(narr) - expected)}")
            if dict(og[old_group].attrs) != {k: v for k, v in dict(ng[new_group].attrs).items() if k != "static_group"}:
                problems.append(f"{new_group}: group attributes differ")
            for name in sorted(expected & set(narr)):
                n_arrays += 1
                if sig(oarr[name]) != sig(narr[name]):
                    problems.append(f"{new_group}/{name}: metadata differs")
                    continue
                ta = base._chunk_table(old.store, f"{old_group}/{name}")
                tb = base._chunk_table(new.store, f"{new_group}/{name}")
                if ta != tb:
                    problems.append(f"{new_group}/{name}: chunk table differs")
                    continue
                kinds = [r[0] for r in ta.values()]
                nv += kinds.count("virtual")
                nn += kinds.count("stored")
    print(f"{n_arrays} arrays compared; {nv} virtual chunk references identical, {nn} native chunks present with identical lengths", flush=True)

    static, _ = build_static()
    sg = dict(ng["native/static"].arrays())
    if set(sg) != set(static):
        problems.append(f"native/static names: missing {sorted(set(static) - set(sg))}, extra {sorted(set(sg) - set(static))}")
    for name in sorted(set(sg) & set(static)):
        dims, a, _ = static[name]
        arr = sg[name]
        if tuple(arr.metadata.dimension_names) != dims or arr.shape != a.shape or not np.array_equal(arr[...], a, equal_nan=True):
            problems.append(f"native/static/{name}: differs from the source")
    print(f"native/static: {len(sg)} arrays checked against the source static files", flush=True)

    for group in ("native/monthly", "native/yearly", "native/static", "regrid/monthly", "regrid/yearly"):
        ds = xr.open_zarr(new.store, group=group, consolidated=False, zarr_format=3)
        print(f"xarray {group}: {len(ds.data_vars)} data variables, {len(ds.coords)} coordinates; dims {dict(ds.sizes)}", flush=True)
    for freq, old_name in SOURCES.items():
        old = client.get_repo(f"{ORG}/{old_name}").readonly_session(branch="main")
        do = xr.open_zarr(old.store, group="raw", consolidated=False, zarr_format=3)
        dn = xr.open_zarr(new.store, group=f"native/{freq}", consolidated=False, zarr_format=3)
        var = next(v for v in do.data_vars if do[v].ndim == 5)
        if "geolon" not in dn[var].coords:
            problems.append(f"{freq}/{var}: geolon is not attached as a coordinate")
        for label, sel in (("first init", slice(0, 1)), ("last init", slice(-1, None))):
            a = do[var].isel(time=sel, member=0, step=0).values
            b = dn[var].isel(time=sel, member=0, step=0).values
            if not np.array_equal(a, b, equal_nan=True):
                problems.append(f"{freq}/{var} {label}: values read through xarray differ from the old repo")
            else:
                print(f"read {freq} {var}, {label} through xarray: identical in old and new repo", flush=True)
    print("PROBLEMS:" if problems else "OK: no problems found", flush=True)
    for p in problems:
        print("  -", p)
    if problems:
        raise SystemExit(1)


def promote() -> None:
    from arraylake import Client
    repo = Client().get_repo(f"{ORG}/{NEW}")
    main_tip = repo.lookup_branch("main")
    repo.reset_branch("main", repo.lookup_branch(BRANCH), from_snapshot_id=main_tip)
    repo.delete_branch(BRANCH)
    print(f"main of {ORG}/{NEW} now at {repo.lookup_branch('main')}; branch {BRANCH!r} deleted", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--promote", action="store_true")
    a = ap.parse_args()
    if a.build:
        build()
    if a.validate:
        validate()
    if a.promote:
        promote()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
