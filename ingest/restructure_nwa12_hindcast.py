#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["arraylake>=1.9", "icechunk>=2.0.5", "obstore>=0.5", "zarr>=3", "numpy", "scipy", "h5py", "xarray"]
# ///
"""NOAA revisions (REVISIONS.md): merge the NWA hindcast daily and monthly repos into one, without re-reading data.

    cefi-nwa-hindcast-daily + cefi-nwa-hindcast-monthly -> noaa-cefi/nwa12-hindcast

New layout (decision of 2026-11-24: sea ice in a nested group, no renames):
    native/daily                 <- daily raw
    native/monthly               <- monthly raw/{main,u_grid,v_grid} merged (ocean h/q/u/v)
    native/monthly/ice           <- monthly raw/{ice,ice_xB,ice_yB} merged (ice grids yT/xT, xB, yB)
    native/static                <- full ocean static set; native/static/ice <- the ice_static.nc fields
    regrid/daily                 <- daily regrid
    regrid/monthly               <- monthly regrid/main;  regrid/monthly/ice <- monthly regrid/ice (its own lon axis differs from regrid/main)
 * Virtual references stay virtual (utilities.copy_node); shared coordinates inside a merged group are kept once.
 * Ocean statics: the NWA decadal-forecast yearly r20250819 static (its land/sea mask matches the data; decision of 2026-10-26) with the
   geolon/geolat family from the hindcast monthly r20250715 static (no land gaps); land is NaN.  Ice statics: the five fields of the
   hindcast monthly ice_static.nc (fill -1e34 as NaN; GEOLON/GEOLAT are masked at the same 228,416 land cells) and its four axes.
 * areacello/deptho/wet and geolon_c/geolat_c (and the ice CELL_AREA) live only in the static groups; geolon/geolat stay in the data groups
   (the data variables name them in `coordinates`), and geolon_u/geolat_u, geolon_v/geolat_v are added to native/monthly for the u/v variables.
 * no regrid/static: NOAA ships no static file for the regridded grid.
Work happens on branch `restructure`; main is moved to it only after the checks pass. The old repos are not touched.
Usage: restructure_nwa12_hindcast.py [--build] [--validate] [--promote]
"""
from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import restructure_common as common  # noqa: E402
import restructure_nwa12_multidecadal as md  # noqa: E402
import restructure_nwa12_seasonal as base  # noqa: E402

ORG, BRANCH = base.ORG, base.BRANCH
NEW = "nwa12-hindcast"
OLD_DAILY, OLD_MONTHLY = "cefi-nwa-hindcast-daily", "cefi-nwa-hindcast-monthly"
ICE_STATIC = "northwest_atlantic/full_domain/hindcast/monthly/raw/r20250715/ice_static.nc"
OCEAN_SUBS, ICE_SUBS = ["main", "u_grid", "v_grid"], ["ice", "ice_xB", "ice_yB"]
MOVED = ("areacello", "deptho", "wet", "geolon_c", "geolat_c")
DESC = ("NOAA CEFI MOM6 NW Atlantic (nwa12) hindcast, daily and monthly, r20250715. Groups: native/{daily,monthly,static}, "
        "regrid/{daily,monthly}; sea ice in native/monthly/ice and regrid/monthly/ice. Virtual references.")
OCEAN_SOURCE = (f"{md.DECADAL_STATIC} (all fields; its mask matches the data), geolon/geolat family from the NWA hindcast monthly "
                "r20250715 ocean_static.nc")


def build_ice_static() -> dict:
    import h5py
    import obstore as obs
    f = h5py.File(io.BytesIO(bytes(obs.get(base.source_store(), ICE_STATIC).bytes())), "r")
    skip = {"_FillValue", "missing_value", "DIMENSION_LIST", "REFERENCE_LIST", "CLASS", "NAME", "_Netcdf4Dimid", "_Netcdf4Coordinates", "coordinates"}
    out = {}
    for n, d in f.items():
        attrs = {k: base._py(v) for k, v in d.attrs.items() if k not in skip}
        a = np.array(d[...])
        if d.ndim == 1:
            out[n] = ((n,), a, attrs)
        else:
            assert a.shape == (845, 775), f"{n}: unexpected shape {a.shape}"
            out[n] = (("yT", "xT"), base.masked(a.astype("float32")), attrs)
    return out


def write_ice_static(s, ice: dict) -> None:
    import zarr
    from zarr.codecs import BloscCodec
    g = zarr.open_group(s.store, path="native/static/ice", mode="a")
    for name, (dims, a, attrs) in ice.items():
        if len(dims) == 2:
            attrs = {**attrs, "_FillValue": base.NAN_FILL}
        arr = g.create_array(name, shape=a.shape, dtype=a.dtype, chunks=a.shape, dimension_names=dims, attributes=attrs,
                             compressors=BloscCodec(cname="zstd", clevel=3, shuffle="shuffle"), fill_value=float("nan"))
        arr[...] = a
    g.update_attributes({
        "title": "Static fields of the ice grid (from ice_static.nc)", "applies_to": ["native/monthly/ice"], "source": ICE_STATIC,
        "comment": "land / no-data cells (fill -1e34) are NaN. The ice grid has the same T points as the ocean h grid: yT, xT equal yh, xh"})


def build() -> None:
    import zarr
    from arraylake import Client
    from utilities import copy_node

    assert len(DESC) <= 255, len(DESC)
    new = f"{ORG}/{NEW}"
    client = Client()
    static, _ = md.build_static()
    ice = build_ice_static()
    print(f"static: {len(static)} ocean arrays, {len(ice)} ice arrays", flush=True)
    names = {r.name for r in client.list_repos(ORG)}
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
    daily = client.get_repo(f"{ORG}/{OLD_DAILY}").readonly_session(branch="main")
    monthly = client.get_repo(f"{ORG}/{OLD_MONTHLY}").readonly_session(branch="main")

    def phase(path: str, label: str, fn) -> None:
        s = repo.writable_session(BRANCH)
        if base._exists(s.store, path):
            print(f"{path} already done", flush=True)
            return
        fn(s)
        print(f"{label}: {s.commit(label)}", flush=True)

    def top(s, name):
        zarr.open_group(s.store, path=name, mode="a")

    def regrid_daily(s):
        top(s, "regrid")
        copy_node(daily, "regrid", s, "regrid/daily")

    def native_daily(s):
        top(s, "native")
        copy_node(daily, "raw", s, "native/daily")
        ng = zarr.open_group(s.store, path="native/daily", mode="r+")
        for n in MOVED:
            print("  " + common.check_moved(f"daily/{n}", ng[n][...], static[n][1]), flush=True)
            del ng[n]
        ng.update_attributes({"static_group": "native/static"})

    def regrid_monthly(s):
        top(s, "regrid")
        copy_node(monthly, "regrid/main", s, "regrid/monthly")
        copy_node(monthly, "regrid/ice", s, "regrid/monthly/ice")

    def native_monthly(s):
        top(s, "native")
        origin = common.merge_subgroups(monthly, "raw", s, "native/monthly", OCEAN_SUBS)
        print(f"  native/monthly: {len(origin)} arrays from {len(OCEAN_SUBS)} subgroups", flush=True)
        ng = zarr.open_group(s.store, path="native/monthly", mode="r+")
        for n in MOVED:
            print("  " + common.check_moved(f"monthly/{n}", ng[n][...], static[n][1]), flush=True)
        common.add_uv_coordinates(ng, static)
        for n in MOVED:
            del ng[n]
        ng.update_attributes({"static_group": "native/static"})

    def native_ice(s):
        origin = common.merge_subgroups(monthly, "raw", s, "native/monthly/ice", ICE_SUBS)
        print(f"  native/monthly/ice: {len(origin)} arrays from {len(ICE_SUBS)} subgroups", flush=True)
        ig = zarr.open_group(s.store, path="native/monthly/ice", mode="r+")
        print("  " + common.check_moved("monthly ice CELL_AREA", ig["CELL_AREA"][...], ice["CELL_AREA"][1]), flush=True)
        del ig["CELL_AREA"]
        ig.update_attributes({"static_group": "native/static/ice"})

    def statics(s):
        base.write_static(s, {"applies_to": ["native/daily", "native/monthly"], "static_source": OCEAN_SOURCE}, static)
        write_ice_static(s, ice)

    phase("regrid/daily", "Copy cefi-nwa-hindcast-daily/regrid to /regrid/daily", regrid_daily)
    phase("native/daily", "Copy cefi-nwa-hindcast-daily/raw to /native/daily; statics move to native/static", native_daily)
    phase("regrid/monthly", "Copy cefi-nwa-hindcast-monthly/regrid/{main,ice} to /regrid/monthly and /regrid/monthly/ice", regrid_monthly)
    phase("native/monthly", "Merge cefi-nwa-hindcast-monthly raw/{main,u_grid,v_grid} into native/monthly; add geolon_u/v", native_monthly)
    phase("native/monthly/ice", "Merge raw/{ice,ice_xB,ice_yB} into native/monthly/ice", native_ice)
    phase("native/static", "Add native/static and native/static/ice (full static sets, land as NaN)", statics)

    old_meta = client.get_repo_object(f"{ORG}/{OLD_MONTHLY}").metadata or {}
    keep = {k: v for k, v in old_meta.items() if k != "frequency"}  # keeps `excluded` (the six arrays NOAA must regenerate)
    client.modify_repo(new, update_metadata={**keep, "domain": "nwa12", "frequency": ["daily", "monthly"]})
    print(f"build done; work is on branch {BRANCH!r}; main is still the empty root", flush=True)


def validate() -> None:
    import xarray as xr
    import zarr
    from arraylake import Client

    client = Client()
    new = client.get_repo(f"{ORG}/{NEW}").readonly_session(branch=BRANCH)
    ng = zarr.open_group(new.store, mode="r")
    problems, n_arrays, nv, nn = [], 0, 0, 0
    sig = lambda x, drop=(): (x.shape, str(x.dtype), x.chunks, repr(sorted((k, v) for k, v in dict(x.attrs).items() if k not in drop)),  # noqa: E731
                              x.metadata.dimension_names, repr(x.metadata.fill_value), repr(x.metadata.codecs))
    join = lambda *p: "/".join(x for x in p if x)  # noqa: E731

    def compare(old, og, old_path, new_path, drop=()):
        nonlocal n_arrays, nv, nn
        n_arrays += 1
        a, b = og[old_path], ng[new_path]
        if sig(a, drop) != sig(b, drop):
            problems.append(f"{new_path}: metadata differs from {old_path}")
            return
        ta, tb = base._chunk_table(old.store, old_path), base._chunk_table(new.store, new_path)
        if ta != tb:
            problems.append(f"{new_path}: chunk table differs from {old_path}")
            return
        kinds = [r[0] for r in ta.values()]
        nv += kinds.count("virtual")
        nn += kinds.count("stored")

    def check_group(old, og, old_group, subs, new_group, moved=(), extra=()):
        """Every array of old_group/<sub> for sub in subs (flat group when subs == [""]) is in new_group once, unchanged."""
        first = {}
        for sub in subs:
            for n, _ in og[join(old_group, sub)].arrays():
                first.setdefault(n, sub)
        expected = (set(first) - set(moved)) | set(extra)
        new_names = set(dict(ng[new_group].arrays()))
        if new_names != expected:
            problems.append(f"{new_group}: names: missing {sorted(expected - new_names)}, extra {sorted(new_names - expected)}")
        for n, sub in sorted(first.items()):
            if n in moved or n not in new_names:
                continue
            old_path = join(old_group, sub, n)
            a_attrs, b_attrs = dict(og[old_path].attrs), dict(ng[join(new_group, n)].attrs)
            if any(b_attrs.get(k) != v for k, v in a_attrs.items() if k != "coordinates"):
                problems.append(f"{new_group}/{n}: an attribute of the old array is missing or changed")
            if "coordinates" in a_attrs and not str(b_attrs.get("coordinates", "")).startswith(str(a_attrs["coordinates"])):
                problems.append(f"{new_group}/{n}: coordinates attribute no longer starts with the old one")
            compare(old, og, old_path, join(new_group, n), drop=tuple(set(a_attrs) | set(b_attrs)))

    uv = ("geolon_u", "geolat_u", "geolon_v", "geolat_v")
    od = client.get_repo(f"{ORG}/{OLD_DAILY}").readonly_session(branch="main")
    om = client.get_repo(f"{ORG}/{OLD_MONTHLY}").readonly_session(branch="main")
    ogd, ogm = zarr.open_group(od.store, mode="r"), zarr.open_group(om.store, mode="r")
    check_group(od, ogd, "raw", [""], "native/daily", moved=MOVED)
    check_group(od, ogd, "regrid", [""], "regrid/daily")
    check_group(om, ogm, "raw", OCEAN_SUBS, "native/monthly", moved=MOVED, extra=uv)
    check_group(om, ogm, "raw", ICE_SUBS, "native/monthly/ice", moved=("CELL_AREA",))
    check_group(om, ogm, "regrid", ["main"], "regrid/monthly")
    check_group(om, ogm, "regrid", ["ice"], "regrid/monthly/ice")
    print(f"{n_arrays} arrays compared; {nv} virtual chunk references identical, {nn} native chunks present with identical lengths", flush=True)

    static, _ = md.build_static()
    ice = build_ice_static()
    for group, expect in (("native/static", static), ("native/static/ice", ice)):
        sg = dict(ng[group].arrays())
        if set(sg) != set(expect):
            problems.append(f"{group}: names: missing {sorted(set(expect) - set(sg))}, extra {sorted(set(sg) - set(expect))}")
        for name in sorted(set(sg) & set(expect)):
            dims, a, _ = expect[name]
            if tuple(sg[name].metadata.dimension_names) != dims or sg[name].shape != a.shape or not np.array_equal(sg[name][...], a, equal_nan=True):
                problems.append(f"{group}/{name}: differs from the source")
        print(f"{group}: {len(sg)} arrays checked against the source static file", flush=True)

    groups = ("native/daily", "native/monthly", "native/monthly/ice", "native/static", "native/static/ice", "regrid/daily", "regrid/monthly", "regrid/monthly/ice")
    for group in groups:
        ds = xr.open_zarr(new.store, group=group, consolidated=False, zarr_format=3)
        print(f"xarray {group}: {len(ds.data_vars)} data variables, {len(ds.coords)} coordinates; dims {dict(ds.sizes)}", flush=True)

    def sample(old, old_group, new_group, coord, label):
        do = xr.open_zarr(old.store, group=old_group, consolidated=False, zarr_format=3)
        dn = xr.open_zarr(new.store, group=new_group, consolidated=False, zarr_format=3)
        var = next((v for v in do.data_vars if do[v].ndim == 3 and v in dn), None)
        if var is None:
            problems.append(f"{label}: no 3-D variable to sample")
            return
        if coord and coord not in dn[var].coords:
            problems.append(f"{label}/{var}: {coord} is not attached as a coordinate")
        for step, sel in (("first step", 0), ("last step", -1)):
            if not np.array_equal(do[var].isel(time=sel).values, dn[var].isel(time=sel).values, equal_nan=True):
                problems.append(f"{label}/{var} {step}: values read through xarray differ from the old repo")
            else:
                print(f"read {label} {var}, {step} through xarray: identical in old and new repo", flush=True)

    sample(od, "raw", "native/daily", "geolon", "daily ocean")
    sample(od, "regrid", "regrid/daily", None, "daily regrid")
    for sub, new_group, coord in (("main", "native/monthly", "geolon"), ("u_grid", "native/monthly", "geolon_u"), ("v_grid", "native/monthly", "geolon_v"),
                                  ("ice", "native/monthly/ice", "geolon"), ("ice_xB", "native/monthly/ice", None), ("ice_yB", "native/monthly/ice", None)):
        sample(om, f"raw/{sub}", new_group, coord, f"monthly {sub}")
    sample(om, "regrid/main", "regrid/monthly", None, "monthly regrid")
    sample(om, "regrid/ice", "regrid/monthly/ice", None, "monthly regrid ice")
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
