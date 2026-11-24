#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["arraylake>=1.9", "icechunk>=2.0.5", "obstore>=0.5", "zarr>=3", "numpy", "scipy", "h5py", "xarray"]
# ///
"""NOAA revisions (REVISIONS.md): merge the Pacific Islands hindcast daily and monthly repos into one, without re-reading data.

    cefi-pci-hindcast-daily + cefi-pci-hindcast-monthly -> noaa-cefi/hic6k-hindcast

New layout: native/{daily,monthly}, native/static, regrid/{daily,monthly}   (each old repo had raw/{main,u_grid,v_grid} + regrid)
 * raw/main, raw/u_grid and raw/v_grid of each old repo become ONE group native/<freq> (restructure_common.merge_subgroups: shared coordinates
   kept once, values asserted identical, attributes merged); regrid is copied to regrid/<freq>. Virtual references stay virtual.
 * native/static holds the full static set once for both frequencies: all fields of the product's own ocean_static.nc (the daily and monthly
   files are asserted identical), land / no-data (1e20) as NaN. The `time` axis of the static file (length 1) is not a static field and is
   left out. The int8 masks (mask, mask_u, mask_v) are kept as they are.
 * geolon/geolat stay in the data groups, and geolon_u/geolat_u and geolon_v/geolat_v are added to them and listed in `coordinates` of the
   u- and v-grid variables; areacello/deptho/wet and geolon_c/geolat_c live only in native/static.
 * no regrid/static: NOAA ships no static file for the regridded grid.
Work happens on branch `restructure`; main is moved to it only after the checks pass. The old repos are not touched.
Usage: restructure_hic6k.py [--build] [--validate] [--promote]
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
import restructure_nwa12_seasonal as base  # noqa: E402

ORG, BRANCH = base.ORG, base.BRANCH
NEW = "hic6k-hindcast"
SOURCES = {"daily": "cefi-pci-hindcast-daily", "monthly": "cefi-pci-hindcast-monthly"}
STATIC_PATHS = {f: f"pacific_islands/full_domain/hindcast/{f}/raw/r20260427/ocean_static.nc" for f in SOURCES}
SUBGROUPS = ["main", "u_grid", "v_grid"]
MOVED_TO_STATIC = ("areacello", "deptho", "wet", "geolon_c", "geolat_c")
DESC = ("NOAA CEFI MOM6 Pacific Islands (hic6k) hindcast, daily and monthly, r20260427 (draft product). Groups: native/{daily,monthly,static}, "
        "regrid/{daily,monthly}. Virtual references to the NOAA NetCDF files.")
STATIC_SOURCE = f"{STATIC_PATHS['monthly']} (all fields; identical to the daily release's static)"
Q, U, V = ({"geolon_c", "geolat_c", "wet_c", "areacello_bu", "Coriolis"},
           {"geolon_u", "geolat_u", "wet_u", "dxCu", "dyCu", "areacello_cu", "mask_u"},
           {"geolon_v", "geolat_v", "wet_v", "dxCv", "dyCv", "areacello_cv", "mask_v"})
AXES = {"yh": 539, "xh": 725, "yq": 539, "xq": 726}  # lengths in the PCI static (yq has no extra row)


def dims_of(name: str) -> tuple[str, str]:
    return ("yq", "xq") if name in Q else ("yh", "xq") if name in U else ("yq", "xh") if name in V else ("yh", "xh")


def read_static(path: str) -> dict:
    import h5py
    import obstore as obs
    f = h5py.File(io.BytesIO(bytes(obs.get(base.source_store(), path).bytes())), "r")
    skip = {"_FillValue", "missing_value", "DIMENSION_LIST", "REFERENCE_LIST", "CLASS", "NAME", "_Netcdf4Dimid", "_Netcdf4Coordinates", "coordinates"}
    out = {}
    for n, d in f.items():
        if n == "time" or not hasattr(d, "shape"):
            continue
        attrs = {k: base._py(v) for k, v in d.attrs.items() if k not in skip}
        a = np.array(d[...])
        if d.ndim == 1:
            out[n] = ((n,), a, attrs)
        else:
            dims = dims_of(n)
            assert a.shape == (AXES[dims[0]], AXES[dims[1]]), f"{n}: shape {a.shape} does not fit dims {dims}"
            out[n] = (dims, base.masked(a.astype("float32")) if a.dtype.kind == "f" else a, attrs)
    return out


def build_static() -> tuple[dict, list[str]]:
    d, m = read_static(STATIC_PATHS["daily"]), read_static(STATIC_PATHS["monthly"])
    assert set(d) == set(m) and all(np.array_equal(d[n][1], m[n][1], equal_nan=d[n][1].dtype.kind == "f") for n in d), \
        "the daily and monthly static files differ"
    for n in base.COORDS:
        assert np.isfinite(m[n][1]).all(), f"{n} has gaps"
    return m, [f"{len(m)} static arrays, daily == monthly"]


def build() -> None:
    import zarr
    from arraylake import Client
    from utilities import copy_node

    assert len(DESC) <= 255, len(DESC)
    new = f"{ORG}/{NEW}"
    client = Client()
    static, notes = build_static()
    print("static:", notes[0], flush=True)
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

    for freq, old in SOURCES.items():
        src = client.get_repo(f"{ORG}/{old}").readonly_session(branch="main")
        s = repo.writable_session(BRANCH)
        if not base._exists(s.store, f"regrid/{freq}"):
            zarr.open_group(s.store, path="regrid", mode="a")
            copy_node(src, "regrid", s, f"regrid/{freq}")
            print(f"copied {old}/regrid -> regrid/{freq}: {s.commit(f'Copy {old}/regrid to /regrid/{freq}')}", flush=True)
        s = repo.writable_session(BRANCH)
        if not base._exists(s.store, f"native/{freq}"):
            zarr.open_group(s.store, path="native", mode="a")
            origin = common.merge_subgroups(src, "raw", s, f"native/{freq}", SUBGROUPS)
            print(f"native/{freq}: {len(origin)} arrays from {len(SUBGROUPS)} subgroups of {old}/raw", flush=True)
            ng = zarr.open_group(s.store, path=f"native/{freq}", mode="r+")
            for n in MOVED_TO_STATIC:
                print("  " + common.check_moved(f"{freq}/{n}", ng[n][...], static[n][1]), flush=True)
            common.add_uv_coordinates(ng, static)
            for n in MOVED_TO_STATIC:
                del ng[n]
            ng.update_attributes({"static_group": "native/static"})
            print(f"native/{freq} done: {s.commit(f'Merge {old}/raw/{{main,u_grid,v_grid}} into native/{freq}; add geolon_u/v coordinates; drop the arrays that move to native/static')}",
                  flush=True)

    s = repo.writable_session(BRANCH)
    if not base._exists(s.store, "native/static"):
        base.write_static(s, {"applies_to": [f"native/{f}" for f in SOURCES], "static_source": STATIC_SOURCE}, static)
        print(f"native/static: {s.commit('Add native/static (full ocean_static, land as NaN)')}", flush=True)

    old_meta = client.get_repo_object(f"{ORG}/{SOURCES['monthly']}").metadata or {}
    keep = {k: v for k, v in old_meta.items() if k not in ("excluded", "frequency")}
    client.modify_repo(new, update_metadata={**keep, "domain": "hic6k", "frequency": ["daily", "monthly"]})
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

    for freq, old_name in SOURCES.items():
        old = client.get_repo(f"{ORG}/{old_name}").readonly_session(branch="main")
        og = zarr.open_group(old.store, mode="r")
        names_o, names_n = set(dict(og["regrid"].arrays())), set(dict(ng[f"regrid/{freq}"].arrays()))
        if names_o != names_n:
            problems.append(f"regrid/{freq}: names differ: {sorted(names_o ^ names_n)}")
        for n in sorted(names_o & names_n):
            compare(old, og, f"regrid/{n}", f"regrid/{freq}/{n}")
        first, expected = {}, set()
        for sub in SUBGROUPS:
            for n, _ in og[f"raw/{sub}"].arrays():
                first.setdefault(n, sub)
                expected.add(n)
        expected = (expected - set(MOVED_TO_STATIC)) | {"geolon_u", "geolat_u", "geolon_v", "geolat_v"}
        new_names = set(dict(ng[f"native/{freq}"].arrays()))
        if new_names != expected:
            problems.append(f"native/{freq}: names: missing {sorted(expected - new_names)}, extra {sorted(new_names - expected)}")
        for n, sub in sorted(first.items()):
            if n in MOVED_TO_STATIC or n not in new_names:
                continue
            a_attrs, b_attrs = dict(og[f"raw/{sub}/{n}"].attrs), dict(ng[f"native/{freq}/{n}"].attrs)
            if any(b_attrs.get(k) != v for k, v in a_attrs.items() if k != "coordinates"):
                problems.append(f"native/{freq}/{n}: an attribute of the old array is missing or changed")
            if "coordinates" in a_attrs and not str(b_attrs.get("coordinates", "")).startswith(str(a_attrs["coordinates"])):
                problems.append(f"native/{freq}/{n}: coordinates attribute no longer starts with the old one")
            compare(old, og, f"raw/{sub}/{n}", f"native/{freq}/{n}", drop=tuple(set(a_attrs) | set(b_attrs)))
    print(f"{n_arrays} arrays compared; {nv} virtual chunk references identical, {nn} native chunks present with identical lengths", flush=True)

    static, _ = build_static()
    sg = dict(ng["native/static"].arrays())
    if set(sg) != set(static):
        problems.append(f"native/static names: missing {sorted(set(static) - set(sg))}, extra {sorted(set(sg) - set(static))}")
    for name in sorted(set(sg) & set(static)):
        dims, a, _ = static[name]
        if tuple(sg[name].metadata.dimension_names) != dims or sg[name].shape != a.shape or not np.array_equal(sg[name][...], a, equal_nan=a.dtype.kind == "f"):
            problems.append(f"native/static/{name}: differs from the source")
    print(f"native/static: {len(sg)} arrays checked against the source static file", flush=True)

    for group in [f"native/{f}" for f in SOURCES] + ["native/static"] + [f"regrid/{f}" for f in SOURCES]:
        ds = xr.open_zarr(new.store, group=group, consolidated=False, zarr_format=3)
        print(f"xarray {group}: {len(ds.data_vars)} data variables, {len(ds.coords)} coordinates; dims {dict(ds.sizes)}", flush=True)
    for freq, old_name in SOURCES.items():
        old = client.get_repo(f"{ORG}/{old_name}").readonly_session(branch="main")
        dn = xr.open_zarr(new.store, group=f"native/{freq}", consolidated=False, zarr_format=3)
        for sub, coord in (("main", "geolon"), ("u_grid", "geolon_u"), ("v_grid", "geolon_v")):
            do = xr.open_zarr(old.store, group=f"raw/{sub}", consolidated=False, zarr_format=3)
            var = next((v for v in do.data_vars if do[v].ndim == 3 and v in dn), None)
            if var is None:
                continue
            if coord not in dn[var].coords:
                problems.append(f"{freq}/{var}: {coord} is not attached as a coordinate")
            for label, sel in (("first step", 0), ("last step", -1)):
                a, b = do[var].isel(time=sel).values, dn[var].isel(time=sel).values
                if not np.array_equal(a, b, equal_nan=True):
                    problems.append(f"{freq}/{var} {label}: values read through xarray differ from the old repo")
                else:
                    print(f"read {freq} {var} ({sub}), {label} through xarray: identical in old and new repo", flush=True)
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
