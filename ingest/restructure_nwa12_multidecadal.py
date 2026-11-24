#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["arraylake>=1.9", "icechunk>=2.0.5", "obstore>=0.5", "zarr>=3", "numpy", "scipy", "h5py", "xarray"]
# ///
"""NOAA revisions (REVISIONS.md): rebuild the NWA multi-decadal outlook repo in the new layout, without re-reading data.

    cefi-nwa-multi-decadal-outlook-yearly -> noaa-cefi/nwa12-multi-decadal-outlook

New layout: native/yearly, native/static, regrid/yearly   (was: raw/main + raw/u_grid + raw/v_grid, regrid)
 * the three raw subgroups (tracer, u and v grid) become ONE group, native/yearly. Arrays are copied one by one with utilities.copy_node
   (virtual references stay virtual, native chunks such as T_adx SSP585 are copied). Coordinates present in several subgroups (time, scenario,
   z_l, xh, yh, xq, yq) are kept once; their values are identical (asserted) and their attributes are merged (the copies in raw/main lacked
   units/long_name/axis on xh, yh, xq, yq; the grid groups lacked `bounds` on time).
 * native/static holds the full static set: all 28 fields + the four axes. Coordinates (geolon/geolat family) from the NWA hindcast monthly
   r20250715 static (no land gaps); the other fields from the NWA decadal-forecast yearly r20250819 static, whose land/sea mask matches this
   release's data exactly (decision of 2026-10-26); land is NaN.
 * geolon/geolat, geolon_u/geolat_u and geolon_v/geolat_v are present in native/yearly too, and the u-grid / v-grid variables list them in
   `coordinates` (they had no 2-D coordinates before). areacello/deptho/wet and geolon_c/geolat_c live only in native/static.
 * no regrid/static: NOAA ships no static file for the regridded grid.
Work happens on branch `restructure`; main is moved to it only after the checks pass. The old repo is not touched.
Usage: restructure_nwa12_multidecadal.py [--build] [--validate] [--promote]
"""
from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import restructure_nwa12_seasonal as base  # noqa: E402

ORG, BRANCH = base.ORG, base.BRANCH
OLD, NEW = "cefi-nwa-multi-decadal-outlook-yearly", "nwa12-multi-decadal-outlook"
DECADAL_STATIC = "northwest_atlantic/full_domain/decadal_forecast/yearly/raw/r20250819/ocean_static.nc"
DESC = ("NOAA CEFI MOM6 NW Atlantic (nwa12) multi-decadal outlook, yearly, r20260331: SSP126/245/370/585 on a `scenario` axis. "
        "Groups: native/yearly, native/static, regrid/yearly. Virtual references; T_adx SSP585 native.")
SUBGROUPS = ("main", "u_grid", "v_grid")
MOVED_TO_STATIC = ("areacello", "deptho", "wet", "geolon_c", "geolat_c")
STATIC_SOURCE = (f"{DECADAL_STATIC} (all fields; its land/sea mask matches this release's data), geolon/geolat family from the NWA hindcast "
                 "monthly r20250715 ocean_static.nc")


def read_static_h5(path: str) -> dict:
    import h5py
    import obstore as obs
    f = h5py.File(io.BytesIO(bytes(obs.get(base.source_store(), path).bytes())), "r")
    skip = {"_FillValue", "missing_value", "DIMENSION_LIST", "REFERENCE_LIST", "CLASS", "NAME", "_Netcdf4Dimid", "_Netcdf4Coordinates", "coordinates"}
    out = {}
    for n, d in f.items():
        if not isinstance(d, h5py.Dataset):
            continue
        if d.ndim == 1:
            dims = (n,)  # axis arrays are named after their dimension
        else:  # no dimension scales in this file: the four NWA static grids differ in shape
            dims = {(845, 775): ("yh", "xh"), (846, 776): ("yq", "xq"), (845, 776): ("yh", "xq"), (846, 775): ("yq", "xh")}[d.shape]
        attrs = {k: base._py(v) for k, v in d.attrs.items() if k not in skip}
        out[n] = (dims, np.array(d[...]), attrs)
    return out


def build_static() -> tuple[dict, list[str]]:
    dec = read_static_h5(DECADAL_STATIC)
    hind = base.read_static(base.HINDCAST_STATIC)
    out, notes = {}, []
    for name, (dims, a, attrs) in dec.items():
        if len(dims) == 1:
            assert np.array_equal(a, hind[name][1]), f"axis {name} differs between the decadal and hindcast statics"
            out[name] = (dims, a, attrs)
        elif name in base.COORDS:
            h = hind[name][1]
            ok = np.abs(a) < 1e19
            assert np.array_equal(h[ok], a[ok]) and (np.abs(h) < 1e19).all(), f"{name}: hindcast static differs / has gaps"
            out[name] = (dims, h.astype("float32"),
                         {**attrs, "comment": "taken from the NWA hindcast monthly r20250715 ocean_static.nc (no land gaps); identical to the "
                                              "decadal-forecast r20250819 static wherever that is unmasked"})
            notes.append(name)
        else:
            out[name] = (dims, base.masked(a.astype("float32")), attrs)
    return out, notes


def build() -> None:
    import zarr
    from arraylake import Client
    from utilities import copy_node

    assert len(DESC) <= 255
    new = f"{ORG}/{NEW}"
    client = Client()
    old_repo = client.get_repo(f"{ORG}/{OLD}")
    resume = NEW in {r.name for r in client.list_repos(ORG)} or new in {r.name for r in client.list_repos(ORG)}
    static, _ = build_static()
    print(f"static: {len(static)} arrays", flush=True)
    if resume:
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

    src = old_repo.readonly_session(branch="main")
    og = zarr.open_group(src.store, mode="r")

    # regrid -> regrid/yearly
    s = repo.writable_session(BRANCH)
    if base._exists(s.store, "regrid/yearly"):
        print("regrid/yearly already copied", flush=True)
    else:
        zarr.open_group(s.store, path="regrid", mode="a")
        copy_node(src, "regrid", s, "regrid/yearly")
        print(f"copied regrid -> regrid/yearly: {s.commit(f'Copy {OLD}/regrid to /regrid/yearly')}", flush=True)

    # raw/{main,u_grid,v_grid} -> native/yearly, one array at a time, shared coordinates once
    s = repo.writable_session(BRANCH)
    if base._exists(s.store, "native/yearly"):
        print("native/yearly already copied", flush=True)
    else:
        grp = zarr.open_group(s.store, path="native/yearly", mode="a")
        grp.update_attributes(dict(og["raw/main"].attrs))
        seen: dict[str, list[str]] = {}
        for sub in SUBGROUPS:
            for name, arr in og[f"raw/{sub}"].arrays():
                seen.setdefault(name, []).append(sub)
        for name, subs in seen.items():
            if len(subs) > 1:
                arrs = [og[f"raw/{x}/{name}"] for x in subs]
                for a in arrs[1:]:
                    assert a.shape == arrs[0].shape and a.dtype == arrs[0].dtype and np.array_equal(a[...], arrs[0][...]), \
                        f"{name}: copies in {subs} differ"
                merged: dict = {}
                for a in arrs:
                    for k, v in dict(a.attrs).items():
                        assert merged.setdefault(k, v) == v, f"{name}: attribute {k!r} conflicts between {subs}"
                src_sub = subs[0]
            else:
                merged, src_sub = None, subs[0]
            copy_node(src, f"raw/{src_sub}/{name}", s, f"native/yearly/{name}")
            if merged is not None:
                zarr.open_array(s.store, path=f"native/yearly/{name}", mode="r+").update_attributes(merged)
        print(f"native/yearly: {len(seen)} arrays from {len(SUBGROUPS)} subgroups ({sum(len(v) > 1 for v in seen.values())} shared coordinates kept once)",
              flush=True)

        # statics: check the old arrays, write native/static, drop the ones that now live only there
        ng = zarr.open_group(s.store, path="native/yearly", mode="r+")
        for n in MOVED_TO_STATIC:
            o, new_a = ng[n][...], static[n][1]
            fill = np.isnan(new_a)
            assert np.array_equal(o[~fill], new_a[~fill], equal_nan=True), f"{n}: old repo differs from the source on valid cells"
            kind = "NaN" if np.isnan(o[fill]).all() else ("1e20" if (o[fill] == np.float32(1e20)).all() else "something else")
            print(f"  {n}: equals the source on {int((~fill).sum())} valid cells; the {int(fill.sum())} land cells were {kind} in the old repo, now NaN", flush=True)
        # 2-D coordinates for the u-grid / v-grid variables: add geolon_u/geolat_u and geolon_v/geolat_v to the group
        for nm in ("geolon_u", "geolat_u", "geolon_v", "geolat_v"):
            dims, a, attrs = static[nm]
            from zarr.codecs import BloscCodec
            arr = ng.create_array(nm, shape=a.shape, dtype=a.dtype, chunks=a.shape, dimension_names=dims,
                                  attributes={**{k: v for k, v in attrs.items() if k != "coordinates"}, "_FillValue": base.NAN_FILL},
                                  compressors=BloscCodec(cname="zstd", clevel=3, shuffle="shuffle"), fill_value=float("nan"))
            arr[...] = a
        for name, arr in ng.arrays():
            d = tuple(arr.metadata.dimension_names or ())
            extra = None
            if arr.ndim >= 3 and d[-2:] == ("yh", "xq"):
                extra = "geolon_u geolat_u"
            elif arr.ndim >= 3 and d[-2:] == ("yq", "xh"):
                extra = "geolon_v geolat_v"
            if extra:
                cur = str(arr.attrs.get("coordinates", "")).split()
                arr.update_attributes({"coordinates": " ".join(cur + [c for c in extra.split() if c not in cur])})
        base.write_static(s, {"applies_to": ["native/yearly"], "static_source": STATIC_SOURCE}, static)
        for n in MOVED_TO_STATIC:
            del ng[n]
        ng.update_attributes({"static_group": "native/static"})
        print(f"native/yearly + native/static: {s.commit('Merge raw/main, u_grid, v_grid into native/yearly; add native/static (full static set, land as NaN)')}",
              flush=True)

    old_meta = client.get_repo_object(f"{ORG}/{OLD}").metadata or {}
    client.modify_repo(new, update_metadata={**{k: v for k, v in old_meta.items() if k != "excluded"}, "domain": "nwa12"})
    print(f"build done; work is on branch {BRANCH!r}; main is still the empty root", flush=True)


def validate() -> None:
    import xarray as xr
    import zarr
    from arraylake import Client

    client = Client()
    old = client.get_repo(f"{ORG}/{OLD}").readonly_session(branch="main")
    new = client.get_repo(f"{ORG}/{NEW}").readonly_session(branch=BRANCH)
    og, ng = zarr.open_group(old.store, mode="r"), zarr.open_group(new.store, mode="r")
    problems, n_arrays, nv, nn = [], 0, 0, 0
    sig = lambda x, drop=(): (x.shape, str(x.dtype), x.chunks, repr(sorted((k, v) for k, v in dict(x.attrs).items() if k not in drop)),  # noqa: E731
                              x.metadata.dimension_names, repr(x.metadata.fill_value), repr(x.metadata.codecs))

    def compare(old_path, new_path, drop=()):
        nonlocal n_arrays, nv, nn
        a, b = og[old_path], ng[new_path]
        n_arrays += 1
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

    # regrid
    names_o, names_n = set(dict(og["regrid"].arrays())), set(dict(ng["regrid/yearly"].arrays()))
    if names_o != names_n:
        problems.append(f"regrid/yearly names differ: {sorted(names_o ^ names_n)}")
    for n in sorted(names_o & names_n):
        compare(f"regrid/{n}", f"regrid/yearly/{n}")
    # native: every old array in the three subgroups (minus the ones moved to static) exists once in native/yearly
    new_names = set(dict(ng["native/yearly"].arrays()))
    expected, first = set(), {}
    for sub in SUBGROUPS:
        for n, _ in og[f"raw/{sub}"].arrays():
            first.setdefault(n, sub)
            expected.add(n)
    expected = (expected - set(MOVED_TO_STATIC)) | {"geolon_u", "geolat_u", "geolon_v", "geolat_v"}
    if new_names != expected:
        problems.append(f"native/yearly names: missing {sorted(expected - new_names)}, extra {sorted(new_names - expected)}")
    for n, sub in sorted(first.items()):
        if n in MOVED_TO_STATIC or n not in new_names:
            continue
        old_arr = og[f"raw/{sub}/{n}"]
        # attributes legitimately change for merged coordinates (union) and for u/v variables (`coordinates` extended)
        a_attrs, b_attrs = dict(old_arr.attrs), dict(ng[f"native/yearly/{n}"].attrs)
        if any(b_attrs.get(k) != v for k, v in a_attrs.items() if k != "coordinates"):
            problems.append(f"native/yearly/{n}: an attribute of the old array is missing or changed")
        if "coordinates" in a_attrs and not str(b_attrs.get("coordinates", "")).startswith(str(a_attrs["coordinates"])):
            problems.append(f"native/yearly/{n}: coordinates attribute no longer starts with the old one")
        compare(f"raw/{sub}/{n}", f"native/yearly/{n}", drop=tuple(set(a_attrs) | set(b_attrs)))
    print(f"{n_arrays} arrays compared; {nv} virtual chunk references identical, {nn} native chunks present with identical lengths", flush=True)

    # statics against the sources
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

    # consumer checks
    for group in ("native/yearly", "native/static", "regrid/yearly"):
        ds = xr.open_zarr(new.store, group=group, consolidated=False, zarr_format=3)
        print(f"xarray {group}: {len(ds.data_vars)} data variables, {len(ds.coords)} coordinates; dims {dict(ds.sizes)}", flush=True)
    dn = xr.open_zarr(new.store, group="native/yearly", consolidated=False, zarr_format=3)
    for var, coord in (("tos", "geolon"), ("uo", "geolon_u"), ("vo", "geolon_v")):
        if var in dn and coord not in dn[var].coords:
            problems.append(f"{var}: {coord} is not attached as a coordinate")
    for var, sub in (("tos", "main"), ("uo", "u_grid"), ("vo", "v_grid")):
        if var not in dn:
            continue
        do = xr.open_zarr(old.store, group=f"raw/{sub}", consolidated=False, zarr_format=3)
        for label, sel in (("SSP126 year 1", dict(scenario=0, time=0)), ("SSP585 last year", dict(scenario=3, time=-1))):
            a, b = do[var].isel(**sel).isel({d: 0 for d in do[var].dims if d == "z_l"}).values, dn[var].isel(**sel).isel({d: 0 for d in dn[var].dims if d == "z_l"}).values
            if not np.array_equal(a, b, equal_nan=True):
                problems.append(f"{var} {label}: values read through xarray differ from the old repo")
            else:
                print(f"read {var} {label} through xarray: identical in old and new repo", flush=True)
    if "T_adx" in dn:
        do = xr.open_zarr(old.store, group="raw/main", consolidated=False, zarr_format=3) if "T_adx" in og["raw/main"] else None
        print("T_adx present in native/yearly (native SSP585 chunks counted above)", flush=True)
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
