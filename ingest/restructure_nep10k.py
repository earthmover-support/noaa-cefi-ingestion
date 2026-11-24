#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["arraylake>=1.9", "icechunk>=2.0.5", "obstore>=0.5", "zarr>=3", "numpy", "scipy", "xarray"]
# ///
"""NOAA revisions (REVISIONS.md): the NEP hindcast daily and monthly repos as one repo, `noaa-cefi/nep10k-hindcast`.

The daily repo (cefi-nep-hindcast-daily, with 4.2 TB of native 3-D chunks) was first renamed WITHOUT copying data (rename_repo_by_import.py: the
same Icechunk storage is now the Arraylake repo `nep10k-hindcast`; `main` is still the old layout, tag `before-rename`). Then, on branch `restructure`:

    /raw     -> /native/daily       (Icechunk move: metadata only, no chunk is touched)
    /regrid  -> /regrid/daily       (via a temporary name: a group cannot move into its own subtree)
    cefi-nep-hindcast-monthly raw/{main,u_grid,v_grid,b_x,b_y} merged -> /native/monthly;  regrid -> /regrid/monthly  (copy_node: virtual references)
    /native/static                  the full ocean_static.nc (34 fields/axes) and the ice_static.nc fields CELL_AREA, COSROT, SINROT

 * Statics: the daily and monthly ocean_static.nc / ice_static.nc are asserted identical. Land is NaN. NEP's own static has its geolon/geolat family
   masked on land (57,677 cells for geolon); the repo's geolon, geolat, geolon_c, geolat_c are gap-free (from the data files) and equal the static
   wherever that is valid, so those four come from the repo; geolon_u/geolat_u and geolon_v/geolat_v have no gap-free source and keep NaN on land.
   The ice fields use the data's dims (jh, ih) instead of the file's (yT, xT); the ice file's geolon/geolat duplicate the ocean ones (asserted)
   and its extra axes (xT, yT, xTe, yTe, xB, yB, ct, nv) are not copied.
 * areacello/deptho/wet and geolon_c/geolat_c leave the data groups (only in native/static); geolon/geolat stay (the data variables name them in
   `coordinates`); geolon_u/geolat_u and geolon_v/geolat_v are added to the data groups for the u- and v-grid variables.
 * no regrid/static: NOAA ships no static file for the regridded grid.
Work happens on branch `restructure`; main is moved to it only by --promote after --validate. Usage: restructure_nep10k.py [--build] [--validate] [--promote]
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
NEW, OLD_MONTHLY = "nep10k-hindcast", "cefi-nep-hindcast-monthly"
PATH = "northeast_pacific/full_domain/hindcast/{f}/raw/r20260701/{n}"
SUBS = ["main", "u_grid", "v_grid", "b_x", "b_y"]
MOVED = ("areacello", "deptho", "wet", "geolon_c", "geolat_c")
GRID_COORDS = {("jh", "ih"): "geolon geolat", ("jq", "iq"): "geolon_c geolat_c", ("jh", "iq"): "geolon_u geolat_u", ("jq", "ih"): "geolon_v geolat_v"}
CFG = {"grid_coords": GRID_COORDS,
       "applies_to": ["native/daily", "native/monthly"],
       "static_source": (PATH.format(f="monthly", n="ocean_static.nc") + " (all fields; identical to the daily release's) and " + PATH.format(f="monthly", n="ice_static.nc")
                         + " (CELL_AREA, COSROT, SINROT); geolon/geolat/geolon_c/geolat_c from the repo's gap-free arrays (the static has them masked on land)")}
DESC = ("NOAA CEFI MOM6 NE Pacific (nep10k) hindcast, daily and monthly, r20260701. Groups: native/{daily,monthly,static}, regrid/{daily,monthly}. "
        "Virtual references; the daily per-year 3-D variables are native copies.")
Q, U, V = ({"geolon_c", "geolat_c", "wet_c", "areacello_bu", "Coriolis"},
           {"geolon_u", "geolat_u", "wet_u", "dxCu", "dyCu", "areacello_cu"},
           {"geolon_v", "geolat_v", "wet_v", "dxCv", "dyCv", "areacello_cv"})


def dims_of(name: str) -> tuple[str, str]:
    return ("jq", "iq") if name in Q else ("jh", "iq") if name in U else ("jq", "ih") if name in V else ("jh", "ih")


def _static_file(path: str):
    import obstore as obs
    from scipy.io import netcdf_file
    return netcdf_file(io.BytesIO(bytes(obs.get(base.source_store(), path).bytes())), mmap=False)


def build_static(repo_raw=None) -> tuple[dict, list[str]]:
    """`repo_raw`: the zarr group holding the repo's gap-free geolon, geolat, geolon_c, geolat_c (old raw group)."""
    notes, files = [], {f: (_static_file(PATH.format(f=f, n="ocean_static.nc")), _static_file(PATH.format(f=f, n="ice_static.nc"))) for f in ("daily", "monthly")}
    for kind, i in (("ocean", 0), ("ice", 1)):
        d, m = files["daily"][i], files["monthly"][i]
        assert set(d.variables) == set(m.variables) and all(np.array_equal(d.variables[n].data, m.variables[n].data, equal_nan=True) for n in d.variables), \
            f"the daily and monthly {kind} static files differ"
    ocean, ice = files["monthly"]
    out = {}
    for n, v in ocean.variables.items():
        a = np.array(v.data)
        a = a.astype(a.dtype.newbyteorder("="))
        attrs = {k: base._py(getattr(v, k)) for k in v._attributes if k not in ("_FillValue", "missing_value")}
        out[n] = ((n,), a, attrs) if a.ndim == 1 else (dims_of(n), base.masked(a.astype("float32")), attrs)
    for n in ("geolon", "geolat", "geolon_c", "geolat_c"):
        if repo_raw is not None:
            g = repo_raw[n][...]
            assert np.isfinite(g).all(), f"{n}: the repo's array has gaps"
            ok = ~np.isnan(out[n][1])
            assert np.array_equal(g[ok], out[n][1][ok]), f"{n}: repo array differs from the static where the static is valid"
            out[n] = (out[n][0], g, {**out[n][2], "comment": "gap-free values (from the data files); equal to ocean_static.nc wherever that is valid (it is masked on land)"})
    for n in ("CELL_AREA", "COSROT", "SINROT"):
        v = ice.variables[n]
        a = base.masked(np.array(v.data).astype("float32"))
        attrs = {k: base._py(getattr(v, k)) for k in v._attributes if k not in ("_FillValue", "missing_value")}
        out[n] = (("jh", "ih"), a, {**attrs, "comment": "from ice_static.nc, on the data's (jh, ih) dims (the file calls them yT, xT); fill -1e34 stored as NaN"})
    for n in ("geolon", "geolat"):
        assert np.array_equal(base.masked(np.array(ice.variables[n].data).astype("float32")), base.masked(np.array(ocean.variables[n].data).astype("float32")),
                              equal_nan=True), f"ice_static {n} differs from the ocean {n}"
    notes.append(f"{len(out)} static arrays (ocean {len(ocean.variables)}, ice 3); daily == monthly; ice geolon/geolat duplicate the ocean ones")
    return out, notes


def build() -> None:
    import zarr
    from arraylake import Client
    from utilities import copy_node, move_node

    assert len(DESC) <= 255, len(DESC)
    new = f"{ORG}/{NEW}"
    client = Client()
    repo = client.get_repo(new)
    if BRANCH not in repo.list_branches():
        if "before-rename" not in repo.list_tags():
            raise SystemExit("tag before-rename missing: the rename (rename_repo_by_import.py) should have been done first")
        repo.create_branch(BRANCH, repo.lookup_branch("main"))
        print(f"created branch {BRANCH!r} at main {repo.lookup_branch('main')}", flush=True)
    store = repo.readonly_session(branch=BRANCH).store
    top = {k for k, _ in zarr.open_group(store, mode="r").members()}
    old_raw = zarr.open_group(repo.readonly_session(branch="main").store, mode="r")["raw"]
    static, notes = build_static(old_raw)
    print("static:", notes[0], flush=True)

    # 1. daily: moves (metadata only)
    if "raw" in top:
        move_node(repo, "/raw", "/native/daily", branch=BRANCH)
        print("moved /raw -> /native/daily", flush=True)
    if "regrid" in top and "native" in top | {"native"}:
        s = repo.readonly_session(branch=BRANCH)
        if not base._exists(s.store, "regrid/daily"):
            move_node(repo, "/regrid", "/_regrid_tmp", branch=BRANCH)
            move_node(repo, "/_regrid_tmp", "/regrid/daily", branch=BRANCH)
            print("moved /regrid -> /regrid/daily", flush=True)

    # 2. monthly: copy the references from the old monthly repo
    monthly = client.get_repo(f"{ORG}/{OLD_MONTHLY}").readonly_session(branch="main")
    s = repo.writable_session(BRANCH)
    if not base._exists(s.store, "regrid/monthly"):
        copy_node(monthly, "regrid", s, "regrid/monthly")
        print(f"copied {OLD_MONTHLY}/regrid -> regrid/monthly: {s.commit('Copy cefi-nep-hindcast-monthly/regrid to /regrid/monthly')}", flush=True)
    s = repo.writable_session(BRANCH)
    if not base._exists(s.store, "native/monthly"):
        origin = common.merge_subgroups(monthly, "raw", s, "native/monthly", SUBS)
        print(f"native/monthly: {len(origin)} arrays from {len(SUBS)} subgroups of {OLD_MONTHLY}/raw", flush=True)
        print(f"native/monthly merged: {s.commit('Merge cefi-nep-hindcast-monthly raw/{main,u_grid,v_grid,b_x,b_y} into native/monthly')}", flush=True)

    # 3. statics and u/v coordinates, both frequencies in one commit
    s = repo.writable_session(BRANCH)
    if not base._exists(s.store, "native/static"):
        for freq in ("daily", "monthly"):
            ng = zarr.open_group(s.store, path=f"native/{freq}", mode="r+")
            for n in MOVED:
                print("  " + common.check_moved(f"{freq}/{n}", ng[n][...], static[n][1]), flush=True)
            common.add_uv_coordinates(ng, static, u_dims=("jh", "iq"), v_dims=("jq", "ih"))
            for n in MOVED:
                del ng[n]
            ng.update_attributes({"static_group": "native/static"})
        base.write_static(s, CFG, static)
        print(f"native/static + u/v coordinates: {s.commit('Add native/static (full ocean_static + ice CELL_AREA/COSROT/SINROT, land as NaN); add geolon_u/v coordinates; drop the arrays that move to native/static')}", flush=True)

    client.modify_repo(new, description=DESC, update_metadata={"domain": "nep10k", "frequency": ["daily", "monthly"]})
    print(f"build done; work is on branch {BRANCH!r}; main is still the old layout", flush=True)


def validate() -> None:
    import xarray as xr
    import zarr
    from arraylake import Client

    client = Client()
    repo = client.get_repo(f"{ORG}/{NEW}")
    old = repo.readonly_session(branch="main")  # still the old daily layout until --promote
    new = repo.readonly_session(branch=BRANCH)
    om = client.get_repo(f"{ORG}/{OLD_MONTHLY}").readonly_session(branch="main")
    og, ogm, ng = (zarr.open_group(x.store, mode="r") for x in (old, om, new))
    problems, n_arrays, nv, nn = [], 0, 0, 0
    sig = lambda x, drop=(): (x.shape, str(x.dtype), x.chunks, repr(sorted((k, v) for k, v in dict(x.attrs).items() if k not in drop)),  # noqa: E731
                              x.metadata.dimension_names, repr(x.metadata.fill_value), repr(x.metadata.codecs))
    join = lambda *p: "/".join(x for x in p if x)  # noqa: E731

    def compare(osess, ogrp, old_path, new_path, drop=()):
        nonlocal n_arrays, nv, nn
        n_arrays += 1
        a, b = ogrp[old_path], ng[new_path]
        if sig(a, drop) != sig(b, drop):
            problems.append(f"{new_path}: metadata differs from {old_path}")
            return
        ta, tb = base._chunk_table(osess.store, old_path), base._chunk_table(new.store, new_path)
        if ta != tb:
            problems.append(f"{new_path}: chunk table differs from {old_path}")
            return
        kinds = [r[0] for r in ta.values()]
        nv += kinds.count("virtual")
        nn += kinds.count("stored")

    def check_group(osess, ogrp, old_group, subs, new_group, moved=(), extra=()):
        first = {}
        for sub in subs:
            for n, _ in ogrp[join(old_group, sub)].arrays():
                first.setdefault(n, sub)
        expected = (set(first) - set(moved)) | set(extra)
        new_names = set(dict(ng[new_group].arrays()))
        if new_names != expected:
            problems.append(f"{new_group}: names: missing {sorted(expected - new_names)}, extra {sorted(new_names - expected)}")
        for n, sub in sorted(first.items()):
            if n in moved or n not in new_names:
                continue
            old_path = join(old_group, sub, n)
            a_attrs, b_attrs = dict(ogrp[old_path].attrs), dict(ng[join(new_group, n)].attrs)
            if any(b_attrs.get(k) != v for k, v in a_attrs.items() if k != "coordinates"):
                problems.append(f"{new_group}/{n}: an attribute of the old array is missing or changed")
            if "coordinates" in a_attrs and not str(b_attrs.get("coordinates", "")).startswith(str(a_attrs["coordinates"])):
                problems.append(f"{new_group}/{n}: coordinates attribute no longer starts with the old one")
            compare(osess, ogrp, old_path, join(new_group, n), drop=tuple(set(a_attrs) | set(b_attrs)))

    uv = ("geolon_u", "geolat_u", "geolon_v", "geolat_v")
    check_group(old, og, "raw", [""], "native/daily", moved=MOVED, extra=uv)
    check_group(old, og, "regrid", [""], "regrid/daily")
    check_group(om, ogm, "raw", SUBS, "native/monthly", moved=MOVED, extra=uv)
    check_group(om, ogm, "regrid", [""], "regrid/monthly")
    print(f"{n_arrays} arrays compared; {nv} virtual chunk references identical, {nn} native chunks present with identical lengths", flush=True)

    static, _ = build_static(og["raw"])
    sg = dict(ng["native/static"].arrays())
    if set(sg) != set(static):
        problems.append(f"native/static names: missing {sorted(set(static) - set(sg))}, extra {sorted(set(sg) - set(static))}")
    for name in sorted(set(sg) & set(static)):
        dims, a, _ = static[name]
        if tuple(sg[name].metadata.dimension_names) != dims or sg[name].shape != a.shape or not np.array_equal(sg[name][...], a, equal_nan=a.dtype.kind == "f"):
            problems.append(f"native/static/{name}: differs from the source")
    print(f"native/static: {len(sg)} arrays checked against the source static files", flush=True)

    for group in ("native/daily", "native/monthly", "native/static", "regrid/daily", "regrid/monthly"):
        ds = xr.open_zarr(new.store, group=group, consolidated=False, zarr_format=3)
        print(f"xarray {group}: {len(ds.data_vars)} data variables, {len(ds.coords)} coordinates; dims {dict(ds.sizes)}", flush=True)

    def sample(osess, old_group, new_group, coord, label, ndim=3):
        do = xr.open_zarr(osess.store, group=old_group, consolidated=False, zarr_format=3)
        dn = xr.open_zarr(new.store, group=new_group, consolidated=False, zarr_format=3)
        var = next((v for v in do.data_vars if do[v].ndim == ndim and v in dn), None)
        if var is None:
            problems.append(f"{label}: no {ndim}-D variable to sample")
            return
        if coord and coord not in dn[var].coords:
            problems.append(f"{label}/{var}: {coord} is not attached as a coordinate")
        for step, t in (("first step", 0), ("last step", -1)):
            sel = {"time": t, **({"z_l": 0} if ndim == 4 else {})}
            a, b = (x[var].isel(sel).isel({d: slice(0, 200) for d in x[var].dims if d in ("jh", "ih", "jq", "iq", "lat", "lon")}).values for x in (do, dn))
            if not np.array_equal(a, b, equal_nan=True):
                problems.append(f"{label}/{var} {step}: values read through xarray differ from the old repo")
            else:
                print(f"read {label} {var}, {step} through xarray: identical in old and new repo", flush=True)

    sample(old, "raw", "native/daily", "geolon", "daily 2-D (virtual)")
    sample(old, "raw", "native/daily", "geolon", "daily 3-D (native)", ndim=4)
    sample(old, "regrid", "regrid/daily", None, "daily regrid")
    for sub, coord in (("main", "geolon"), ("u_grid", "geolon_u"), ("v_grid", "geolon_v"), ("b_x", None), ("b_y", None)):
        sample(om, f"raw/{sub}", "native/monthly", coord, f"monthly {sub}")
    sample(om, "regrid", "regrid/monthly", None, "monthly regrid")
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
    print(f"main of {ORG}/{NEW} now at {repo.lookup_branch('main')} (was {main_tip}, still tagged before-rename); branch {BRANCH!r} deleted", flush=True)


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
