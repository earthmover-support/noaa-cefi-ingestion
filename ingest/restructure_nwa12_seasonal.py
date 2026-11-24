#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["arraylake>=1.9", "icechunk>=2.0.5", "obstore>=0.5", "zarr>=3", "numpy", "scipy", "xarray"]
# ///
"""NOAA revisions (REVISIONS.md): rebuild the NWA seasonal forecast / reforecast repos in the new layout, without re-reading data.

    cefi-nwa-seasonal-forecast-monthly   -> noaa-cefi/nwa12-seasonal-forecast
    cefi-nwa-seasonal-reforecast-monthly -> noaa-cefi/nwa12-seasonal-reforecast

New layout:  native/monthly, native/static, regrid/monthly   (was: raw, regrid)
 * `raw` is copied to `native/monthly`, `regrid` to `regrid/monthly` with utilities.copy_node: virtual chunks stay virtual references,
   native chunks (init 202510 etc.) are copied byte for byte.
 * `native/static` holds the FULL ocean_static.nc of the product (all 28 fields + the 1-D axes). Land is NaN (1e20 -> NaN, as in the other
   repos). The geolon/geolat family (incl. _c, _u, _v) comes from the NWA hindcast monthly r20250715 static, whose coordinates have no land gaps
   (checked equal to the product's own static wherever that is unmasked), as in the existing repos.
 * areacello/deptho/wet are removed from native/monthly (they now live once, in native/static); geolon/geolat stay there because the data
   variables name them in their `coordinates` attribute.
 * There is no `regrid/static`: NOAA ships no static file for the regridded grid (only the 1-D lat/lon, which are in regrid/monthly).
Work happens on branch `restructure` and `main` is moved to it only after the checks pass. The old repo is not touched.

Phases: --build (create repo + branch + copy + statics + checks, commits to the branch), --validate (fresh process, compares with the old repo),
--promote (point main at the branch). Usage: restructure_nwa12_seasonal.py --product forecast|reforecast [--build] [--validate] [--promote]
"""
from __future__ import annotations

import argparse
import base64
import io
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ORG = "noaa-cefi"
BUCKET = "noaa-oar-cefi-regional-mom6-pds"
SOURCE_URL = f"s3://{BUCKET}"
NICKNAME = BUCKET
SOURCE_NICKNAME = f"{BUCKET}-source"
HINDCAST_STATIC = "northwest_atlantic/full_domain/hindcast/monthly/raw/r20250715/ocean_static.nc"
BRANCH = "restructure"
PRODUCTS = {
    "forecast": dict(old="cefi-nwa-seasonal-forecast-monthly", new="nwa12-seasonal-forecast", exp="seasonal_forecast", release="r20250710",
                     desc="NOAA CEFI MOM6 NW Atlantic (nwa12) seasonal forecast, monthly, r20250710. Forecast layout (time = init, member, "
                          "step, valid_time). Groups: native/monthly, native/static, regrid/monthly. Virtual references; init 202510 native."),
    "reforecast": dict(old="cefi-nwa-seasonal-reforecast-monthly", new="nwa12-seasonal-reforecast", exp="seasonal_reforecast", release="r20250413",
                       desc="NOAA CEFI MOM6 NW Atlantic (nwa12) seasonal reforecast, monthly, r20250413. Forecast layout (time = init, "
                            "member, step, valid_time). Groups: native/monthly, native/static, regrid/monthly. Virtual references."),
}
COORDS = ("geolon", "geolat", "geolon_c", "geolat_c", "geolon_u", "geolat_u", "geolon_v", "geolat_v")
GRID_COORDS = {("yh", "xh"): "geolon geolat", ("yq", "xq"): "geolon_c geolat_c", ("yh", "xq"): "geolon_u geolat_u",
               ("yq", "xh"): "geolon_v geolat_v"}
MOVED_TO_STATIC = ("areacello", "deptho", "wet")
NAN_FILL = base64.b64encode(np.float64("nan").tobytes()).decode()  # the `_FillValue` encoding the other repos use


def source_store():
    import obstore as obs
    return obs.store.S3Store(BUCKET, region="us-east-1", skip_signature=True)


def _py(v):
    if isinstance(v, bytes):
        return v.decode()
    if isinstance(v, np.ndarray):
        return v.tolist()
    return v.item() if isinstance(v, np.generic) else v


def read_static(path: str) -> dict:
    """{name: (dims, float array in native byte order, attrs)} of a netCDF3 static file, read in memory."""
    import obstore as obs
    from scipy.io import netcdf_file
    f = netcdf_file(io.BytesIO(bytes(obs.get(source_store(), path).bytes())), mmap=False)
    out = {}
    for n, v in f.variables.items():
        a = np.array(v.data)
        a = a.astype(a.dtype.newbyteorder("="))
        attrs = {k: _py(getattr(v, k)) for k in v._attributes if k not in ("_FillValue", "missing_value")}
        out[n] = (tuple(v.dimensions), a, attrs)
    return out


def masked(a: np.ndarray) -> np.ndarray:
    """The 'no data' fill (1e20) as NaN, as in the other repos."""
    return np.where(np.abs(a) > 1e19, np.float32(np.nan), a).astype(a.dtype)


def build_static(cfg: dict) -> tuple[dict, list[str]]:
    own = read_static(f"northwest_atlantic/full_domain/{cfg['exp']}/monthly/raw/{cfg['release']}/ocean_static.nc")
    hind = read_static(HINDCAST_STATIC)
    notes, out = [], {}
    for name, (dims, a, attrs) in own.items():
        if len(dims) == 1:
            out[name] = (dims, a, attrs)
            continue
        if name in COORDS:
            assert name in hind, f"{name} missing from the hindcast static"
            h = hind[name][1]
            ok = np.abs(a) < 1e19
            assert h.shape == a.shape and np.array_equal(h[ok], a[ok]), f"{name}: hindcast static differs from the product's own where unmasked"
            assert np.isfinite(h).all() and (np.abs(h) < 1e19).all(), f"{name}: hindcast static has gaps"
            attrs = {**attrs, "comment": "taken from the NWA hindcast monthly r20250715 ocean_static.nc: this release's own ocean_static.nc "
                                         "masks land with 1e20; identical wherever that file is unmasked"}
            out[name] = (dims, h.astype("float32"), attrs)
            notes.append(f"{name}: from hindcast static, equal to own on {int(ok.sum())} unmasked cells")
        else:
            out[name] = (dims, masked(a.astype("float32")), attrs)
    return out, notes


def write_static(session, cfg: dict, static: dict) -> None:
    import zarr
    from zarr.codecs import BloscCodec
    g = zarr.open_group(session.store, path="native/static", mode="a")
    for name, (dims, a, attrs) in static.items():
        attrs = {k: v for k, v in attrs.items() if k != "coordinates"}
        if len(dims) == 2:
            attrs["coordinates"] = GRID_COORDS[dims]
            if a.dtype.kind == "f":
                attrs["_FillValue"] = NAN_FILL
        arr = g.create_array(name, shape=a.shape, dtype=a.dtype, chunks=a.shape, dimension_names=dims, attributes=attrs,
                             compressors=BloscCodec(cname="zstd", clevel=3, shuffle="shuffle"),
                             fill_value=float("nan") if a.dtype.kind == "f" else 0)
        arr[...] = a
    g.update_attributes({
        "title": "Static (time-invariant) grid fields of the native NWA12 grid",
        "applies_to": cfg.get("applies_to", ["native/monthly"]),
        "source": cfg.get("static_source") or (
            f"northwest_atlantic/full_domain/{cfg['exp']}/monthly/raw/{cfg['release']}/ocean_static.nc (all fields), "
            "geolon/geolat family from the NWA hindcast monthly r20250715 ocean_static.nc"),
        "comment": "land / no-data cells are NaN. Grid positions: h = tracer (yh, xh), q = corner (yq, xq), u = (yh, xq), v = (yq, xh)"})


def build(product: str) -> None:
    import icechunk as ic  # noqa: F401
    import zarr
    from arraylake import Client
    from utilities import copy_node

    cfg = PRODUCTS[product]
    assert len(cfg["desc"]) <= 255, len(cfg["desc"])
    new = f"{ORG}/{cfg['new']}"
    client = Client()
    old_repo = client.get_repo(f"{ORG}/{cfg['old']}")
    existing = {r.name for r in client.list_repos(ORG)}
    resume = cfg["new"] in existing or new in existing
    static, notes = build_static(cfg)
    print(f"static: {len(static)} arrays; " + "; ".join(notes[:3]) + " ...", flush=True)

    if resume:
        repo = client.get_repo(new)
        if len(list(repo.ancestry(branch="main"))) > 2:  # initial snapshot + our root commit
            raise SystemExit(f"{new} exists and main has more than the root commit; refusing to touch it")
        print(f"{new} exists with an untouched main: resuming the build on branch {BRANCH!r}", flush=True)
    else:
        repo = client.create_repo(new, bucket_config_nickname=NICKNAME, description=cfg["desc"],
                                  authorize_virtual_chunk_access={SOURCE_URL: SOURCE_NICKNAME})
        print(f"created {new}", flush=True)
        s = repo.writable_session("main")
        zarr.open_group(s.store, mode="a")
        root_commit = s.commit("Create root group")
        repo.create_branch(BRANCH, root_commit)

    src = old_repo.readonly_session(branch="main")
    for dst_group, src_group in (("native/monthly", "raw"), ("regrid/monthly", "regrid")):
        s = repo.writable_session(BRANCH)
        if _exists(s.store, dst_group):
            print(f"{dst_group} already copied", flush=True)
            continue
        zarr.open_group(s.store, path=dst_group.split("/")[0], mode="a")
        copy_node(src, src_group, s, dst_group)
        snap = s.commit(f"Copy {cfg['old']}/{src_group} to /{dst_group} (virtual chunk references and native chunks)")
        print(f"copied {src_group} -> {dst_group}: {snap}", flush=True)

    # statics: the old repo stored land as the literal 1e20 in areacello/deptho/wet (while declaring NaN as _FillValue, so xarray returns
    # 1e20 as data). Check that it equals the product's own static everywhere except those fill cells, then replace by NaN-masked arrays.
    s = repo.writable_session(BRANCH)
    if _exists(s.store, "native/static"):
        print("native/static already written", flush=True)
    else:
        nm = zarr.open_group(s.store, path="native/monthly", mode="r+")
        for n in MOVED_TO_STATIC:
            o, new_a = nm[n][...], static[n][1]
            fill = np.isnan(new_a)
            assert np.array_equal(o[~fill], new_a[~fill]), f"{n}: old repo array differs from the product's own static on valid cells"
            assert (o[fill] == np.float32(1e20)).all(), f"{n}: old repo array is not the 1e20 fill on the masked cells"
            print(f"  {n}: old repo equals the source on {int((~fill).sum())} valid cells; {int(fill.sum())} land cells were 1e20, now NaN", flush=True)
        write_static(s, cfg, static)
        for n in MOVED_TO_STATIC:
            del nm[n]
        nm.update_attributes({"static_group": "native/static"})
        snap = s.commit("Add native/static (full ocean_static, land as NaN) and remove areacello/deptho/wet from native/monthly "
                        "(now only in native/static)")
        print(f"statics: {snap}", flush=True)

    old_meta = client.get_repo_object(f"{ORG}/{cfg['old']}").metadata or {}
    client.modify_repo(new, update_metadata={**{k: v for k, v in old_meta.items() if k not in ("excluded",)}, "domain": "nwa12"})
    print(f"build done; work is on branch {BRANCH!r}; main is still the empty root", flush=True)


def _exists(store, path: str) -> bool:
    import zarr
    try:
        zarr.open_group(store, path=path, mode="r")
        return True
    except Exception:  # noqa: BLE001
        return False


def _chunk_table(store, path: str) -> dict:
    import icechunk as ic
    from zarr.core.sync import sync

    async def go():
        rows = {}
        async for coords, kinds, paths, offsets, lengths, _ in store.array_chunk_iterator(path):
            for i, k in enumerate(kinds):
                idx = tuple(int(c) for c in coords[i])
                if k == ic.ChunkType.virtual:
                    rows[idx] = ("virtual", str(paths[i]), int(offsets[i]), int(lengths[i]))
                else:  # native / inline: the object id differs between repos; the bytes are compared separately
                    rows[idx] = ("stored", None, None, int(lengths[i]))
        return rows
    return sync(go())


def validate(product: str) -> None:
    import xarray as xr
    import zarr
    from arraylake import Client
    
    cfg = PRODUCTS[product]
    client = Client()
    old = client.get_repo(f"{ORG}/{cfg['old']}").readonly_session(branch="main")
    new_repo = client.get_repo(f"{ORG}/{cfg['new']}")
    new = new_repo.readonly_session(branch=BRANCH)
    og, ng = zarr.open_group(old.store, mode="r"), zarr.open_group(new.store, mode="r")
    problems, n_arrays, n_virtual, n_native = [], 0, 0, 0
    for old_group, new_group in (("raw", "native/monthly"), ("regrid", "regrid/monthly")):
        oarr = dict(og[old_group].arrays())
        narr = dict(ng[new_group].arrays())
        expected = {k for k in oarr if not (old_group == "raw" and k in MOVED_TO_STATIC)}
        if set(narr) != expected:
            problems.append(f"{new_group}: array names differ: missing {sorted(expected - set(narr))}, extra {sorted(set(narr) - expected)}")
        if dict(og[old_group].attrs) != {k: v for k, v in dict(ng[new_group].attrs).items() if k != "static_group"}:
            problems.append(f"{new_group}: group attributes differ")
        for name in sorted(expected & set(narr)):
            a, b = oarr[name], narr[name]
            n_arrays += 1
            sig = lambda x: (x.shape, str(x.dtype), x.chunks, repr(sorted(dict(x.attrs).items())), x.metadata.dimension_names,  # noqa: E731
                             repr(x.metadata.fill_value), repr(x.metadata.codecs))
            if sig(a) != sig(b):
                problems.append(f"{new_group}/{name}: metadata differs")
                continue
            ta, tb = _chunk_table(old.store, f"{old_group}/{name}"), _chunk_table(new.store, f"{new_group}/{name}")
            if ta != tb:
                problems.append(f"{new_group}/{name}: chunk table differs ({len(ta)} vs {len(tb)} chunks)")
                continue
            kinds = [row[0] for row in ta.values()]
            n_virtual += kinds.count("virtual")
            n_native += kinds.count("stored")
    print(f"{n_arrays} arrays compared; {n_virtual} virtual chunk references identical, {n_native} native chunks present with identical lengths", flush=True)

    # statics
    own = read_static(f"northwest_atlantic/full_domain/{cfg['exp']}/monthly/raw/{cfg['release']}/ocean_static.nc")
    sg = dict(ng["native/static"].arrays())
    if set(sg) != set(own):
        problems.append(f"native/static: names differ: missing {sorted(set(own) - set(sg))}, extra {sorted(set(sg) - set(own))}")
    for name in sorted(set(sg) & set(own)):
        dims, a, _ = own[name]
        arr = sg[name]
        if tuple(arr.metadata.dimension_names) != dims or arr.shape != a.shape:
            problems.append(f"native/static/{name}: dims/shape differ")
            continue
        got = arr[...]
        if name not in COORDS and len(dims) == 2:
            exp = masked(a.astype("float32"))
            if not np.array_equal(got, exp, equal_nan=True):
                problems.append(f"native/static/{name}: values differ from the source file")
        elif len(dims) == 1 and not np.array_equal(got, a):
            problems.append(f"native/static/{name}: values differ from the source file")
        if len(dims) == 2 and name not in COORDS and "coordinates" not in arr.attrs:
            problems.append(f"native/static/{name}: no coordinates attribute")
    print(f"native/static: {len(sg)} arrays checked against the source ocean_static.nc", flush=True)

    # consumer check: open every group with xarray as a user would
    for group in ("native/monthly", "native/static", "regrid/monthly"):
        ds = xr.open_zarr(new.store, group=group, consolidated=False, zarr_format=3)
        print(f"xarray {group}: {len(ds.data_vars)} data variables, {len(ds.coords)} coordinates; dims {dict(ds.sizes)}", flush=True)
    ds = xr.open_zarr(new.store, group="native/static", consolidated=False, zarr_format=3)
    for expect in ("geolon", "geolat", "geolon_c", "geolat_c", "geolon_u", "geolat_u", "geolon_v", "geolat_v"):
        if expect not in ds.coords:
            problems.append(f"native/static: {expect} is not a coordinate when opened with xarray")
    dm = xr.open_zarr(new.store, group="native/monthly", consolidated=False, zarr_format=3)
    do = xr.open_zarr(old.store, group="raw", consolidated=False, zarr_format=3)
    var = next(v for v in do.data_vars if do[v].ndim == 5)
    for label, sel in (("first init (virtual)", slice(0, 1)), ("last init (native)", slice(-1, None))):
        a = do[var].isel(time=sel, member=0, step=0).values
        b = dm[var].isel(time=sel, member=0, step=0).values
        if not np.array_equal(a, b, equal_nan=True):
            problems.append(f"{var}: {label}: values read through xarray differ from the old repo")
        else:
            print(f"read {var} through xarray, {label}: identical in old and new repo ({a.shape})", flush=True)
    anyvar = dm[var]
    if "geolon" not in anyvar.coords:
        problems.append(f"{var}: geolon is not attached as a coordinate in native/monthly")
    print("PROBLEMS:" if problems else "OK: no problems found", flush=True)
    for p in problems:
        print("  -", p)
    if problems:
        raise SystemExit(1)


def promote(product: str) -> None:
    from arraylake import Client
    cfg = PRODUCTS[product]
    repo = Client().get_repo(f"{ORG}/{cfg['new']}")
    main_tip = repo.lookup_branch("main")
    repo.reset_branch("main", repo.lookup_branch(BRANCH), from_snapshot_id=main_tip)
    repo.delete_branch(BRANCH)
    print(f"main of {ORG}/{cfg['new']} now at {repo.lookup_branch('main')}; branch {BRANCH!r} deleted", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--product", choices=list(PRODUCTS), required=True)
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--promote", action="store_true")
    a = ap.parse_args()
    if a.build:
        build(a.product)
    if a.validate:
        validate(a.product)
    if a.promote:
        promote(a.product)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
