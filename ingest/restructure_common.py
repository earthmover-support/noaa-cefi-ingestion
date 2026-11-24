"""Helpers shared by the restructure_*.py scripts that merge several old repos / subgroups into one new repo (see REVISIONS.md)."""
from __future__ import annotations

import numpy as np

import restructure_nwa12_seasonal as base

UV_COORDS = {"u": ("geolon_u", "geolat_u"), "v": ("geolon_v", "geolat_v")}


def merge_subgroups(src_session, old_group, s, dst_group: str, subs: list[str], renames: dict | None = None) -> dict[str, str]:
    """Copy the arrays of old_group/<sub> for every sub into ONE group `dst_group` of session `s` (copy_node, one array at a time).

    Names that exist in several subgroups (shared coordinates such as time, yh, xq) are kept once: their values must be identical and their
    attributes are merged (a conflicting attribute value is an error). `renames[sub]` maps old names to new names for subgroups whose
    names would collide with arrays of the same name on other dimensions; the `coordinates` attributes of that subgroup's arrays are updated.
    Returns {new name: subgroup it was copied from}."""
    import zarr
    from utilities import copy_node

    renames = renames or {}
    og = zarr.open_group(src_session.store, mode="r")
    grp = zarr.open_group(s.store, path=dst_group, mode="a")
    grp.update_attributes(dict(og[f"{old_group}/{subs[0]}"].attrs))
    seen: dict[str, list[str]] = {}
    for sub in subs:
        for name, _ in og[f"{old_group}/{sub}"].arrays():
            seen.setdefault(renames.get(sub, {}).get(name, name), []).append(sub)
    inverse = {(sub, renames.get(sub, {}).get(n, n)): n for sub in subs for n, _ in og[f"{old_group}/{sub}"].arrays()}
    origin = {}
    for new_name, in_subs in seen.items():
        arrs = [og[f"{old_group}/{x}/{inverse[(x, new_name)]}"] for x in in_subs]
        merged = None
        if len(in_subs) > 1:
            for a in arrs[1:]:
                assert a.shape == arrs[0].shape and a.dtype == arrs[0].dtype and np.array_equal(a[...], arrs[0][...], equal_nan=a.dtype.kind == "f"), \
                    f"{new_name}: copies in {in_subs} differ"
            merged = {}
            for a in arrs:
                for k, v in dict(a.attrs).items():
                    assert merged.setdefault(k, v) == v, f"{new_name}: attribute {k!r} conflicts between {in_subs}"
        sub = in_subs[0]
        old_name = inverse[(sub, new_name)]
        copy_node(src_session, f"{old_group}/{sub}/{old_name}", s, f"{dst_group}/{new_name}")
        arr = zarr.open_array(s.store, path=f"{dst_group}/{new_name}", mode="r+")
        attrs = merged if merged is not None else dict(arr.attrs)
        if renames.get(sub) and "coordinates" in attrs:
            attrs["coordinates"] = " ".join(renames[sub].get(tok, tok) for tok in str(attrs["coordinates"]).split())
        if merged is not None or renames.get(sub):
            arr.update_attributes(attrs)
        origin[new_name] = sub
    return origin


def check_moved(label: str, old: np.ndarray, new: np.ndarray) -> str:
    """Assert the old repo's static array equals the source static on every cell valid in both; describe the rest."""
    o_valid, s_valid = np.isfinite(old) & (np.abs(old) < 1e19), ~np.isnan(new)
    both = o_valid & s_valid
    assert np.array_equal(old[both], new[both]), f"{label}: old repo differs from the source on cells valid in both"
    rest = old[~o_valid]
    fill = "1e20" if rest.size and (rest == np.float32(1e20)).all() else ("NaN" if rest.size and np.isnan(rest).all() else ("-" if not rest.size else "mixed"))
    return (f"{label}: equal on {int(both.sum())} cells valid in both; valid only in the old repo {int((o_valid & ~s_valid).sum())}, "
            f"only in the static {int((s_valid & ~o_valid).sum())}; old fill cells were {fill}; all become NaN")


def add_uv_coordinates(group, static: dict, kinds=("u", "v"), u_dims=("yh", "xq"), v_dims=("yq", "xh")) -> None:
    """Add geolon_u/geolat_u (and _v) to the data group and list them in `coordinates` of the u-grid / v-grid variables
    (dims ending in `u_dims` / `v_dims`, default (yh, xq) / (yq, xh)), as those variables had no 2-D coordinates."""
    from zarr.codecs import BloscCodec
    for kind in kinds:
        for nm in UV_COORDS[kind]:
            dims, a, attrs = static[nm]
            arr = group.create_array(nm, shape=a.shape, dtype=a.dtype, chunks=a.shape, dimension_names=dims,
                                     attributes={**{k: v for k, v in attrs.items() if k != "coordinates"}, "_FillValue": base.NAN_FILL},
                                     compressors=BloscCodec(cname="zstd", clevel=3, shuffle="shuffle"), fill_value=float("nan"))
            arr[...] = a
    for name, arr in group.arrays():
        d = tuple(arr.metadata.dimension_names or ())
        extra = None
        if "u" in kinds and arr.ndim >= 3 and d[-2:] == u_dims:
            extra = UV_COORDS["u"]
        elif "v" in kinds and arr.ndim >= 3 and d[-2:] == v_dims:
            extra = UV_COORDS["v"]
        if extra:
            cur = str(arr.attrs.get("coordinates", "")).split()
            arr.update_attributes({"coordinates": " ".join(cur + [c for c in extra if c not in cur])})
