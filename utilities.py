"""Restructure an existing (virtual) Icechunk repo in place.

Every function here only rewrites metadata or chunk *references*: virtual
chunks stay virtual and nothing is re-read from the source NetCDF files.
See RESTRUCTURING.md for usage, and test_utilities.py for the tests.
"""

import asyncio
import json

import icechunk as ic
import zarr
from zarr.core.buffer import default_buffer_prototype
from zarr.core.sync import sync

# Attributes (CF conventions) whose values name other variables in the group.
LINK_ATTRS = ("coordinates", "bounds", "ancillary_variables", "cell_measures", "grid_mapping")


def _node_path(group: str, name: str = "") -> str:
    parts = [p for p in (group.strip("/"), name.strip("/")) if p]
    return "/" + "/".join(parts)


def _read_json(store, key: str) -> dict:
    buf = sync(store.get(key, prototype=default_buffer_prototype()))
    return json.loads(buf.to_bytes())


def _write_json(store, key: str, doc: dict) -> None:
    buf = default_buffer_prototype().buffer.from_bytes(json.dumps(doc).encode())
    sync(store.set(key, buf))


def _replace_token(value: str, old: str, new: str) -> str:
    return " ".join(new if tok == old else tok for tok in value.split())


# ---------------------------------------------------------------------------
# (1) Rename a variable
# ---------------------------------------------------------------------------
def rename_variable(repo: ic.Repository, group: str, old: str, new: str, *, branch: str = "main") -> None:
    """Rename array ``old`` to ``new`` inside ``group``, fixing every reference to it.

    If ``old`` is a dimension coordinate (e.g. ``xh``) the dimension is renamed
    in every array of the group too. Makes two commits: one for the metadata
    references, one for the rename itself.
    """
    # Step 1: rewrite references to `old` in the metadata of the other arrays
    session = repo.writable_session(branch)
    store = session.store
    grp = zarr.open_group(store, path=group, mode="r")
    if old not in grp or new in grp:
        raise ValueError(f"need {old!r} to exist and {new!r} not to exist in {group!r}")

    keys = [f"{arr.path}/zarr.json" for _, arr in grp.arrays()]
    keys.append(f"{grp.path}/zarr.json" if grp.path else "zarr.json")  # group-level attributes
    for key in keys:
        doc = _read_json(store, key)
        changed = False
        dims = doc.get("dimension_names")
        if dims and old in dims:
            doc["dimension_names"] = [new if d == old else d for d in dims]
            changed = True
        attrs = doc.get("attributes", {})
        for attr in LINK_ATTRS:
            if isinstance(attrs.get(attr), str) and old in attrs[attr].split():
                attrs[attr] = _replace_token(attrs[attr], old, new)
                changed = True
        if changed:
            _write_json(store, key, doc)
    if session.has_uncommitted_changes:
        session.commit(f"Update references to {old!r} -> {new!r} in {group or '/'}")

    # Step 2: rename the array itself (a pure metadata operation)
    session = repo.rearrange_session(branch)
    session.move(_node_path(group, old), _node_path(group, new))
    session.commit(f"Rename {_node_path(group, old)} -> {_node_path(group, new)}")


# ---------------------------------------------------------------------------
# (2) Promote / demote coordinates
# ---------------------------------------------------------------------------
def _dims(arr) -> tuple[str, ...]:
    return tuple(arr.metadata.dimension_names or ())


def _coordinate_names(grp) -> set[str]:
    arrays = dict(grp.arrays())
    names = {n for n, a in arrays.items() if _dims(a) == (n,)}  # dimension coordinates
    for a in [*arrays.values(), grp]:
        names.update(str(a.attrs.get("coordinates", "")).split())
    return names


def set_coords(repo: ic.Repository, group: str, names: list[str], *, branch: str = "main") -> None:
    """Make the arrays ``names`` coordinates (like ``xr.Dataset.set_coords``)."""
    session = repo.writable_session(branch)
    grp = zarr.open_group(session.store, path=group, mode="r+")
    arrays = dict(grp.arrays())
    for name in names:
        if name not in arrays:
            raise KeyError(f"{name!r} not found in {group!r}")
        data_vars = {n: a for n, a in arrays.items() if n not in _coordinate_names(grp)}
        # like xarray: attach to every data variable that spans the coordinate's dimensions
        targets = [a for n, a in data_vars.items() if n != name and set(_dims(arrays[name])) <= set(_dims(a))]
        for a in targets or [grp]:  # no such data variable: record it on the group instead
            current = str(a.attrs.get("coordinates", "")).split()
            if name not in current:
                a.update_attributes({"coordinates": " ".join([*current, name])})
    session.commit(f"Set coordinates {names} in {group or '/'}")


def reset_coords(repo: ic.Repository, group: str, names: list[str], *, branch: str = "main") -> None:
    """Turn coordinates ``names`` back into data variables (like ``xr.Dataset.reset_coords``)."""
    session = repo.writable_session(branch)
    grp = zarr.open_group(session.store, path=group, mode="r+")
    arrays = dict(grp.arrays())
    for name in names:
        if name in arrays and _dims(arrays[name]) == (name,):
            raise ValueError(f"{name!r} is a dimension coordinate; it cannot become a data variable")
    for a in [*arrays.values(), grp]:
        current = str(a.attrs.get("coordinates", "")).split()
        kept = [c for c in current if c not in names]
        if kept != current:
            if kept:
                a.update_attributes({"coordinates": " ".join(kept)})
            else:
                del a.attrs["coordinates"]
    session.commit(f"Reset coordinates {names} in {group or '/'}")


# ---------------------------------------------------------------------------
# (3) + (4) Move arrays or groups
# ---------------------------------------------------------------------------
def move_node(repo: ic.Repository, src: str, dst: str, *, branch: str = "main") -> None:
    """Move an array or group (with everything inside it) from ``src`` to ``dst``.

    Missing parent groups of ``dst`` are created first (one extra commit).
    """
    parent = dst.rstrip("/").rsplit("/", 1)[0]
    if parent:
        session = repo.writable_session(branch)
        zarr.open_group(session.store, path=parent, mode="a")  # creates missing groups
        if session.has_uncommitted_changes:
            session.commit(f"Create group {parent}")
    session = repo.rearrange_session(branch)
    session.move(src, dst)
    session.commit(f"Move {src} -> {dst}")


# ---------------------------------------------------------------------------
# (5) Copy a group or array, possibly from a different repo
# ---------------------------------------------------------------------------
async def _copy_node_async(src_store, src_path, dst_store, dst_path, nodes, max_concurrency) -> None:
    proto = default_buffer_prototype()
    limit = asyncio.Semaphore(max_concurrency)

    def join(*parts):
        return "/".join(p for p in parts if p)

    async def copy_key(src_key, dst_key):
        async with limit:
            buf = await src_store.get(src_key, prototype=proto)
            await dst_store.set(dst_key, buf)

    for rel, node in nodes:
        src_node, dst_node = join(src_path, rel), join(dst_path, rel)
        # metadata (zarr.json) is copied verbatim
        await copy_key(join(src_node, "zarr.json"), join(dst_node, "zarr.json"))
        if not isinstance(node, zarr.Array):
            continue
        # Drain the iterator before writing: it holds a read lock on the source
        # session, so writing while it is open deadlocks when src and dst are the same session.
        batches = [batch async for batch in src_store.array_chunk_iterator(src_node)]
        for coords, kinds, paths, offsets, lengths, _ in batches:
            refs, copies = [], []
            for i, kind in enumerate(kinds):
                index = [int(c) for c in coords[i]]
                if kind == ic.ChunkType.virtual:
                    refs.append(ic.VirtualChunkSpec(index, paths[i], int(offsets[i]), int(lengths[i])))
                else:  # native or inline: copy the stored (still encoded) bytes
                    chunk_key = node.metadata.encode_chunk_key(tuple(index))
                    copies.append(copy_key(join(src_node, chunk_key), join(dst_node, chunk_key)))
            if refs:
                failed = await dst_store.set_virtual_refs_async("/" + dst_node, refs, validate_containers=True)
                if failed:
                    raise ValueError(
                        f"{len(failed)} virtual refs of /{src_node} point outside the destination "
                        "repo's virtual chunk containers"
                    )
            await asyncio.gather(*copies)


def copy_node(
    src_session: ic.Session,
    src_path: str,
    dst_session: ic.Session,
    dst_path: str,
    *,
    max_concurrency: int = 32,
) -> None:
    """Copy the array or group at ``src_path`` (and everything inside it) to ``dst_path``.

    Virtual chunks are copied as virtual references; native and inline chunks
    are copied byte for byte. The sessions can belong to the same repo or to
    different repos. The parent of ``dst_path`` must exist and ``dst_path``
    must not. Nothing is committed: call ``dst_session.commit(...)`` afterwards.
    """
    src_path, dst_path = src_path.strip("/"), dst_path.strip("/")
    root = zarr.open(src_session.store, path=src_path, mode="r")
    nodes = [("", root)]
    if isinstance(root, zarr.Group):
        nodes += sorted(root.members(max_depth=None), key=lambda m: m[0].count("/"))  # parents first
    sync(_copy_node_async(src_session.store, src_path, dst_session.store, dst_path, nodes, max_concurrency))
