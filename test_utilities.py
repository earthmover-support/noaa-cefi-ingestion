# /// script
# dependencies = ["icechunk==2.3.1", "zarr==3.4.2", "xarray", "virtualizarr[icechunk]==2.8.0", "obstore", "numpy", "arro3-core"]
# ///
"""Check utilities.py on local repos shaped like the CEFI ones (see RESTRUCTURING.md)."""

import shutil
import sys
import tempfile
from pathlib import Path

import icechunk as ic
import numpy as np
import xarray as xr
import zarr
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import LocalStore
from virtualizarr import open_virtual_dataset
from virtualizarr.parsers import ZarrParser

sys.path.insert(0, str(Path(__file__).parent))
from utilities import copy_node, move_node, rename_variable, reset_coords, set_coords  # noqa: E402

TMP = Path(tempfile.mkdtemp())
SRC_PREFIX = f"file://{TMP}/sources/"


def kinds(session, path):
    out = set()

    async def go():
        async for _, k, *_ in session.store.array_chunk_iterator(path):
            out.update(int(x) for x in k)

    zarr.core.sync.sync(go())
    return {{1: "native", 2: "virtual", 3: "inline"}[x] for x in out}


def source_dataset(seed):
    rng = np.random.default_rng(seed)
    return xr.Dataset(
        {
            "tos": (("time", "yh", "xh"), rng.random((4, 30, 50), dtype="f4"), {"long_name": "SST"}),
            "sos": (("time", "yh", "xh"), rng.random((4, 30, 50), dtype="f4"), {"long_name": "SSS"}),
            "areacello": (("yh", "xh"), rng.random((30, 50), dtype="f4")),
        },
        coords={
            "time": np.arange(4.0),
            "yh": np.arange(30.0),
            "xh": np.arange(50.0) + 100,
            "geolon": (("yh", "xh"), rng.random((30, 50), dtype="f4")),
        },
    )


def make_repo(name, seed):
    ds = source_dataset(seed)
    src = TMP / "sources" / f"{name}.zarr"
    ds.to_zarr(src, zarr_format=3, consolidated=False, encoding={v: {"chunks": (1, 30, 50)} for v in ("tos", "sos")})
    vds = open_virtual_dataset(
        url=f"file://{src}",
        parser=ZarrParser(),
        registry=ObjectStoreRegistry({"file://": LocalStore()}),
    )
    config = ic.RepositoryConfig.default()
    config.set_virtual_chunk_container(ic.VirtualChunkContainer(SRC_PREFIX, ic.local_filesystem_store(str(TMP / "sources"))))
    repo = ic.Repository.create(
        ic.local_filesystem_storage(str(TMP / name)),
        config,
        authorize_virtual_chunk_access={SRC_PREFIX: ic.credentials.LocalFileSystemAccess},
    )
    session = repo.writable_session("main")
    vds.vz.to_icechunk(session.store, group="raw/main")
    # a natively-stored array, like the NEP daily 3-D variables
    grp = zarr.open_group(session.store, path="raw/main", mode="r+")
    arr = grp.create_array("native_var", shape=(4, 30, 50), chunks=(1, 30, 50), dtype="f4", dimension_names=("time", "yh", "xh"))
    arr[:] = ds.tos.values * 2
    arr.attrs["coordinates"] = "geolon"
    small = grp.create_array("small_var", shape=(4,), chunks=(4,), dtype="f4", dimension_names=("time",))
    small[:] = np.arange(4.0)  # 16 bytes: stored inline in the manifest
    session.commit("initial")
    ds["native_var"] = ds.tos * 2
    ds["small_var"] = ("time", np.arange(4.0, dtype="f4"))
    return repo, ds


def open_ds(repo, group="raw/main"):
    return xr.open_zarr(repo.readonly_session("main").store, group=group, consolidated=False, chunks=None).load()


def check(cond, msg):
    print(("PASS " if cond else "FAIL ") + msg)
    if not cond:
        raise SystemExit(1)


# ---------------------------------------------------------------- setup
repo, ds = make_repo("a", 0)
before = open_ds(repo)
print(before)
print({n: dict(a.attrs) for n, a in zarr.open_group(repo.readonly_session("main").store, path="raw/main", mode="r").arrays() if "coordinates" in a.attrs})
check(kinds(repo.readonly_session("main"), "/raw/main/tos") == {"virtual"}, "tos is virtual after ingest")
check(kinds(repo.readonly_session("main"), "/raw/main/native_var") == {"native"}, "native_var is native")
check(kinds(repo.readonly_session("main"), "/raw/main/small_var") == {"inline"}, "small_var is inline")
check("geolon" in before.coords, "geolon starts as a coordinate")

# ---------------------------------------------------------------- (1a) rename data variable
rename_variable(repo, "raw/main", "tos", "sst")
after = open_ds(repo)
check("sst" in after and "tos" not in after, "data var renamed")
np.testing.assert_array_equal(after.sst.values, before.tos.values)
check(kinds(repo.readonly_session("main"), "/raw/main/sst") == {"virtual"}, "renamed data var still virtual")

# ---------------------------------------------------------------- (1b) rename non-dimension coordinate
rename_variable(repo, "raw/main", "geolon", "lon2d")
after = open_ds(repo)
check("lon2d" in after.coords and "geolon" not in after.variables, "2-D coord renamed and still a coordinate")
check(after.sst.attrs.get("coordinates") is None, "xarray consumed coordinates attr")  # decoded away

# ---------------------------------------------------------------- (1c) rename dimension coordinate
rename_variable(repo, "raw/main", "xh", "lon")
after = open_ds(repo)
check("lon" in after.dims and "xh" not in after.dims and "lon" in after.indexes, "dimension coordinate renamed everywhere")
np.testing.assert_array_equal(after.lon.values, before.xh.values)
np.testing.assert_array_equal(after.sst.values, before.tos.values)
np.testing.assert_array_equal(after.native_var.values, before.native_var.values)
s = repo.readonly_session("main")
check(kinds(s, "/raw/main/sst") == {"virtual"} and kinds(s, "/raw/main/native_var") == {"native"}, "chunk kinds preserved after dim rename")

# ---------------------------------------------------------------- (2) promote / demote
set_coords(repo, "raw/main", ["areacello"])
after = open_ds(repo)
check("areacello" in after.coords, "areacello promoted to coordinate")
reset_coords(repo, "raw/main", ["areacello", "lon2d"])
after = open_ds(repo)
check("areacello" in after.data_vars and "lon2d" in after.data_vars, "areacello and lon2d demoted to data variables")
try:
    reset_coords(repo, "raw/main", ["time"])
    check(False, "demoting a dimension coordinate should fail")
except ValueError:
    check(True, "demoting a dimension coordinate is refused")
set_coords(repo, "raw/main", ["lon2d"])
check("lon2d" in open_ds(repo).coords, "lon2d promoted back")

# ---------------------------------------------------------------- (3) move array into a new (nested) group
move_node(repo, "/raw/main/sos", "/derived/salinity/sos")
s = repo.readonly_session("main")
moved = zarr.open_array(s.store, path="derived/salinity/sos", mode="r")
np.testing.assert_array_equal(moved[:], before.sos.values)
check(kinds(s, "/derived/salinity/sos") == {"virtual"}, "moved array still virtual")
check("sos" not in open_ds(repo), "sos gone from old group")

# ---------------------------------------------------------------- (4) move group
move_node(repo, "/raw", "/products/nwa/raw")
after = open_ds(repo, "products/nwa/raw/main")
np.testing.assert_array_equal(after.sst.values, before.tos.values)
check(kinds(repo.readonly_session("main"), "/products/nwa/raw/main/sst") == {"virtual"}, "group moved, contents still virtual")

# ---------------------------------------------------------------- (3b) copy coordinates alongside a moved variable (same repo)
session = repo.writable_session("main")
for c in ["time", "yh", "lon"]:
    copy_node(session, f"/products/nwa/raw/main/{c}", session, f"/derived/salinity/{c}")
session.commit("Copy coordinates for sos")
sal = open_ds(repo, "derived/salinity")
check(set(sal.indexes) == {"time", "yh", "lon"} and "sos" in sal, "same-repo copy of coordinates works")
check(kinds(repo.readonly_session("main"), "/derived/salinity/time") == kinds(repo.readonly_session("main"), "/products/nwa/raw/main/time"), "copied coords keep chunk kind")

# ---------------------------------------------------------------- (5) combine repos
repo_b, ds_b = make_repo("b", 1)
config = ic.RepositoryConfig.default()
config.set_virtual_chunk_container(ic.VirtualChunkContainer(SRC_PREFIX, ic.local_filesystem_store(str(TMP / "sources"))))
combined = ic.Repository.create(
    ic.local_filesystem_storage(str(TMP / "combined")),
    config,
    authorize_virtual_chunk_access={SRC_PREFIX: ic.credentials.LocalFileSystemAccess},
)
dst = combined.writable_session("main")
zarr.open_group(dst.store, mode="a")  # root group
copy_node(repo.readonly_session("main"), "/", dst, "/repo_a")
copy_node(repo_b.readonly_session("main"), "/", dst, "/repo_b")
dst.commit("Combine repo_a and repo_b")

ca = open_ds(combined, "repo_a/products/nwa/raw/main")
cb = open_ds(combined, "repo_b/raw/main")
xr.testing.assert_identical(ca, open_ds(repo, "products/nwa/raw/main"))
xr.testing.assert_identical(cb, open_ds(repo_b))
s = combined.readonly_session("main")
check(kinds(s, "/repo_b/raw/main/tos") == {"virtual"}, "combined: virtual stays virtual")
check(kinds(s, "/repo_b/raw/main/native_var") == {"native"}, "combined: native copied as native")
check(kinds(s, "/repo_b/raw/main/small_var") == {"inline"}, "combined: inline copied as inline")
print(zarr.open_group(s.store, mode="r").tree())

# copied native chunks must not depend on the source repo
shutil.rmtree(TMP / "b")
cb2 = open_ds(ic.Repository.open(ic.local_filesystem_storage(str(TMP / "combined")), authorize_virtual_chunk_access={SRC_PREFIX: ic.credentials.LocalFileSystemAccess}), "repo_b/raw/main")
xr.testing.assert_identical(cb2, cb)
check(True, "combined repo readable after deleting source repo b")
# ---------------------------------------------------------------- branch workflow
main_tip = repo.lookup_branch("main")
repo.create_branch("restructure", main_tip)
rename_variable(repo, "products/nwa/raw/main", "sst", "sea_surface_temperature", branch="restructure")
check("sst" in open_ds(repo, "products/nwa/raw/main"), "main untouched while working on a branch")
repo.reset_branch("main", repo.lookup_branch("restructure"), from_snapshot_id=main_tip)
repo.delete_branch("restructure")
check("sea_surface_temperature" in open_ds(repo, "products/nwa/raw/main"), "main fast-forwarded to branch")
print("ALL OK")
