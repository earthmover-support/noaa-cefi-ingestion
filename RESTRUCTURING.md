# Restructuring the NOAA CEFI Icechunk repositories

This guide shows how to make structural and metadata changes to the repos in the `NOAA-CEFI` Arraylake organization:

1. [Rename a variable](#1-rename-a-variable)
2. [Turn a variable into a coordinate, or back into a data variable](#2-promote-or-demote-coordinates)
3. [Move arrays to a different group](#3-move-arrays-to-a-different-group)
4. [Move groups to a different place in the hierarchy](#4-move-groups)
5. [Combine several repos into one](#5-combine-several-repos-into-one)
6. [Change a repo's description](#6-change-a-repos-description)

None of these operations re-read or re-ingest the NOAA NetCDF files. They only rewrite Zarr metadata (`zarr.json`) and chunk *references*, so virtual chunks stay virtual and each change takes seconds. The exception is combining repos that hold natively stored data (see section 5).

Every change is an Icechunk commit, so it is versioned and can be undone (see [Undoing a change](#undoing-a-change)).

## Setup

```shell
pip install arraylake "icechunk>=2.0.5" "zarr>=3" xarray
arraylake auth login
```

Download [`utilities.py`](utilities.py) from this repository and save it next to your script or notebook. It provides five functions:

| Function | Task |
|---|---|
| `rename_variable(repo, group, old, new)` | 1 |
| `set_coords(repo, group, names)` / `reset_coords(repo, group, names)` | 2 |
| `move_node(repo, src, dst)` | 3, 4 |
| `copy_node(src_session, src_path, dst_session, dst_path)` | 3, 5 |

Each repo has two top-level groups, `raw` and `regrid`, and each of those contains one or more Xarray datasets (`raw/main`, `raw/ice`, `regrid/main`, …). Group paths in the functions below are written without a leading slash, e.g. `"raw/main"`. Node paths for moves start with `/`, e.g. `"/raw/main/tos"`. To see a repo's layout:

```python
import zarr
from arraylake import Client

client = Client()
repo = client.get_repo("NOAA-CEFI/cefi-nwa-hindcast-monthly")
print(zarr.open_group(repo.readonly_session("main").store, mode="r").tree())
```

## Recommended workflow: make changes on a branch first

Every function takes a `branch=` argument (default `"main"`). Making changes on a scratch branch lets you check the result before anyone reading `main` sees it:

```python
from utilities import rename_variable

repo = client.get_repo("NOAA-CEFI/cefi-nwa-hindcast-monthly")
main_tip = repo.lookup_branch("main")
repo.create_branch("restructure", main_tip)

rename_variable(repo, "raw/main", "tos", "sea_surface_temperature", branch="restructure")
# ... more changes, all with branch="restructure" ...

# Check the result the way your users will see it
import xarray as xr
ds = xr.open_zarr(repo.readonly_session(branch="restructure").store, group="raw/main", consolidated=False)
print(ds)

# Happy? Point main at the branch, then delete the branch.
# from_snapshot_id makes this fail instead of overwriting anything if main changed in the meantime.
repo.reset_branch("main", repo.lookup_branch("restructure"), from_snapshot_id=main_tip)
repo.delete_branch("restructure")
```

Renaming or moving variables changes how existing users and code address the data. If people depend on the current layout, consider tagging the current state first (`repo.create_tag("before-restructure", main_tip)`), so it stays readable at a fixed name.

## 1. Rename a variable

```python
from utilities import rename_variable

rename_variable(repo, "raw/main", "tos", "sea_surface_temperature")
```

The function handles the three kinds of variable:

- **Data variable** (e.g. `tos`): the array is renamed.
- **Non-dimension coordinate** (e.g. `geolon`): the array is renamed, and the name is updated in every `coordinates` attribute that lists it. Otherwise Xarray would stop treating it as a coordinate. The CF attributes `bounds`, `ancillary_variables`, `cell_measures` and `grid_mapping` are updated the same way.
- **Dimension coordinate** (e.g. `xh`): the array is renamed, and the dimension is renamed in every array of the group that uses it. `rename_variable(repo, "raw/main", "xh", "lon")` is the equivalent of `ds.rename({"xh": "lon"})`.

Notes:

- It makes two commits: one updating references in the other arrays' metadata, then one for the rename itself. (Icechunk performs renames in a dedicated "rearrange" session, separate from other edits.)
- Only the given group is changed. If the same dimension name appears in other groups (e.g. both `raw/main` and `regrid/main`), run it once per group.
- It refuses to run if `new` already exists in the group.

## 2. Promote or demote coordinates

Xarray decides which variables are coordinates from the `coordinates` attribute of each array. In these repos, for example, every data variable on the tracer grid has `coordinates: "geolon geolat"`. Dimension coordinates (1-D arrays named after their dimension, like `time` or `xh`) are always coordinates.

```python
from utilities import set_coords, reset_coords

# make `areacello` a coordinate (like ds.set_coords("areacello"))
set_coords(repo, "raw/main", ["areacello"])

# turn coordinates back into data variables (like ds.reset_coords(["geolon", "geolat"]))
reset_coords(repo, "raw/main", ["geolon", "geolat"])
```

`set_coords` adds the name to the `coordinates` attribute of every data variable whose dimensions include all of the new coordinate's dimensions (what Xarray itself does when writing). If no data variable qualifies, it records the name on the group instead. `reset_coords` removes the name from every `coordinates` attribute in the group. Dimension coordinates cannot be demoted, and `reset_coords` raises an error if you try.

Each call makes one commit and changes only attributes.

## 3. Move arrays to a different group

```python
from utilities import move_node

move_node(repo, "/raw/main/tos", "/raw/surface/tos")
```

Missing parent groups (here `/raw/surface`) are created first, in their own commit. The array keeps all its chunks (virtual or native) and attributes.

Moving a single variable does **not** bring its coordinates along. Opened with Xarray, `raw/surface` would contain `tos` but no `time`, `yh`, `xh`, `geolon` or `geolat`. To keep the new group a self-contained dataset, copy the coordinates in as well. Copies of virtual arrays are just new references to the same NetCDF bytes:

```python
from utilities import copy_node

session = repo.writable_session("main")
for name in ["time", "yh", "xh", "geolon", "geolat"]:
    copy_node(session, f"/raw/main/{name}", session, f"/raw/surface/{name}")
session.commit("Copy coordinates for raw/surface")
```

Alternatively, organise the hierarchy so that shared coordinates live in a parent group. `xarray.open_datatree` makes child groups inherit the coordinates of their parents.

## 4. Move groups

```python
move_node(repo, "/raw/ice", "/sea_ice/raw")
```

A group moves with everything inside it. As with arrays, missing parent groups are created first. Icechunk enforces some rules here:

- A move never overwrites: if something already exists at the destination, delete or move it first.
- A group cannot be moved into its own subtree (`/raw` → `/raw/old` is rejected). Create the new group and move the children into it one by one instead.

## 5. Combine several repos into one

Icechunk has no "merge two repos" operation, but `copy_node` can copy a whole group from one repo into another. Virtual chunks are copied as **virtual references** (pointing at the same NOAA NetCDF files). Natively stored chunks are copied **byte for byte**, so the new repo does not depend on the old ones. You can delete the old repos afterwards without breaking the new one.

The new repo must be allowed to reference the NOAA bucket, which on Arraylake is set when the repo is created:

```python
import zarr
from arraylake import Client
from utilities import copy_node

client = Client()
combined = client.create_repo(
    "NOAA-CEFI/cefi-nwa-hindcast",
    bucket_config_nickname="noaa-oar-cefi-regional-mom6-pds",
    description="NOAA CEFI MOM6 Northwest Atlantic hindcast: daily and monthly",
    authorize_virtual_chunk_access={
        "s3://noaa-oar-cefi-regional-mom6-pds/": "noaa-oar-cefi-regional-mom6-pds-source"
    },
)

session = combined.writable_session("main")
zarr.open_group(session.store, mode="a")  # make sure the root group exists
session.commit("Create root group")

sources = {
    "daily": "NOAA-CEFI/cefi-nwa-hindcast-daily",
    "monthly": "NOAA-CEFI/cefi-nwa-hindcast-monthly",
}
for name, source in sources.items():
    src = client.get_repo(source).readonly_session("main")
    session = combined.writable_session("main")
    copy_node(src, "/", session, f"/{name}")  # source root -> /daily, /monthly
    session.commit(f"Copy {source} into /{name}")
```

The result has `/daily/raw/main`, `/monthly/regrid/ice`, and so on. Group attributes are copied too.

Things to know:

- **Natively stored data is really copied.** Most of these repos are entirely virtual and copy in seconds to minutes. `cefi-nep-hindcast-daily` holds about 4.2 TB of native 3-D variables, and copying it means downloading and re-uploading all of it. Run that from a machine in AWS `us-east-1` and expect hours, or copy it one group at a time (`copy_node(src, "/raw/main", session, "/nep_daily/raw/main")` with a commit after each). The smaller native pieces (seasonal forecast init 202510, multi-decadal `T_adx` SSP585) are not a concern.
- **History is not carried over.** The new repo starts with a fresh history; the old repos keep theirs.
- **Arraylake repo metadata** (description, the key/value metadata shown in the web app) is not copied. Set it with `client.modify_repo` (section 6).
- You can also use `copy_node` within a single repo, e.g. to duplicate a group before experimenting with it.

## 6. Change a repo's description

The description and the metadata fields shown in the Arraylake web app belong to Arraylake, not to Icechunk, and are changed with the Arraylake client:

```python
from arraylake import Client

client = Client()
client.modify_repo(
    "NOAA-CEFI/cefi-nwa-hindcast-monthly",
    description="NOAA CEFI MOM6 Northwest Atlantic hindcast (monthly, r20250715) ...",
)

# key/value metadata can be changed the same way
client.modify_repo("NOAA-CEFI/cefi-nwa-hindcast-monthly", update_metadata={"release": "r20250715"})
client.modify_repo("NOAA-CEFI/cefi-nwa-hindcast-monthly", remove_metadata=["excluded"])
```

This takes effect immediately and does not create an Icechunk commit. Dataset-level attributes (the `cefi_*` attributes Xarray shows in `ds.attrs`) are something else: they are Zarr group attributes and can be changed in a normal commit:

```python
session = repo.writable_session("main")
zarr.open_group(session.store, path="raw/main", mode="r+").update_attributes({"title": "..."})
session.commit("Update title")
```

## Undoing a change

Every change above is one or two commits. To see recent history and roll `main` back to an earlier snapshot:

```python
for snap in list(repo.ancestry(branch="main"))[:10]:
    print(snap.id, snap.written_at, snap.message)

repo.reset_branch("main", "<snapshot id to go back to>")
```

The later snapshots are kept until garbage collection removes them, so for a while you can move forward again by resetting to a later ID.

## About `utilities.py`

[`utilities.py`](utilities.py) was tested with icechunk 2.3.1, zarr 3.4.2, xarray 2026.9.0 and arraylake 1.9.0. `rename_variable` and `copy_node` use two zarr helpers from `zarr.core` (`sync` and `default_buffer_prototype`), because zarr has no public API for editing an array's dimension names.

[`test_utilities.py`](test_utilities.py) checks every function on small local repos shaped like the CEFI ones: it creates virtual, native and inline chunks and verifies that each operation keeps their values and their chunk types. Run it with `uv run test_utilities.py`.
