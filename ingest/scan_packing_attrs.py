#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["coiled", "distributed", "obstore>=0.5", "h5py", "numpy"]
# ///
"""Header-only scan of every ingested source NetCDF4 file: CF packing attributes (scale_factor / add_offset and friends),
dtypes, HDF5 scale-offset filter. No data is read. Writes packing_scan.json next to this script."""
from __future__ import annotations

import json
from pathlib import Path

BUCKET = "noaa-oar-cefi-regional-mom6-pds"
REGION = "us-east-1"
WORKSPACE = "earthmover-devs"
ENV_NAME = "cefi-ingest-2"
N_WORKERS = 16
PRODUCTS = {  # product -> (prefix, release)
    "nwa-decadal-yearly": ("northwest_atlantic/full_domain/decadal_forecast/yearly", "r20250819"),
    "nwa-decadal-monthly": ("northwest_atlantic/full_domain/decadal_forecast/monthly", "r20250925"),
    "nwa-seasonal-forecast": ("northwest_atlantic/full_domain/seasonal_forecast/monthly", "r20250710"),
    "nwa-seasonal-reforecast": ("northwest_atlantic/full_domain/seasonal_reforecast/monthly", "r20250413"),
    "nwa-hindcast-daily": ("northwest_atlantic/full_domain/hindcast/daily", "r20250715"),
    "nwa-hindcast-monthly": ("northwest_atlantic/full_domain/hindcast/monthly", "r20250715"),
    "nwa-multidecadal": ("northwest_atlantic/full_domain/multi_decadal_outlook/yearly", "r20260331"),
    "pci-hindcast-daily": ("pacific_islands/full_domain/hindcast/daily", "r20260427"),
    "pci-hindcast-monthly": ("pacific_islands/full_domain/hindcast/monthly", "r20260427"),
    "nep-hindcast-daily": ("northeast_pacific/full_domain/hindcast/daily", "r20260701"),
    "nep-hindcast-monthly": ("northeast_pacific/full_domain/hindcast/monthly", "r20260701"),
}
KEYS = ("scale_factor", "add_offset", "scale", "offset", "valid_range", "valid_min", "valid_max", "packing", "units_scale")


def store():
    import obstore as obs
    return obs.store.S3Store(BUCKET, region=REGION, skip_signature=True)


def list_files() -> list[tuple[str, str, str]]:
    out = []
    s = store()
    for product, (prefix, release) in PRODUCTS.items():
        for grid in ("raw", "regrid"):
            for b in s.list(prefix=f"{prefix}/{grid}/{release}/"):
                for o in b:
                    if o["path"].endswith(".nc"):
                        out.append((product, grid, o["path"]))
    return out


def scan_file(product: str, grid: str, path: str) -> dict:
    import io as _io

    import h5py
    import numpy as np
    import obstore as obs

    class R(_io.RawIOBase):
        def __init__(self, r): self.r = r
        def readable(self): return True
        def seekable(self): return True
        def tell(self): return self.r.tell()
        def seek(self, o, w=0): return self.r.seek(o, w)
        def readinto(self, b):
            d = bytes(self.r.read(len(b)))
            b[:len(d)] = d
            return len(d)

    def jv(v):
        v = np.asarray(v)
        return v.tolist() if v.size <= 4 else f"<array {v.shape}>"

    rec = {"product": product, "grid": grid, "path": path}
    try:
        f = h5py.File(R(obs.open_reader(store(), path, buffer_size=256 * 1024)), "r")
        dsets = {}

        def visit(name, obj):
            if isinstance(obj, h5py.Dataset):
                attrs = {k: jv(obj.attrs[k]) for k in KEYS if k in obj.attrs}
                dsets[name] = {"dtype": str(obj.dtype), "ndim": obj.ndim, "scaleoffset": obj.scaleoffset is not None,
                               "attrs": attrs}

        f.visititems(visit)
        rec["datasets"] = dsets
        rec["group_attrs"] = {k: jv(f.attrs[k]) for k in KEYS if k in f.attrs}
    except Exception as exc:  # noqa: BLE001
        rec["error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    return rec


def main() -> int:
    import coiled
    from distributed import Client, as_completed

    files = list_files()
    print(f"{len(files)} files under {len(PRODUCTS)} products", flush=True)
    cluster = coiled.Cluster(
        name="cefi-packing-scan", workspace=WORKSPACE, software=ENV_NAME, n_workers=N_WORKERS, region=REGION,
        spot_policy="spot_with_fallback", worker_cpu=2, worker_memory="8GiB", worker_options={"nthreads": 1},
        scheduler_vm_types=["m6i.xlarge"], idle_timeout="15 minutes", shutdown_on_close=True)
    results = []
    try:
        client = Client(cluster)
        futs = client.map(lambda a: scan_file(*a), files, retries=2)
        for i, fu in enumerate(as_completed(futs), 1):
            try:
                results.append(fu.result())
            except Exception as exc:  # noqa: BLE001
                results.append({"error": f"task failed: {exc}"})
            if i % 500 == 0:
                print(f"  {i}/{len(files)}", flush=True)
                Path(__file__).with_name("packing_scan.json").write_text(json.dumps(results))
    finally:
        Path(__file__).with_name("packing_scan.json").write_text(json.dumps(results))
        cluster.close()
    print(f"done: {len(results)} results, {sum('error' in r for r in results)} errors", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
