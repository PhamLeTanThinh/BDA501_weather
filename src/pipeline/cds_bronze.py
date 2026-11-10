"""Raw CDS (zip NetCDF) → bronze/era5_cds_hourly (Parquet), chỉ các ô đất liền trong cells_land.csv.

Spark không đọc được NetCDF nên bước này chạy bằng Python (netCDF4 + pyarrow), một file Parquet cho mỗi
zip raw. Bronze giữ nguyên giá trị và đơn vị gốc của CDS (K, Pa, m, 0–1, m/s…) và thời gian UTC;
đổi đơn vị, đổi giờ Việt Nam và kiểm tra hợp lệ để cho silver. Mỗi dòng = (ô, giờ UTC).

  * Biến tức thời (t2m, msl, …) là giá trị tại valid_time.
  * tp và vimd là lượng tích lũy trong giờ (valid_time - 1h, valid_time], CDS đã tách sẵn theo giờ.

File đã có thì bỏ qua (chạy lại để tiếp tục); --force để ghi lại.

    python -m src.pipeline.cds_bronze --profile official
"""
import argparse
import csv
import io
import time
import zipfile
from datetime import datetime, timezone

import netCDF4
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from src.common.config import load_config, path

INSTANT = ["t2m", "d2m", "msl", "tcc", "u10", "v10", "tcwv", "viwve", "viwvn", "cape", "kx"]
ACCUM = ["tp", "vimd"]
VARS = ACCUM + INSTANT


def load_cells(cells_csv):
    with open(cells_csv, encoding="utf-8") as f:
        return [(r["grid_id"], float(r["grid_lat"]), float(r["grid_lon"])) for r in csv.DictReader(f)]


def read_zip(zpath):
    """{tên biến: mảng (giờ, lat, lon)}, valid_time (epoch giây UTC), lat, lon từ một zip CDS."""
    out, times, lat, lon = {}, None, None, None
    with zipfile.ZipFile(zpath) as z:
        for name in z.namelist():
            with netCDF4.Dataset("mem", memory=z.read(name)) as d:
                t = d.variables["valid_time"][:].astype("int64")
                if times is None:
                    times, lat, lon = t, d.variables["latitude"][:], d.variables["longitude"][:]
                elif not np.array_equal(times, t):
                    raise ValueError(f"{zpath.name}: {name} lệch trục thời gian")
                for v in d.variables:
                    if d.variables[v].ndim == 3:
                        out[v] = np.ma.filled(d.variables[v][:].astype("float32"), np.nan)
    missing = set(VARS) - set(out)
    if missing:
        raise ValueError(f"{zpath.name}: thiếu biến {sorted(missing)}")
    return out, times, np.asarray(lat), np.asarray(lon)


def to_table(zpath, cells, ingested_at):
    data, times, lat, lon = read_zip(zpath)
    ilat = {round(float(x), 4): i for i, x in enumerate(lat)}
    ilon = {round(float(x), 4): i for i, x in enumerate(lon)}
    li = np.array([ilat[la] for _, la, _ in cells])
    lj = np.array([ilon[lo] for _, _, lo in cells])
    nt, nc = len(times), len(cells)
    cols = {"grid_id": pa.array(np.repeat([c[0] for c in cells], nt)),
            # thứ tự dòng: theo ô, trong ô theo giờ
            "valid_time": pa.array(np.tile(times, nc) * 1_000_000, type=pa.timestamp("us", tz="UTC"))}
    for v in VARS:
        cols[v] = pa.array(data[v][:, li, lj].T.ravel())  # (giờ, ô) → (ô, giờ)
    cols["source_file"] = pa.array([zpath.name] * (nt * nc))
    cols["ingested_at"] = pa.array([ingested_at] * (nt * nc), type=pa.timestamp("us", tz="UTC"))
    return pa.table(cols), times


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="official")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    raw, out = path(cfg, "raw_cds"), path(cfg, "lake") / "bronze" / "era5_cds_hourly"
    cells = load_cells(path(cfg, "cells"))
    zips = sorted(raw.glob("year=*/era5_vn_*.zip"))
    print(f"{len(zips)} file raw, {len(cells)} ô → {out}", flush=True)
    t_all = time.time()
    for z in zips:
        year = z.parent.name.split("=")[1]  # năm UTC của file raw
        dest = out / f"year={year}" / f"part-{z.stem}.parquet"
        if dest.exists() and not args.force:
            continue
        t0 = time.time()
        table, times = to_table(z, cells, datetime.now(timezone.utc))
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".tmp")
        pq.write_table(table, tmp, compression="zstd", row_group_size=1_000_000)
        tmp.replace(dest)
        print(f"{z.name}: {table.num_rows:,} dòng ({len(times)} giờ × {len(cells)} ô), "
              f"{dest.stat().st_size / 1e6:.0f} MB, {time.time() - t0:.0f} s", flush=True)
    print(f"Xong bronze trong {time.time() - t_all:.0f} s", flush=True)


if __name__ == "__main__":
    main()
