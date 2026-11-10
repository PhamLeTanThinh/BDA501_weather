"""Tải ERA5 gốc từ Copernicus Climate Data Store một lần cho cả khung Việt Nam (không gọi API theo từng điểm).

Mỗi request = tối đa N tháng liên tiếp trong cùng một năm (mặc định 1) × 13 biến × 24 giờ × khung lat/lon.
CDS giới hạn kích thước request (cả năm bị từ chối); nếu khối N tháng bị báo "too large" thì tự tách thành
từng tháng. CDS chỉ cho xếp hàng rất ít request mỗi tài khoản, nên chạy tuần tự.

File: <out>/year=YYYY/era5_vn_YYYY-MM.zip (1 tháng) hoặc era5_vn_YYYY-MM_MM.zip (khối nhiều tháng);
mỗi zip chứa 2 NetCDF (biến tức thời + lượng mưa tích lũy). Tháng đã có thì bỏ qua → chạy lại để tải tiếp.
Kèm một request nhỏ cho ngày cuối năm trước (vd. 31/12/2014): 7 giờ đầu ngày 01/01 giờ Việt Nam nằm trong ngày đó theo UTC.

Cần: tài khoản CDS, đã chấp nhận licence ERA5, file ~/.cdsapirc chứa url + key.

    python -m src.ingest.download_era5_cds --years 2015 2025 --out D:/BDA501_data/raw/era5_cds
"""
import argparse
import csv
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import cdsapi

DATASET = "reanalysis-era5-single-levels"
VARIABLES = [
    # bề mặt (7 biến, giống pilot)
    "total_precipitation", "2m_temperature", "2m_dewpoint_temperature", "mean_sea_level_pressure",
    "total_cloud_cover", "10m_u_component_of_wind", "10m_v_component_of_wind",
    # cột khí quyển (6 biến): ẩm có sẵn, ẩm vận chuyển tới, ẩm hội tụ, độ bất ổn định
    "total_column_water_vapour", "vertical_integral_of_eastward_water_vapour_flux",
    "vertical_integral_of_northward_water_vapour_flux", "vertically_integrated_moisture_divergence",
    "convective_available_potential_energy", "k_index",
]
AREA_VN = [23.5, 102.0, 8.5, 110.0]  # North, West, South, East: đất liền Việt Nam + một dải biển ven bờ


def request(year, area, months, day=None):
    return {"product_type": ["reanalysis"], "variable": VARIABLES, "year": [str(year)],
            "month": [f"{m:02d}" for m in months],
            "day": [day] if day else [f"{d:02d}" for d in range(1, 32)],
            "time": [f"{h:02d}:00" for h in range(24)], "area": area,
            "data_format": "netcdf", "download_format": "unarchived"}


def months_on_disk(folder, year):
    """Các tháng của năm đã có file (đơn tháng hoặc khối nhiều tháng)."""
    have = set()
    for f in folder.glob(f"era5_vn_{year}-*.zip"):
        m = re.fullmatch(rf"era5_vn_{year}-(\d\d)(?:_(\d\d))?\.zip", f.name)
        if m:
            a, b = int(m.group(1)), int(m.group(2) or m.group(1))
            have.update(range(a, b + 1))
    return have


def chunks(missing, size):
    """Gom các tháng còn thiếu thành các khối liên tiếp, mỗi khối tối đa `size` tháng."""
    out, cur = [], []
    for m in missing:
        if cur and (m != cur[-1] + 1 or len(cur) == size):
            out.append(cur)
            cur = []
        cur.append(m)
    return out + ([cur] if cur else [])


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", nargs=2, type=int, default=[2015, 2025], help="năm đầu và năm cuối (gồm cả hai)")
    ap.add_argument("--out", default="D:/BDA501_data/raw/era5_cds")
    ap.add_argument("--area", nargs=4, type=float, default=AREA_VN, metavar=("N", "W", "S", "E"))
    ap.add_argument("--months-per-request", type=int, default=1)
    ap.add_argument("--log", default="download_log.csv", help="tên file log trong <out>")
    ap.add_argument("--no-lead-day", action="store_true",
                    help="bỏ ngày 31/12 năm trước (khi năm trước đã được tải đủ ở lượt khác)")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    log = out / args.log
    new = not log.exists()
    client = cdsapi.Client()

    def fetch(year, months, dest, day=None):
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".part")
        t0 = time.time()
        print(f"{dest.name}: gửi request (chờ trong hàng đợi CDS)…", flush=True)
        try:
            client.retrieve(DATASET, request(year, args.area, months, day), str(tmp))
            tmp.replace(dest)  # file chỉ xuất hiện khi đã tải xong
            status = "ok"
        except Exception as e:  # noqa: BLE001 — ghi lỗi rồi đi tiếp; chạy lại sẽ thử lại
            status = f"error: {e}"[:300]
        size = dest.stat().st_size if dest.exists() else 0
        w.writerow([datetime.now(timezone.utc).isoformat(), year, f"{months[0]:02d}-{months[-1]:02d}", status, size,
                    round(time.time() - t0), dest])
        f.flush()
        print(f"{dest.name}: {status}, {size / 1e6:.1f} MB, {time.time() - t0:.0f} s", flush=True)
        return status

    with open(log, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["finished_utc", "year", "months", "status", "bytes", "seconds", "file"])
        lead = args.years[0] - 1
        lead_file = out / f"year={lead}" / f"era5_vn_{lead}-12-31.zip"
        if not args.no_lead_day and not lead_file.exists():
            fetch(lead, [12], lead_file, day="31")
        for year in range(args.years[0], args.years[1] + 1):
            folder = out / f"year={year}"
            missing = [m for m in range(1, 13) if m not in months_on_disk(folder, year)]
            for block in chunks(missing, args.months_per_request):
                name = f"era5_vn_{year}-{block[0]:02d}" + (f"_{block[-1]:02d}" if len(block) > 1 else "") + ".zip"
                status = fetch(year, block, folder / name)
                if "too large" in status and len(block) > 1:  # khối quá lớn → tách từng tháng
                    for m in block:
                        fetch(year, [m], folder / f"era5_vn_{year}-{m:02d}.zip")
    print("Xong vòng tải; tháng nào lỗi sẽ được thử lại ở lần chạy sau.", flush=True)


if __name__ == "__main__":
    main()
