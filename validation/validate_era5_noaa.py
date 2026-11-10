"""So sánh lượng mưa ngày ERA5 (Open-Meteo) với trạm đo NOAA GHCN-Daily ở Việt Nam.

Trả lời câu hỏi: dữ liệu ERA5 dùng làm nguồn chính có đúng với thực tế không?

Cách chạy:
    python validate_era5_noaa.py                 # 2015-2024, mô hình era5
    python validate_era5_noaa.py --model ecmwf_ifs --start 2017-01-01

Kết quả: results_<model>.csv (theo trạm) và report_<model>.md (tóm tắt).
Dữ liệu tải về được cache trong ../data/validation/, chạy lại không tải trùng.
"""
import argparse
import csv
import json
import math
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
CACHE = HERE.parent / "data" / "validation"

# Trạm GHCN-Daily ở Việt Nam còn số liệu đến 2024-2025 (tên suy ra từ tọa độ, cần kiểm tra lại).
STATIONS = [
    ("VMM00048806", "Sơn La", 21.333, 103.900),
    ("VMM00048808", "Cao Bằng", 22.667, 106.250),
    ("VMM00048825", "Hà Nội (Hà Đông)", 20.967, 105.767),
    ("VMM00048826", "Hải Phòng (Phù Liễn)", 20.800, 106.633),
    ("VMM00048830", "Lạng Sơn", 21.833, 106.767),
    ("VMM00048840", "Thanh Hóa", 19.750, 105.783),
    ("VMM00048845", "Vinh", 18.737, 105.671),
    ("VMM00048848", "Đồng Hới", 17.483, 106.600),
    ("VMM00048855", "Đà Nẵng", 16.044, 108.199),
    ("VMW00041011", "Quy Nhơn", 13.767, 109.217),
    ("VMW00041010", "Nha Trang", 12.233, 109.200),
    ("VMM00048887", "Phan Thiết", 10.933, 108.100),
    ("VMM00048914", "Cà Mau", 9.183, 105.150),
    ("VMM00048917", "Phú Quốc", 10.217, 103.967),
]

HEAVY_MM = 50.0  # QĐ 18/2021/QĐ-TTg, Điều 5, khoản 17
NOAA_URL = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/{sid}.csv"
ERA_URL = ("https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}"
           "&start_date={start}&end_date={end}&hourly=precipitation&models={model}"
           "&timezone=GMT&cell_selection=land")


def fetch(url, dest, pause=0.0, retries=5):
    """Tải url về dest nếu chưa có; thử lại khi gặp 429/5xx với thời gian chờ tăng dần."""
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    wait = 30
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                data = r.read()
            dest.write_bytes(data)
            time.sleep(pause)
            return dest
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and attempt < retries - 1:
                print(f"  HTTP {e.code}, chờ {wait}s rồi thử lại")
                time.sleep(wait)
                wait *= 2
                continue
            raise
    raise RuntimeError(f"Không tải được {url}")


def load_noaa(path, start, end):
    """Mưa ngày của trạm (mm). Bỏ giá trị trống và giá trị bị gắn cờ chất lượng (Q-flag)."""
    out, flagged = {}, 0
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            d = r["DATE"]
            if not (start <= d <= end) or not r.get("PRCP", "").strip():
                continue
            attrs = (r.get("PRCP_ATTRIBUTES") or "").split(",")
            if len(attrs) > 1 and attrs[1].strip():
                flagged += 1
                continue
            out[d] = int(r["PRCP"]) / 10.0  # GHCN lưu theo 1/10 mm
    return out, flagged


def load_era(path):
    """Gộp mưa giờ ERA5 (giờ UTC) thành mưa ngày theo ngày UTC và theo ngày giờ Việt Nam."""
    d = json.loads(path.read_text(encoding="utf-8"))
    if "hourly" not in d:
        raise RuntimeError(f"Open-Meteo trả lỗi: {d}")
    assert d["utc_offset_seconds"] == 0 and d["hourly_units"]["precipitation"] == "mm"
    utc, vn, nulls = {}, {}, 0
    for t, x in zip(d["hourly"]["time"], d["hourly"]["precipitation"]):
        if x is None:
            nulls += 1
            continue
        ts = datetime.fromisoformat(t)
        utc[ts.date().isoformat()] = utc.get(ts.date().isoformat(), 0.0) + x
        dv = (ts + timedelta(hours=7)).date().isoformat()
        vn[dv] = vn.get(dv, 0.0) + x
    return utc, vn, nulls, (d["latitude"], d["longitude"], d.get("elevation"))


def load_era_cds(lat, lon, silver_dir):
    """Như load_era nhưng đọc ERA5 tải từ CDS (lake silver/grid_weather, mm/giờ) tại ô đất liền gần trạm nhất."""
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    sys.path.insert(0, str(HERE.parent))
    from src.features.spatial_weather import grid_id
    with open(HERE.parent / "data" / "reference" / "cells_land.csv", encoding="utf-8") as f:
        cells = [(float(r["grid_lat"]), float(r["grid_lon"])) for r in csv.DictReader(f)]
    glat, glon = min(cells, key=lambda c: (c[0] - lat) ** 2 + ((c[1] - lon) * math.cos(math.radians(lat))) ** 2)
    tbl = ds.dataset(silver_dir, format="parquet", partitioning="hive").to_table(
        columns=["ts", "precipitation"], filter=pc.field("grid_id") == grid_id(glat, glon))
    utc, vn, nulls = {}, {}, 0
    for ts, x in zip(tbl["ts"].to_pylist(), tbl["precipitation"].to_pylist()):
        if x is None:
            nulls += 1
            continue
        u = ts.astimezone(timezone.utc).replace(tzinfo=None) if ts.tzinfo else ts  # Spark lưu instant UTC
        utc[u.date().isoformat()] = utc.get(u.date().isoformat(), 0.0) + x
        dv = (u + timedelta(hours=7)).date().isoformat()
        vn[dv] = vn.get(dv, 0.0) + x
    return utc, vn, nulls, (glat, glon, None)


def shift(day, n):
    return (date.fromisoformat(day) + timedelta(days=n)).isoformat()


def corr(a, b):
    if len(a) < 3:
        return float("nan")
    ma, mb = sum(a) / len(a), sum(b) / len(b)
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((y - mb) ** 2 for y in b)
    if va == 0 or vb == 0:
        return float("nan")
    return sum((x - ma) * (y - mb) for x, y in zip(a, b)) / math.sqrt(va * vb)


def quantile(values, q):
    v = sorted(values)
    if not v:
        return float("nan")
    i = min(len(v) - 1, max(0, int(round(q * (len(v) - 1)))))
    return v[i]


def compare(st, era):
    """Các chỉ số so sánh trên những ngày trạm có số liệu."""
    days = [d for d in st if d in era]
    a = [st[d] for d in days]
    b = [era[d] for d in days]
    heavy = [d for d in days if st[d] > HEAVY_MM]
    near = lambda d: max(era.get(shift(d, k), 0.0) for k in (-1, 0, 1))
    hits = sum(era[d] > HEAVY_MM for d in heavy)
    hits_tol = sum(near(d) > HEAVY_MM for d in heavy)
    false_alarm = sum(1 for d in days if era[d] > HEAVY_MM and st[d] <= HEAVY_MM)
    lags = {}
    for k in (-1, 0, 1):
        pairs = [(st[d], era[shift(d, k)]) for d in days if shift(d, k) in era]
        lags[k] = corr([p[0] for p in pairs], [p[1] for p in pairs])
    # Ngưỡng ERA5 để tần suất "mưa lớn" bằng tần suất ở trạm (quantile mapping)
    freq = len(heavy) / len(days) if days else 0.0
    thr = quantile(b, 1 - freq) if freq > 0 else float("nan")
    return {
        "n_days": len(days),
        "corr": corr(a, b),
        "mae": sum(abs(x - y) for x, y in zip(a, b)) / len(days) if days else float("nan"),
        "ratio_total": (sum(b) / sum(a)) if sum(a) else float("nan"),
        "heavy_station": len(heavy),
        "hit": hits,
        "hit_pm1": hits_tol,
        "false_alarm": false_alarm,
        "max_station": max(a) if a else float("nan"),
        "max_era": max(b) if b else float("nan"),
        "corr_lag_-1": lags[-1],
        "corr_lag_0": lags[0],
        "corr_lag_+1": lags[1],
        "era_threshold_matched": thr,
    }


def main():
    sys.stdout.reconfigure(encoding="utf-8")  # console Windows mặc định không in được tiếng Việt
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end", default="2024-12-31")
    ap.add_argument("--model", default="era5")
    ap.add_argument("--source", choices=["open-meteo", "cds"], default="open-meteo",
                    help="cds: đọc ERA5 đã tải từ CDS trong lake (silver), kết quả ghi results_cds.csv")
    ap.add_argument("--silver", default="D:/BDA501_data/lake/silver/grid_weather")
    ap.add_argument("--pause", type=float, default=25.0, help="giây nghỉ sau mỗi lần gọi Open-Meteo")
    args = ap.parse_args()

    n_total = (date.fromisoformat(args.end) - date.fromisoformat(args.start)).days + 1
    rows = []
    for sid, name, lat, lon in STATIONS:
        print(f"[{sid}] {name}")
        noaa_path = fetch(NOAA_URL.format(sid=sid), CACHE / "noaa" / f"{sid}.csv")
        st, flagged = load_noaa(noaa_path, args.start, args.end)
        if args.source == "cds":
            utc, vn, nulls, grid = load_era_cds(lat, lon, args.silver)
        else:
            era_path = fetch(ERA_URL.format(lat=lat, lon=lon, start=args.start, end=args.end, model=args.model),
                             CACHE / args.model / f"{sid}_{args.start}_{args.end}.json", pause=args.pause)
            utc, vn, nulls, grid = load_era(era_path)
        if nulls:
            print(f"  cảnh báo: {nulls} giờ ERA5 bị null (không coi là 0)")
        for tz, era in (("UTC", utc), ("VN", vn)):
            m = compare(st, era)
            rows.append({"station": sid, "name": name, "day": tz,
                         "coverage_pct": 100 * len(st) / n_total, "qflag_removed": flagged,
                         "grid": f"{grid[0]},{grid[1]}", **m})

    tag = "cds" if args.source == "cds" else args.model
    out_csv = HERE / f"results_{tag}.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    def fmt(x, nd=2):
        return "–" if isinstance(x, float) and math.isnan(x) else (f"{x:.{nd}f}" if isinstance(x, float) else str(x))

    lines = [f"# ERA5 ({tag}) so với trạm NOAA, {args.start} → {args.end}", "",
             "Mưa lớn = trên 50 mm/ngày. Chỉ so trên những ngày trạm có số liệu.", "",
             "| Trạm | Ngày theo | Phủ % | Tương quan (lệch -1/0/+1 ngày) | Tổng ERA5/trạm | Ngày mưa lớn trạm | ERA5 bắt đúng | Bắt ±1 ngày | Báo nhầm | Max trạm / ERA5 | Ngưỡng ERA5 tương đương |",
             "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        lines.append(f"| {r['name']} | {r['day']} | {fmt(r['coverage_pct'], 0)} | "
                     f"{fmt(r['corr_lag_-1'])} / **{fmt(r['corr_lag_0'])}** / {fmt(r['corr_lag_+1'])} | "
                     f"{fmt(r['ratio_total'])} | {r['heavy_station']} | {r['hit']} | {r['hit_pm1']} | "
                     f"{r['false_alarm']} | {fmt(r['max_station'], 0)} / {fmt(r['max_era'], 0)} | "
                     f"{fmt(r['era_threshold_matched'], 1)} mm |")
    for tz in ("UTC", "VN"):
        sub = [r for r in rows if r["day"] == tz and r["n_days"] > 0]
        hv = sum(r["heavy_station"] for r in sub)
        lines += ["", f"**Tổng hợp, ngày theo {tz}:** {hv} ngày mưa lớn ở trạm; ERA5 bắt đúng "
                      f"{sum(r['hit'] for r in sub)} ({100 * sum(r['hit'] for r in sub) / max(hv, 1):.0f}%), "
                      f"bắt ±1 ngày {sum(r['hit_pm1'] for r in sub)} ({100 * sum(r['hit_pm1'] for r in sub) / max(hv, 1):.0f}%), "
                      f"báo nhầm {sum(r['false_alarm'] for r in sub)}; tương quan trung vị "
                      f"{fmt(sorted(r['corr_lag_0'] for r in sub)[len(sub) // 2])}; "
                      f"tỷ lệ tổng mưa ERA5/trạm trung vị {fmt(sorted(r['ratio_total'] for r in sub)[len(sub) // 2])}."]
    out_md = HERE / f"report_{tag}.md"
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nĐã ghi {out_csv.name} và {out_md.name}")


if __name__ == "__main__":
    main()
