"""Thu thập ERA5 theo giờ từ Open-Meteo cho ô trung tâm và ô lân cận của từng điểm đo.

Bước 1  probe: hỏi API 1 ngày để biết ô lưới thật của điểm (cell_selection=land) và của từng ô lân cận
        (cell_selection=nearest) → data/reference/grid_plan.csv, đã khử trùng ô.
Bước 2  fetch: mỗi ô lưới duy nhất × mỗi năm = 1 request → data/raw/era5/grid=<id>/year=<y>.json.
        Ô dùng chung giữa nhiều điểm chỉ tải một lần. File đã có thì bỏ qua (chạy lại để resume).

Mỗi lần gọi được ghi vào data/raw/era5/collect_log.csv (thời điểm, url, kết quả kiểm tra).

    python -m src.ingest.collect_era5 --profile pilot
"""
import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from src.common.config import ROOT, load_config, load_points, path
from src.features.spatial_weather import build_grid_plan, grid_id, neighbor_candidates


class QuotaExceeded(RuntimeError):
    pass


def get_json(url, retries=4):
    wait = 60
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=180) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            if e.code == 429 and "Daily" in body:
                raise QuotaExceeded(body)
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                print(f"  HTTP {e.code}, chờ {wait}s", flush=True)
                time.sleep(wait)
                wait *= 2
                continue
            raise RuntimeError(f"HTTP {e.code}: {body}") from e
        except (json.JSONDecodeError, urllib.error.URLError, ConnectionError, TimeoutError) as e:
            # phản hồi rỗng/đứt giữa chừng: thử lại, không ghi file hỏng
            if attempt < retries - 1:
                print(f"  {type(e).__name__}, chờ {wait}s", flush=True)
                time.sleep(wait)
                wait *= 2
                continue
            raise
    raise RuntimeError(url)


def archive_url(cfg, lat, lon, start, end, cell_selection, variables):
    q = {"latitude": lat, "longitude": lon, "start_date": start, "end_date": end,
         "hourly": ",".join(variables), "models": cfg["source"]["model"],
         "timezone": cfg["source"]["timezone"], "cell_selection": cell_selection}
    return cfg["source"]["api"] + "?" + urllib.parse.urlencode(q)


def probe(cfg, lat, lon, cell_selection, cache):
    """Ô lưới API chọn cho tọa độ (lat, lon). Kết quả cache trong raw/_probe."""
    f = cache / f"{lat:.4f}_{lon:.4f}_{cell_selection}.json"
    if f.exists():
        d = json.loads(f.read_text(encoding="utf-8"))
    else:
        day = f"{cfg['years'][0]}-01-01"
        d = get_json(archive_url(cfg, lat, lon, day, day, cell_selection, ["precipitation"]))
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(json.dumps(d), encoding="utf-8")
        time.sleep(2)
    return d["latitude"], d["longitude"]


def make_plan(cfg):
    points = load_points(cfg)
    cache = path(cfg, "raw") / "_probe"
    step, pattern = cfg["source"]["grid_step_deg"], cfg["neighbors"]["pattern"]
    plan, dropped = [], []
    for p in points:
        center = probe(cfg, p["lat"], p["lon"], cfg["source"]["center_cell_selection"], cache)
        nbs = []
        for di, dj, lat, lon in neighbor_candidates(*center, pattern=pattern, step=step):
            glat, glon = probe(cfg, lat, lon, cfg["source"]["neighbor_cell_selection"], cache)
            nbs.append((di, dj, glat, glon))
        rows, drop = build_grid_plan(p["location_id"], center, nbs)
        for r in rows:
            r.update(point_lat=p["lat"], point_lon=p["lon"])
        plan += rows
        dropped += drop
        print(f"{p['location_id']}: ô trung tâm {rows[0]['grid_id']}, {len(rows) - 1} ô lân cận"
              + (f", bỏ {len(drop)}" if drop else ""), flush=True)
    out = path(cfg, "grid_plan")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(plan[0]))
        w.writeheader()
        w.writerows(plan)
    with open(out.with_name("grid_plan_dropped.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["location_id", "grid_id", "di", "dj", "reason"])
        w.writeheader()
        w.writerows(dropped)
    return plan


def validate(cfg, d, gid, year):
    """Kiểm tra từng file ngay khi tải: múi giờ, đơn vị, đúng ô, đủ giờ. Trả về số giá trị null."""
    src = cfg["source"]
    assert d["utc_offset_seconds"] == src["expected_utc_offset_seconds"], d["utc_offset_seconds"]
    for v, unit in src["expected_units"].items():
        assert d["hourly_units"][v] == unit, (v, d["hourly_units"][v])
    assert grid_id(d["latitude"], d["longitude"]) == gid, (gid, d["latitude"], d["longitude"])
    n = len(d["hourly"]["time"])
    leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    assert n == (8784 if leap else 8760), n
    return {v: sum(x is None for x in d["hourly"][v]) for v in src["hourly"]}


def fetch_all(cfg, plan):
    grids = {}
    for r in plan:
        grids.setdefault(r["grid_id"], (r["grid_lat"], r["grid_lon"]))
    raw, log_path = path(cfg, "raw"), path(cfg, "raw") / "collect_log.csv"
    new_log = not log_path.exists()
    log = open(log_path, "a", newline="", encoding="utf-8")
    w = csv.writer(log)
    if new_log:
        w.writerow(["fetched_at_utc", "grid_id", "year", "status", "rows", "null_counts", "url"])
    y0, y1 = cfg["years"]
    todo = [(g, y) for g in sorted(grids) for y in range(y0, y1 + 1)
            if not (raw / f"grid={g}" / f"year={y}.json").exists()]
    print(f"{len(grids)} ô lưới × {y1 - y0 + 1} năm, còn {len(todo)} request", flush=True)
    try:
        for i, (g, y) in enumerate(todo, 1):
            lat, lon = grids[g]
            url = archive_url(cfg, lat, lon, f"{y}-01-01", f"{y}-12-31", "nearest", cfg["source"]["hourly"])
            d = get_json(url)
            nulls = validate(cfg, d, g, y)
            d["_provenance"] = {"url": url, "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                                "grid_id": g, "year": y}
            dest = raw / f"grid={g}" / f"year={y}.json"
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(".tmp")
            tmp.write_text(json.dumps(d), encoding="utf-8")
            tmp.replace(dest)  # file chỉ xuất hiện khi đã ghi xong
            w.writerow([d["_provenance"]["fetched_at_utc"], g, y, "ok", len(d["hourly"]["time"]),
                        json.dumps(nulls), url])
            log.flush()
            print(f"[{i}/{len(todo)}] {g} {y} ok" + (f" null={nulls}" if any(nulls.values()) else ""), flush=True)
            time.sleep(cfg["source"]["pause_seconds"])
    except QuotaExceeded as e:
        print(f"Hết lượt trong ngày, chạy lại sau để tiếp tục: {e}", flush=True)
        return 2
    finally:
        log.close()
    return 0


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="pilot")
    ap.add_argument("--plan-only", action="store_true")
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    plan = make_plan(cfg)
    if not args.plan_only:
        sys.exit(fetch_all(cfg, plan))


if __name__ == "__main__":
    main()
