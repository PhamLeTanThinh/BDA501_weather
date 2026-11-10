"""Tạo bảng tra cứu địa lý cho từng điểm đo (pilot) hoặc từng ô đất liền (official) → paths.geography.

  * elevation_m, terrain_slope_deg: 9 điểm DEM quanh tọa độ điểm đo (Open-Meteo Elevation API,
    Copernicus GLO-90, tối đa 100 tọa độ/request).
  * distance_to_coast_km: Natural Earth 1:10m coastline (public domain), cắt theo bbox Việt Nam.

Không có mạng / thiếu file thì dừng với thông báo rõ ràng, không ghi giá trị ước đoán.

    python -m src.ingest.collect_geography --profile pilot
"""
import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from src.common.config import load_config, load_points, path
from src.features.geospatial import (coastline_segments, distance_to_coast_km, horn_slope_deg,
                                     slope_stencil)


def coastline(cfg):
    dest = path(cfg, "coastline")
    if not dest.exists():
        url = cfg["geography"]["coastline_url"]
        print(f"Tải coastline: {url}")
        with urllib.request.urlopen(url, timeout=300) as r:
            full = json.loads(r.read())
        lon0, lat0, lon1, lat1 = cfg["geography"]["coastline_bbox"]
        keep = []
        for f in full["features"]:
            g = f["geometry"]
            lines = g["coordinates"] if g["type"] == "MultiLineString" else [g["coordinates"]]
            if any(lon0 <= x <= lon1 and lat0 <= y <= lat1 for line in lines for x, y in line):
                keep.append(f)
        dest.write_text(json.dumps({"type": "FeatureCollection", "source": url, "features": keep}),
                        encoding="utf-8")
    return json.loads(dest.read_text(encoding="utf-8"))


def elevations(cfg, coords, pause=1.5, retries=15):
    """Độ cao DEM theo lô 100 tọa độ. Lô đã tải được lưu cache (chạy lại không gọi lại API)."""
    cache_path = path(cfg, "geography").with_suffix(".elev_cache.json")
    cache = json.loads(cache_path.read_text(encoding="utf-8")) if cache_path.exists() else {}
    out = []
    for i in range(0, len(coords), 100):
        chunk = coords[i:i + 100]
        ck = ";".join(f"{a:.6f},{b:.6f}" for a, b in chunk)
        if ck in cache:
            out += cache[ck]
            continue
        api = cfg["geography"]["elevation_api"]
        topo = "opentopodata" in api  # OpenTopoData: locations=lat,lon|lat,lon; null trên biển → 0 m
        q = (urllib.parse.urlencode({"locations": "|".join(f"{a:.6f},{b:.6f}" for a, b in chunk)}) if topo else
             urllib.parse.urlencode({"latitude": ",".join(f"{a:.6f}" for a, _ in chunk),
                                     "longitude": ",".join(f"{b:.6f}" for _, b in chunk)}))
        for k in range(retries):  # API miễn phí giới hạn theo phút: 429 thì chờ rồi thử lại
            try:
                with urllib.request.urlopen(api + "?" + q, timeout=120) as r:
                    body = json.loads(r.read())
                cache[ck] = ([x["elevation"] if x["elevation"] is not None else 0.0 for x in body["results"]] if topo
                             else body["elevation"])
                out += cache[ck]
                cache_path.write_text(json.dumps(cache), encoding="utf-8")
                break
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
                code = getattr(e, "code", None)
                if (code is not None and code not in (429, 500, 502, 503, 504)) or k == retries - 1:
                    raise
                print(f"lỗi {code or e} ở lượt {i // 100 + 1}/{-(-len(coords) // 100)}, chờ 60 s", flush=True)
                time.sleep(60)
        time.sleep(pause)
    if any(v is None for v in out):
        raise RuntimeError("Elevation API trả null")
    return out


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="pilot")
    cfg = load_config(profile=ap.parse_args().profile)
    points = load_points(cfg)
    if "cells" in cfg["paths"]:  # bản chính thức: mỗi ô ERA5 đất liền là một "điểm", lấy tại tâm ô
        with open(path(cfg, "cells"), encoding="utf-8") as f:
            points = [{"location_id": r["grid_id"], "lat": float(r["grid_lat"]), "lon": float(r["grid_lon"])}
                      for r in csv.DictReader(f)]
    spacing = cfg["geography"]["slope_spacing_m"]
    segs = coastline_segments(coastline(cfg), cfg["geography"]["coastline_bbox"])
    stencils = [slope_stencil(p["lat"], p["lon"], spacing) for p in points]
    z = elevations(cfg, [c for s in stencils for c in s])
    rows = []
    for k, p in enumerate(points):
        zz = z[9 * k: 9 * k + 9]
        rows.append({"location_id": p["location_id"], "elevation_m": zz[4],
                     "distance_to_coast_km": round(distance_to_coast_km(p["lat"], p["lon"], segs), 3),
                     "terrain_slope_deg": round(horn_slope_deg(zz, spacing), 3),
                     "source": f"DEM={cfg['geography'].get('dem_name', 'Copernicus GLO-90 via Open-Meteo')}; slope Horn 3x3 @{spacing}m; "
                               "coast=Natural Earth 10m v5.1.2"})
    print(f"{len(rows)} dòng → {path(cfg, 'geography')}")
    with open(path(cfg, "geography"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
