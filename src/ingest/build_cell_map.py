"""Bảng ô lưới ERA5 thuộc đất liền Việt Nam → data/reference/cells_land.csv.

Mỗi ô 0,25° được lấy mẫu 5×5 điểm. land_frac là tỷ lệ điểm mẫu nằm trong lãnh thổ Việt Nam (ranh giới
tỉnh geoBoundaries ADM1, 63 tỉnh cũ). Ô được giữ khi land_frac >= MIN_LAND_FRAC, hoặc khi ô chứa một
trong 63 điểm của points.csv (để ô của các điểm cũ luôn có mặt). Tỉnh của ô = tỉnh chiếm nhiều điểm mẫu
nhất; tỉnh mới và vùng lấy theo points.csv.

    python -m src.ingest.build_cell_map
"""
import csv
import json
from collections import Counter

import numpy as np
from matplotlib.path import Path as MplPath

from src.common.config import ROOT
from src.features.spatial_weather import grid_id

REF = ROOT / "data" / "reference"
BOUNDARIES = REF / "vn_adm1_geoboundaries.geojson"
OUT = REF / "cells_land.csv"
AREA = (23.5, 102.0, 8.5, 110.0)  # N, W, S, E: đúng khung đã tải từ CDS
STEP = 0.25
SAMPLES = 5
MIN_LAND_FRAC = 0.25
# tên trong geoBoundaries khác points.csv; Côn Đảo thuộc tỉnh Bà Rịa–Vũng Tàu cũ
RENAME = {"Ho Chi Minh": "TP. Hồ Chí Minh", "Côn Đảo": "Bà Rịa–Vũng Tàu"}


def load_provinces(path=BOUNDARIES):
    """[(tên tỉnh cũ, [(Path vành ngoài, [Path lỗ])])] từ GeoJSON Polygon / MultiPolygon."""
    data = json.loads(path.read_text(encoding="utf-8"))
    out = []
    for f in data["features"]:
        name = f["properties"]["shapeName"].strip()
        name = RENAME.get(name, name)
        g = f["geometry"]
        polys = g["coordinates"] if g["type"] == "MultiPolygon" else [g["coordinates"]]
        out.append((name, [(MplPath(np.asarray(p[0])), [MplPath(np.asarray(h)) for h in p[1:]]) for p in polys]))
    return out


def province_of(provinces, lon, lat):
    """Tên tỉnh chứa từng điểm (mảng), '' nếu ngoài lãnh thổ."""
    pts = np.column_stack([lon, lat])
    name = np.full(len(pts), "", dtype=object)
    for prov, polys in provinces:
        for outer, holes in polys:
            inside = outer.contains_points(pts)
            for h in holes:
                inside &= ~h.contains_points(pts)
            name[inside & (name == "")] = prov
    return name


def cell_centres():
    n, w, s, e = AREA
    lats = np.round(np.arange(n, s - STEP / 2, -STEP), 4)
    lons = np.round(np.arange(w, e + STEP / 2, STEP), 4)
    return [(la, lo) for la in lats for lo in lons]


def build(points_path=REF / "points.csv"):
    provinces = load_provinces()
    with open(points_path, encoding="utf-8") as f:
        points = list(csv.DictReader(f))
    meta = {p["old_province"]: p for p in points}
    province_of_point = {p["location_id"]: p["old_province"] for p in points}
    point_cell = {}
    for p in points:  # ô chứa điểm: làm tròn về tâm ô gần nhất
        key = (round(round(float(p["lat"]) / STEP) * STEP, 4), round(round(float(p["lon"]) / STEP) * STEP, 4))
        point_cell.setdefault(key, []).append(p["location_id"])
    off = (np.arange(SAMPLES) + 0.5) / SAMPLES * STEP - STEP / 2
    rows = []
    for la, lo in cell_centres():
        slat, slon = np.meshgrid(la + off, lo + off)
        names = province_of(provinces, slon.ravel(), slat.ravel())
        inside = [x for x in names if x]
        frac = len(inside) / names.size
        locs = point_cell.get((la, lo), [])
        if frac < MIN_LAND_FRAC and not locs:
            continue
        # ô ven biển chứa điểm nhưng không có điểm mẫu nào trên đất liền: lấy tỉnh của điểm
        prov = Counter(inside).most_common(1)[0][0] if inside else province_of_point[locs[0]]
        m = meta[prov]
        rows.append({"grid_id": grid_id(la, lo), "grid_lat": la, "grid_lon": lo, "land_frac": round(frac, 2),
                     "old_province": prov, "new_province": m["new_province"],
                     "new_province_code": m["new_province_code"], "region": m["region"],
                     "location_ids": "|".join(locs)})
    return rows


def main():
    rows = build()
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    n_prov = len({r["old_province"] for r in rows})
    n_pts = sum(len(r["location_ids"].split("|")) for r in rows if r["location_ids"])
    print(f"{len(rows)} ô đất liền, {n_prov} tỉnh cũ, {len({r['new_province'] for r in rows})} tỉnh mới, "
          f"{n_pts}/63 điểm có ô → {OUT}")


if __name__ == "__main__":
    main()
