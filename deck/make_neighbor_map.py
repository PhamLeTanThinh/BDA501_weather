"""Bản đồ minh họa ô ERA5 trung tâm + 4 ô lân cận của một điểm dự báo, trên nền OpenStreetMap (máy chủ tile.openstreetmap.de).

Ô lưới lấy từ data/reference/grid_plan.csv (ô API thật trả về), nền là tile OSM (cache trong
data/reference/osm_tiles/, ghi nguồn © OpenStreetMap contributors).

    python deck/make_neighbor_map.py HNOI
"""
import csv
import io
import math
import sys
import time
import urllib.request
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.patches as mp  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from PIL import Image  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TILES = ROOT / "data" / "reference" / "osm_tiles"
Z = 10
STEP = 0.25
TEAL, ORANGE, GREY = "#0E7C86", "#E0712B", "#5E6B73"


def deg2xy(lat, lon, z=Z):
    n = 2 ** z
    x = (lon + 180) / 360 * n
    y = (1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n
    return x, y


def tile(x, y):
    f = TILES / f"{Z}_{x}_{y}.png"
    if not f.exists():
        f.parent.mkdir(parents=True, exist_ok=True)
        req = urllib.request.Request(f"https://tile.openstreetmap.de/{Z}/{x}/{y}.png",
                                     headers={"User-Agent": "BDA501-student-project/1.0 (static slide figure)"})
        f.write_bytes(urllib.request.urlopen(req, timeout=60).read())
        time.sleep(1)
    return Image.open(io.BytesIO(f.read_bytes())).convert("RGB")


def main(loc="HNOI"):
    rows = [r for r in csv.DictReader(open(ROOT / "data" / "reference" / "grid_plan.csv", encoding="utf-8")) if r["location_id"] == loc]
    plat, plon = float(rows[0]["point_lat"]), float(rows[0]["point_lon"])
    lats = [float(r["grid_lat"]) for r in rows]
    lons = [float(r["grid_lon"]) for r in rows]
    pad = STEP * 0.5 + 0.08
    lat0, lat1, lon0, lon1 = min(lats) - pad, max(lats) + pad, min(lons) - pad, max(lons) + pad
    x0, y1 = deg2xy(lat0, lon0)
    x1, y0 = deg2xy(lat1, lon1)
    tx0, tx1, ty0, ty1 = int(x0), int(x1), int(y0), int(y1)
    mosaic = Image.new("RGB", ((tx1 - tx0 + 1) * 256, (ty1 - ty0 + 1) * 256))
    for tx in range(tx0, tx1 + 1):
        for ty in range(ty0, ty1 + 1):
            mosaic.paste(tile(tx, ty), ((tx - tx0) * 256, (ty - ty0) * 256))
    crop = mosaic.crop((int((x0 - tx0) * 256), int((y0 - ty0) * 256), int((x1 - tx0) * 256), int((y1 - ty0) * 256)))
    px = lambda lat, lon: ((deg2xy(lat, lon)[0] - x0) * 256, (deg2xy(lat, lon)[1] - y0) * 256)

    for lang in ("en", "vi"):
        T = (lambda en, vi: en if lang == "en" else vi)
        fig, ax = plt.subplots(figsize=(6, 6 * crop.height / crop.width), dpi=200)
        ax.imshow(crop)
        ax.add_patch(mp.Rectangle((0, 0), crop.width, crop.height, color="white", alpha=0.25))
        for r in rows:
            glat, glon = float(r["grid_lat"]), float(r["grid_lon"])
            (ax_, ay_), (bx, by) = px(glat + STEP / 2, glon - STEP / 2), px(glat - STEP / 2, glon + STEP / 2)
            centre = r["role"] == "center"
            col = ORANGE if centre else TEAL
            ax.add_patch(mp.Rectangle((ax_, ay_), bx - ax_, by - ay_, facecolor=col, alpha=0.28, edgecolor=col, linewidth=2.2))
            cx, cy = px(glat, glon)
            label = T("centre cell\n(weather M1)", "ô trung tâm\n(thời tiết M1)") if centre else T("neighbour\ncell (M3)", "ô lân cận\n(M3)")
            ax.text(cx, cy + (by - ay_) * 0.28, label, ha="center", va="center", fontsize=8.5, fontweight="bold", color="white",
                    bbox=dict(boxstyle="round,pad=0.25", facecolor=col, edgecolor="none", alpha=0.9))
            ax.plot(cx, cy, marker="+", color=col, markersize=7, mew=1.5)
        # các ô góc không dùng
        c = next(r for r in rows if r["role"] == "center")
        for di in (-1, 1):
            for dj in (-1, 1):
                glat, glon = float(c["grid_lat"]) + di * STEP, float(c["grid_lon"]) + dj * STEP
                (ax_, ay_), (bx, by) = px(glat + STEP / 2, glon - STEP / 2), px(glat - STEP / 2, glon + STEP / 2)
                ax.add_patch(mp.Rectangle((ax_, ay_), bx - ax_, by - ay_, fill=False, edgecolor=GREY, linewidth=1, linestyle="--", alpha=0.7))
        ppx, ppy = px(plat, plon)
        ax.plot(ppx, ppy, marker="*", color="#B23A48", markersize=17, markeredgecolor="white", mew=1.2)
        ax.annotate(T("prediction point\nHa Noi (21.03 N, 105.85 E)", "điểm dự báo\nHà Nội (21,03 B, 105,85 Đ)"), (ppx, ppy),
                    xytext=(ppx + 20, ppy - 55), fontsize=8.5, fontweight="bold", color="#B23A48",
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#B23A48", alpha=0.95),
                    arrowprops=dict(arrowstyle="-", color="#B23A48", lw=1.2))
        ax.text(crop.width - 4, crop.height - 4, "© OpenStreetMap contributors", ha="right", va="bottom", fontsize=6, color="#333",
                bbox=dict(facecolor="white", edgecolor="none", alpha=0.8, pad=1))
        ax.set_axis_off()
        fig.subplots_adjust(0, 0, 1, 1)
        out = ROOT / "reports" / "figures" / f"neighbor_map_{lang}.png"
        fig.savefig(out, dpi=200)
        plt.close(fig)
        print(out)


if __name__ == "__main__":
    main(*sys.argv[1:])
