"""Bản đồ minh họa: trạm đo mưa NOAA (một điểm) nằm trong một ô ERA5 (~26 × 28 km), nền OpenStreetMap.

Ô lưới lấy từ data/reference/grid_plan.csv (ô trung tâm của Hà Nội), trạm Hà Đông lấy từ
validation/validate_era5_noaa.py.

    python deck/make_gauge_map.py
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
sys.path.insert(0, str(ROOT / "validation"))
from validate_era5_noaa import STATIONS  # noqa: E402

TILES = ROOT / "data" / "reference" / "osm_tiles"
Z = 11
STEP = 0.25
ORANGE, RED, TEAL = "#E0712B", "#B23A48", "#0E7C86"
KM_PER_DEG = 111.32


def deg2xy(lat, lon):
    n = 2 ** Z
    return (lon + 180) / 360 * n, (1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n


def tile(x, y):
    f = TILES / f"{Z}_{x}_{y}.png"
    if not f.exists():
        f.parent.mkdir(parents=True, exist_ok=True)
        req = urllib.request.Request(f"https://tile.openstreetmap.de/{Z}/{x}/{y}.png",
                                     headers={"User-Agent": "BDA501-student-project/1.0 (static slide figure)"})
        f.write_bytes(urllib.request.urlopen(req, timeout=60).read())
        time.sleep(1)
    return Image.open(io.BytesIO(f.read_bytes())).convert("RGB")


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    c = next(r for r in csv.DictReader(open(ROOT / "data" / "reference" / "grid_plan.csv", encoding="utf-8"))
             if r["location_id"] == "HNOI" and r["role"] == "center")
    glat, glon = float(c["grid_lat"]), float(c["grid_lon"])
    sid, sname, slat, slon = next(s for s in STATIONS if s[0] == "VMM00048825")
    pad = 0.05
    lat0, lat1 = glat - STEP / 2 - pad, glat + STEP / 2 + pad
    lon0, lon1 = glon - STEP / 2 - pad, glon + STEP / 2 + pad
    x0, y1 = deg2xy(lat0, lon0)
    x1, y0 = deg2xy(lat1, lon1)
    tx0, tx1, ty0, ty1 = int(x0), int(x1), int(y0), int(y1)
    mosaic = Image.new("RGB", ((tx1 - tx0 + 1) * 256, (ty1 - ty0 + 1) * 256))
    for tx in range(tx0, tx1 + 1):
        for ty in range(ty0, ty1 + 1):
            mosaic.paste(tile(tx, ty), ((tx - tx0) * 256, (ty - ty0) * 256))
    crop = mosaic.crop((int((x0 - tx0) * 256), int((y0 - ty0) * 256), int((x1 - tx0) * 256), int((y1 - ty0) * 256)))
    px = lambda lat, lon: ((deg2xy(lat, lon)[0] - x0) * 256, (deg2xy(lat, lon)[1] - y0) * 256)
    w_km = STEP * KM_PER_DEG * math.cos(math.radians(glat))
    h_km = STEP * KM_PER_DEG

    for lang in ("en", "vi"):
        T = (lambda en, vi: en if lang == "en" else vi)
        fig, ax = plt.subplots(figsize=(6, 6 * crop.height / crop.width), dpi=200)
        ax.imshow(crop)
        ax.add_patch(mp.Rectangle((0, 0), crop.width, crop.height, color="white", alpha=0.2))
        (ax_, ay_), (bx, by) = px(glat + STEP / 2, glon - STEP / 2), px(glat - STEP / 2, glon + STEP / 2)
        ax.add_patch(mp.Rectangle((ax_, ay_), bx - ax_, by - ay_, facecolor=ORANGE, alpha=0.18, edgecolor=ORANGE, linewidth=2.5))
        ax.text(ax_ + 8, ay_ + 8, T("one ERA5 cell: its rain = the average over this whole square",
                                   "một ô ERA5: lượng mưa = trung bình trên cả ô vuông này"),
                fontsize=8, fontweight="bold", color="white", va="top", bbox=dict(boxstyle="round,pad=0.3", facecolor=ORANGE, edgecolor="none"))
        # kích thước ô
        ax.annotate("", xy=(bx, by + 18), xytext=(ax_, by + 18), arrowprops=dict(arrowstyle="<->", color=ORANGE, lw=1.6))
        ax.text((ax_ + bx) / 2, by + 30, f"≈ {w_km:.0f} km", ha="center", va="top", fontsize=9, fontweight="bold", color=ORANGE,
                bbox=dict(facecolor="white", edgecolor="none", alpha=0.85, pad=1))
        ax.annotate("", xy=(bx + 18, ay_), xytext=(bx + 18, by), arrowprops=dict(arrowstyle="<->", color=ORANGE, lw=1.6))
        ax.text(bx + 26, (ay_ + by) / 2, f"≈ {h_km:.0f} km", ha="left", va="center", fontsize=9, fontweight="bold", color=ORANGE, rotation=90,
                bbox=dict(facecolor="white", edgecolor="none", alpha=0.85, pad=1))
        # trạm đo
        gx, gy = px(slat, slon)
        ax.plot(gx, gy, marker="v", color=TEAL, markersize=13, markeredgecolor="white", mew=1.2)
        ax.annotate(T(f"NOAA rain gauge\n{sname}: one point,\nfunnel ≈ 20 cm wide", f"Trạm đo mưa NOAA\n{sname}: một điểm,\nmiệng ống ≈ 20 cm"),
                    (gx, gy), xytext=(gx - 175, gy + 85), fontsize=8, fontweight="bold", color=TEAL,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor=TEAL),
                    arrowprops=dict(arrowstyle="-", color=TEAL, lw=1.2))
        # thước tỷ lệ 10 km
        km_px = (px(glat, glon + 10 / (KM_PER_DEG * math.cos(math.radians(glat))))[0] - px(glat, glon)[0])
        sx, sy = 14, crop.height - 22
        ax.plot([sx, sx + km_px], [sy, sy], color="black", lw=3)
        ax.text(sx + km_px / 2, sy - 6, "10 km", ha="center", va="bottom", fontsize=8, fontweight="bold",
                bbox=dict(facecolor="white", edgecolor="none", alpha=0.85, pad=1))
        ax.text(crop.width - 4, crop.height - 4, "© OpenStreetMap contributors", ha="right", va="bottom", fontsize=6, color="#333",
                bbox=dict(facecolor="white", edgecolor="none", alpha=0.8, pad=1))
        ax.set_xlim(0, crop.width)
        ax.set_ylim(crop.height, 0)
        ax.set_axis_off()
        fig.subplots_adjust(0, 0, 1, 1)
        out = ROOT / "reports" / "figures" / f"gauge_vs_cell_{lang}.png"
        fig.savefig(out, dpi=200)
        plt.close(fig)
        print(out, f"cell ≈ {w_km:.1f} × {h_km:.1f} km")


if __name__ == "__main__":
    main()
