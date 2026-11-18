"""Figure for the recall-by-province analysis: (left) recall per cell on a map, (right) province recall vs heavy-rain rate.

    scripts/run_in_spark.sh src.analytics.province_recall_fig   → reports/official/model/fig_province_recall.png
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "reports" / "official" / "model"
REGION = {"Bac": ("North", "#5B7DB1"), "BacTrungBo": ("North-central coast", "#0E7C86"),
          "NamTrungBo": ("South-central coast", "#2E9E6B"), "TayNguyen": ("Central Highlands", "#E0A33B"),
          "Nam": ("South", "#B23A48")}


def main():
    c = pd.read_csv(OUT / "province_recall_cells.csv")
    p = pd.read_csv(OUT / "province_recall_provinces.csv")
    ref = pd.read_csv(ROOT / "data" / "reference" / "cells_land.csv")
    c = c.merge(ref[["grid_id", "grid_lat", "grid_lon"]], left_on="cell", right_on="grid_id")
    miss = ref[~ref.grid_id.isin(c.cell)]

    fig, (a, b) = plt.subplots(1, 2, figsize=(13, 7.2), gridspec_kw={"width_ratios": [0.9, 1.25]})
    sc = a.scatter(c.grid_lon, c.grid_lat, c=c.recall_f1, cmap="RdYlGn", vmin=0, vmax=0.9, s=46, marker="s", linewidths=0)
    a.scatter(miss.grid_lon, miss.grid_lat, s=46, marker="s", facecolors="none", edgecolors="#888", linewidths=0.8)
    a.set_aspect("equal"); a.set_xlim(101.8, 110); a.set_ylim(8.2, 23.7)
    a.set_title("Recall per cell, test 2024–2025 (threshold 0.932)", fontsize=12, loc="left")
    a.tick_params(labelsize=8); a.set_xlabel("lon", fontsize=8); a.set_ylabel("lat", fontsize=8)
    cb = fig.colorbar(sc, ax=a, shrink=0.6, pad=0.02); cb.ax.tick_params(labelsize=8); cb.set_label("recall", fontsize=9)
    a.text(102, 8.5, "hollow = 10 cells not scored\n(fewer than 2 land neighbours)", fontsize=8, color="#555")

    for r, (name, col) in REGION.items():
        d = p[p.region == r]
        b.scatter(d.heavy_rate_cells * 100, d.prov_recall_f1 * 100, s=d.cells * 9, color=col, alpha=0.8, label=name, edgecolors="white")
    for _, r in p.iterrows():
        if r.prov_recall_f1 > 0.6 or r.prov_recall_f1 < 0.2 or r.province in ("Lào Cai", "Tuyên Quang", "Lâm Đồng", "Hà Nội"):
            b.annotate(r.province, (r.heavy_rate_cells * 100, r.prov_recall_f1 * 100), fontsize=7.5, xytext=(4, 2),
                       textcoords="offset points", color="#333")
    b.axhline(46.4, color="#999", ls="--", lw=0.8); b.text(2.95, 47.3, "national 46%", fontsize=8, color="#666", ha="right")
    b.set_xlabel("heavy-rain share of cell-hours 2015–2025 (%)", fontsize=10)
    b.set_ylabel("province recall, any-area (%)", fontsize=10)
    b.set_title("Provinces: recall follows how often heavy rain occurs (bubble = cells)", fontsize=12, loc="left")
    b.legend(fontsize=8, loc="lower right", frameon=False); b.tick_params(labelsize=8)
    b.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(OUT / "fig_province_recall.png", dpi=160)
    print("written", OUT / "fig_province_recall.png")


if __name__ == "__main__":
    main()
