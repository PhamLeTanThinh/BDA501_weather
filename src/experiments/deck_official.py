"""Số liệu bản chính thức cho slide → reports/official/deck_official.json, kèm hình bản đồ ô theo vùng.

Chỉ đọc các báo cáo đã có (manifest tải CDS, quality, eda, hdfs); không chạy Spark.

    python -m src.experiments.deck_official
"""
import csv
import json
import re

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from src.common.config import ROOT, load_config, path  # noqa: E402

OUT = ROOT / "reports" / "official"
CAT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
REGIONS = [("Bac", "Bắc Bộ / North"), ("BacTrungBo", "Bắc Trung Bộ / North Central"),
           ("NamTrungBo", "Nam Trung Bộ / South Central"), ("TayNguyen", "Tây Nguyên / Highlands"),
           ("Nam", "Nam Bộ / South")]


def download_stats(raw):
    """Số request CDS thành công theo tài khoản, thời gian chờ trung vị, số file và dung lượng raw."""
    accounts, waits = {}, []
    for m in sorted(raw.glob("jobs_*.csv")):
        with open(m, encoding="utf-8") as f:
            rows = [r for r in csv.DictReader(f) if r["status"] == "successful"]
        accounts[m.stem.removeprefix("jobs_")] = len({r["file"] for r in rows})
        for r in rows:
            s = re.match(r"(\d+) s", r["note"] or "")
            if s and int(s.group(1)) > 60:  # bỏ các dòng ghi lại khi job khởi động lại (vài giây)
                waits.append(int(s.group(1)))
    zips = sorted(raw.glob("year=*/era5_vn_*.zip"))
    return {"requests_by_account": accounts, "requests_total": sum(accounts.values()),
            "accounts": len(accounts), "median_request_hours": round(pd.Series(waits).median() / 3600, 1),
            "zip_files": len(zips), "raw_gb": round(sum(z.stat().st_size for z in zips) / 1e9, 2)}


def e0_stats(raw_silver):
    """E0 trên dữ liệu CDS: tổng hợp results_cds.csv + 3 ngày mưa nhiều nhất ở Đà Nẵng và Hà Đông."""
    import sys
    sys.path.insert(0, str(ROOT / "validation"))
    import validate_era5_noaa as V
    v = pd.read_csv(ROOT / "validation" / "results_cds.csv")
    vn = v[v.day == "VN"]
    noaa = {"stations": vn[["name", "corr", "ratio_total", "heavy_station", "hit", "hit_pm1", "false_alarm",
                            "max_station", "max_era", "era_threshold_matched"]].round(3).to_dict("records"),
            "heavy_days": int(vn.heavy_station.sum()), "hit": int(vn.hit.sum()), "hit_pm1": int(vn.hit_pm1.sum()),
            "false_alarm": int(vn.false_alarm.sum()), "median_corr_vn": round(float(vn["corr"].median()), 2),
            "median_corr_utc": round(float(v[v.day == "UTC"]["corr"].median()), 2),
            "median_ratio": round(float(vn.ratio_total.median()), 2), "years": "2015–2025", "n_years": 11}
    st_ll = {sid: (lat, lon) for sid, _, lat, lon in V.STATIONS}
    rows = []
    for sid, name in (("VMM00048855", "Da Nang"), ("VMM00048825", "Ha Noi (Ha Dong)")):
        st, _ = V.load_noaa(ROOT / "data" / "validation" / "noaa" / f"{sid}.csv", "2015-01-01", "2025-12-31")
        _, era, _, _ = V.load_era_cds(*st_ll[sid], raw_silver)
        heavy = sorted((d for d in st if st[d] > 50 and d in era), key=lambda d: -st[d])
        rows += [{"station": name, "date": d, "gauge_mm": round(st[d], 1), "era5_mm": round(era[d], 1),
                  "era5_heavy": era[d] > 50} for d in heavy[:3]]
    return {"noaa": noaa, "samples": rows}


def cell_map(cells, out):
    fig, ax = plt.subplots(figsize=(4.6, 8.4), facecolor="#ffffff")
    for color, (code, label) in zip(CAT, REGIONS):
        d = cells[cells.region == code]
        ax.scatter(d.grid_lon, d.grid_lat, marker="s", s=30, color=color, linewidths=0,
                   label=f"{label} ({len(d)})")
    ax.set_aspect("equal")
    for s in ax.spines.values():
        s.set_visible(False)
    ax.tick_params(colors="#52514e", labelsize=8)
    # chú thích đặt dưới bản đồ để không che các ô Nam Bộ
    ax.legend(frameon=False, fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.04), ncol=1,
              labelcolor="#52514e", markerscale=1.2)
    fig.tight_layout()
    fig.savefig(out, dpi=220)
    plt.close(fig)


def hero_map(by_cell, out):
    """Bản đồ trang bìa: tỷ lệ mưa lớn theo ô, nền trong suốt; xanh nhạt = thấp, cam đậm = cao (trên nền sáng)."""
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list("hero", ["#C9DEE0", "#6FAAB0", "#F2A65A", "#C2501A"])
    fig, ax = plt.subplots(figsize=(4.0, 7.3))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    ax.scatter(by_cell.lon, by_cell.lat, c=by_cell.positive_rate, cmap=cmap, marker="s", s=34, linewidths=0)
    ax.set_aspect("equal")
    ax.axis("off")
    fig.subplots_adjust(0, 0, 1, 1)
    fig.savefig(out, dpi=260, transparent=True, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)


def main():
    cfg = load_config(profile="official")
    cells = pd.read_csv(path(cfg, "cells"))
    q = OUT / "quality"
    eda = OUT / "eda"
    corr = pd.read_csv(eda / "feature_label_corr.csv")
    split = pd.read_csv(eda / "label_by_split.csv").fillna({"split": "excluded"}).set_index("split")
    prov = pd.read_csv(eda / "label_by_new_province.csv")
    data = {
        "download": download_stats(path(cfg, "raw_cds")),
        "cells": {"n": len(cells), "old_provinces": cells.old_province.nunique(),
                  "new_provinces": cells.new_province.nunique(),
                  "by_region": cells.region.value_counts().to_dict(),
                  "cells_per_new_province": {"min": int(cells.new_province.value_counts().min()),
                                             "max": int(cells.new_province.value_counts().max())},
                  "examples": {p: int((cells.new_province == p).sum()) for p in ["Lâm Đồng", "Gia Lai", "Hà Nội", "Hưng Yên", "Nghệ An"]}},
        "silver": json.loads((q / "silver_summary.json").read_text(encoding="utf-8")),
        "gold": json.loads((q / "gold_summary.json").read_text(encoding="utf-8")),
        "volume": json.loads((eda / "volume.json").read_text(encoding="utf-8")),
        "split": {s: {"rows": int(r.rows), "positives": int(r.positives), "rate": float(r.positive_rate)}
                  for s, r in split.iterrows()},
        "top_corr": corr.head(8).to_dict("records"),
        "top_provinces": prov.head(5)[["new_province", "positive_rate"]].to_dict("records"),
        # JSON không có NaN (Node không đọc được): ô trống → null
        "precip": pd.read_csv(eda / "precip_quantiles.csv").astype(object).where(lambda d: d.notna(), None).to_dict("records"),
    }
    mdir = OUT / "model"
    if (mdir / "ablation_results.csv").exists():  # ablation M1–M4 trên dữ liệu chính thức
        res = pd.read_csv(mdir / "ablation_results.csv")
        clean = lambda df: df.round(5).astype(object).where(lambda d: d.notna(), None).to_dict("records")  # NaN → null
        data["ablation"] = {"results": clean(res),
                            "deltas": pd.read_csv(mdir / "ablation_deltas.csv").round(5).to_dict("records"),
                            "by_region": pd.read_csv(mdir / "ablation_by_region.csv").round(4).to_dict("records"),
                            "importance": pd.read_csv(mdir / "feature_importance.csv").round(4).to_dict("records"),
                            "manifest": json.loads((mdir / "ablation_manifest.json").read_text(encoding="utf-8"))}
    data["e0"] = e0_stats(str(path(cfg, "lake") / "silver" / "grid_weather"))
    hdfs = OUT / "hdfs_ls.txt"
    data["hdfs"] = hdfs.read_text(encoding="utf-8").splitlines()[:2] if hdfs.exists() else []
    (OUT / "deck_official.json").write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str),
                                            encoding="utf-8")
    cell_map(cells, OUT / "fig_cells_region.png")
    hero_map(pd.read_csv(eda / "label_by_cell.csv"), OUT / "fig_hero_map.png")
    print(json.dumps(data["download"], indent=2), data["hdfs"])


if __name__ == "__main__":
    main()
