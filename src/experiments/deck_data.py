"""Gom số liệu đã chạy (reports/, lake) thành reports/deck_data.json cho bộ slide.
Không tính lại model; chỉ đọc kết quả có sẵn, nên mọi số trên slide truy được về file nguồn.

    python -m src.experiments.deck_data --profile pilot
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from src.common.config import ROOT, load_config, path

R = ROOT / "reports"


def dir_size_mb(p: Path):
    return round(sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) / 1e6, 1) if p.exists() else None


def file_count(p: Path, pattern="*"):
    return sum(1 for f in p.rglob(pattern) if f.is_file()) if p.exists() else 0


def read_json(p):
    return json.loads(Path(p).read_text(encoding="utf-8")) if Path(p).exists() else None


def samples(cfg, lake):
    """Vài dòng dữ liệu thật của từng thực nghiệm để đưa lên slide."""
    sys.path.insert(0, str(ROOT / "validation"))
    import validate_era5_noaa as V
    out = {}

    # E0: ngày mưa lớn ở trạm, so với mưa ERA5 cùng ngày (giờ VN)
    rows = []
    for sid, name in (("VMM00048855", "Da Nang"), ("VMM00048825", "Ha Noi (Ha Dong)")):
        st, _ = V.load_noaa(ROOT / "data" / "validation" / "noaa" / f"{sid}.csv", "2015-01-01", "2024-12-31")
        _, vn, _, _ = V.load_era(ROOT / "data" / "validation" / "era5" / f"{sid}_2015-01-01_2024-12-31.json")
        heavy = sorted((d for d in st if st[d] > 50 and d in vn), key=lambda d: -st[d])
        for day in heavy[:3]:
            rows.append({"station": name, "date": day, "gauge_mm": round(st[day], 1), "era5_mm": round(vn[day], 1),
                         "era5_heavy": vn[day] > 50})
    out["e0_noaa_vs_era5"] = rows

    # E1–E3: dòng gold của Hải Phòng quanh bão Yagi, mỗi 6 giờ
    cols = ["location_id", "ts", "rain_3h", "rain_24h", "rh_mean_6h", "pressure_change_24h", "elevation_m",
            "distance_to_coast_km", "neighbor_rain_sum_6h", "neighbor_pressure_change_24h", "rain_next_24h", "label"]
    g = pd.read_parquet(lake / "gold" / "features_hourly" / "year=2024", columns=[c for c in cols if c != "year"])
    g = g[(g.location_id == "HPHO") & (g.ts >= "2024-09-05 12:00") & (g.ts <= "2024-09-08 00:00")]
    g = g[g.ts.dt.hour % 12 == 0].sort_values("ts")
    g["ts"] = g.ts.dt.strftime("%d/%m %H:%M")
    out["gold_rows"] = g.round(2).to_dict("records")

    # E2–E3: xác suất của M1/M2/M3 trên cùng các dòng test
    pe = pd.read_parquet(R / "figures" / "_predictions_eval.parquet")
    pe = pe[pe.split == "test"]
    wide = pe[pe.model.isin(["M1", "M2", "M3"])].pivot_table(index=["location_id", "ts", "label"], columns="model",
                                                             values="p").reset_index()
    pick = []
    for loc in ("HGIA", "LDON", "HUE"):
        w = wide[wide.location_id == loc]
        pos = w[w.label == 1].sort_values("ts").head(1)
        neg = w[(w.label == 0) & (w.M1 > 0.3)].sort_values("ts").head(1)
        pick += [pos, neg]
    s = pd.concat(pick)
    s["ts"] = pd.to_datetime(s.ts).dt.strftime("%Y-%m-%d %H:%M")
    out["predictions"] = s.round(3).to_dict("records")

    # MapReduce: vài dòng input và output
    inp = sorted((ROOT / "data" / "mr" / "input").glob("part-*.csv"))[0].read_text(encoding="utf-8").splitlines()
    outp = sorted((ROOT / "data" / "mr" / "output").glob("part-*"))[0].read_text(encoding="utf-8").splitlines()
    out["mr_input"], out["mr_output"] = inp[:4], [l for l in outp if l.startswith(("HGIA|06", "HUE|10", "LDON|10", "HGIA|07"))][:4]

    # E7: dòng back-test Yagi
    y = pd.read_csv(R / "yagi_replay_backtest.csv")
    y = y[(y.location_id == "HPHO") & y.ts.str.slice(11, 13).isin(["00", "12"]) & (y.ts >= "2024-09-05")].head(9)
    out["yagi_rows"] = y.round(3).to_dict("records")
    return out


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="pilot")
    cfg = load_config(profile=ap.parse_args().profile)
    lake, raw = path(cfg, "lake"), path(cfg, "raw")
    d = {}

    # NOAA vs ERA5 (validation/)
    v = pd.read_csv(ROOT / "validation" / "results_era5.csv")
    vn = v[v.day == "VN"]
    d["noaa"] = {"stations": vn[["name", "corr", "ratio_total", "heavy_station", "hit", "hit_pm1",
                                 "false_alarm", "max_station", "max_era", "era_threshold_matched"]]
                 .round(3).to_dict("records"),
                 "heavy_days": int(vn.heavy_station.sum()), "hit": int(vn.hit.sum()), "hit_pm1": int(vn.hit_pm1.sum()),
                 "false_alarm": int(vn.false_alarm.sum()),
                 "median_corr_vn": round(float(vn["corr"].median()), 2),
                 "median_corr_utc": round(float(v[v.day == "UTC"]["corr"].median()), 2),
                 "median_ratio": round(float(vn.ratio_total.median()), 2)}

    # dữ liệu và chất lượng
    d["quality"] = {"silver": read_json(R / "quality" / "silver_summary.json"),
                    "gold": read_json(R / "quality" / "gold_summary.json"),
                    "funnel": pd.read_csv(R / "quality" / "silver_funnel.csv").to_dict("records"),
                    "exclusions": pd.read_csv(R / "quality" / "gold_split_exclusions.csv").fillna("kept").to_dict("records")}
    plan = pd.read_csv(path(cfg, "grid_plan"))
    d["ingest"] = {"raw_files": file_count(raw, "year=*.json"), "raw_mb": dir_size_mb(raw),
                   "grids": int(plan.grid_id.nunique()), "locations": int(plan.location_id.nunique()),
                   "neighbors": int((plan.role == "neighbor").sum()),
                   "shared_grids": int(plan.groupby("grid_id").location_id.nunique().gt(1).sum()),
                   "years": cfg["years"]}
    d["storage"] = {k: dir_size_mb(lake / p) for k, p in
                    {"bronze": "bronze/era5_hourly", "silver": "silver/grid_weather",
                     "gold": "gold/features_hourly", "models": "models"}.items()}
    d["storage"]["raw_json"] = d["ingest"]["raw_mb"]
    d["storage"]["gold_files"] = file_count(lake / "gold" / "features_hourly", "*.parquet")
    d["geography"] = pd.read_csv(path(cfg, "geography")).drop(columns=["source"]).to_dict("records")

    # EDA từ Q1 Spark SQL
    q1 = R / "spark_sql" / "q1_heavy_rate_region_month_result.csv"
    if q1.exists():
        d["eda_region_month"] = pd.read_csv(q1).to_dict("records")
    q2 = R / "spark_sql" / "q2_heavy_days_province_year_result.csv"
    if q2.exists():
        d["eda_province_year"] = pd.read_csv(q2).to_dict("records")

    # ablation
    d["ablation"] = {"results": pd.read_csv(R / "ablation_results.csv").to_dict("records"),
                     "deltas": pd.read_csv(R / "ablation_deltas.csv").to_dict("records"),
                     "cohort": pd.read_csv(R / "ablation_cohort.csv").to_dict("records"),
                     "importance": pd.read_csv(R / "feature_importance.csv").to_dict("records"),
                     "manifest": read_json(R / "ablation_manifest.json")}
    from src.experiments.selection import select_model
    d["selected_model"] = select_model(R / "ablation_deltas.csv")
    p8 = R / "runs" / "pilot8"
    if p8.exists():  # lần chạy 8 điểm, giữ lại để so với lần thêm điểm vùng cao
        d["ablation_pilot8"] = {"results": pd.read_csv(p8 / "ablation_results.csv").to_dict("records"),
                                "deltas": pd.read_csv(p8 / "ablation_deltas.csv").to_dict("records"),
                                "locations": read_json(p8 / "ablation_manifest.json")["locations"]}
    for name in ("results", "deltas", "per_location"):
        f = R / f"ablation_lolo_{name}.csv"
        if f.exists():
            d["ablation"][f"lolo_{name}"] = pd.read_csv(f).to_dict("records")
    pe = R / "figures" / "_predictions_eval.parquet"
    if pe.exists():
        from src.experiments.metrics import pr_curve
        p = pd.read_parquet(pe)
        curves = {}
        for m in ("M1", "M2", "M3", "persistence_rain24h"):
            t = p[(p.model == m) & (p.split == "test")]
            prec, rec, _ = pr_curve(t.label.to_numpy(), t.p.to_numpy())
            # precision nội suy: precision lớn nhất đạt được ở recall >= r
            best = np.maximum.accumulate(prec[::-1])[::-1]
            grid = np.linspace(0, 1, 41)
            idx = np.searchsorted(rec, grid, side="left").clip(0, len(rec) - 1)
            curves[m] = [{"recall": round(float(g), 3), "precision": round(float(best[i]), 4)} for g, i in zip(grid, idx)]
        d["pr_curves"] = curves

    # streaming, Mongo, MapReduce, Spark SQL plans, performance
    y = R / "yagi_replay_backtest.csv"
    if y.exists():
        d["yagi"] = pd.read_csv(y).to_dict("records")
    d["stream"] = {"files": file_count(ROOT / cfg["streaming"]["stream_in"], "*.json"),
                   "checkpoint_commits": sum(1 for x in (ROOT / cfg["streaming"]["checkpoint"] / "commits").glob("*")
                                             if x.is_file() and x.name.isdigit())}
    d["mongo"] = read_json(R / "mongo" / "read_back.json")
    d["mapreduce"] = {"validation": read_json(R / "mapreduce" / "validation.json"),
                      "input_mb": dir_size_mb(ROOT / "data" / "mr" / "input")}
    log = R / "mapreduce" / "job.log"
    if log.exists():
        keep = ("Launched map tasks", "Launched reduce tasks", "Map input records", "Map output records",
                "Combine input records", "Combine output records", "Reduce input records",
                "Reduce output records", "Reduce shuffle bytes", "Running job", "completed successfully", "real\t")
        d["mapreduce"]["log"] = [l.strip() for l in log.read_text(encoding="utf-8", errors="replace").splitlines()
                                 if any(k in l for k in keep)]
    plans = {}
    for f in sorted((R / "spark_sql").glob("*_plan.txt")) + sorted((R / "performance").glob("*_plan_*.txt")):
        lines = f.read_text(encoding="utf-8").splitlines()
        start = next((i for i, l in enumerate(lines) if "Physical Plan" in l), -1) + 1
        tree = []
        for l in lines[start:]:
            if not l.strip():
                break
            tree.append(l.rstrip())
        plans[f.stem] = tree
    d["plans"] = plans
    ps = R / "performance" / "perf_summary.csv"
    if ps.exists():
        d["performance"] = {"summary": pd.read_csv(ps).to_dict("records"),
                            "env": read_json(R / "performance" / "environment.json")}

    d["samples"] = samples(cfg, lake)

    def clean(x):  # NaN không hợp lệ trong JSON chuẩn → null
        if isinstance(x, float) and x != x:
            return None
        if isinstance(x, dict):
            return {k: clean(v) for k, v in x.items()}
        if isinstance(x, list):
            return [clean(v) for v in x]
        return x

    out = R / "deck_data.json"
    out.write_text(json.dumps(clean(d), indent=1, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"→ {out} ({out.stat().st_size / 1e3:.0f} KB)")


if __name__ == "__main__":
    main()
