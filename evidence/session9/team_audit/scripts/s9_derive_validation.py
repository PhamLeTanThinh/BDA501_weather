"""Session 9 - tái dẫn xuất số liệu kiểm chứng dữ liệu từ output đã lưu trong reports/ (không cần Spark).

Đọc: reports/official/quality/{silver_summary,gold_summary,neighbor_summary}.json, gold_by_split.csv,
     reports/official/model/ablation_manifest.json, ablation_results.csv,
     reports/official/eda/{volume.json,label_by_cell.csv,label_by_new_province.csv}, validation/results_cds.csv,
     reports/mapreduce/validation.json
Ghi: evidence/session9/D_data_validation.csv và evidence/session9/out/derive_validation.txt

    python evidence/session9/scripts/s9_derive_validation.py
"""
import csv
import json
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
R = ROOT / "reports"
OUT = ROOT / "evidence" / "session9"


def jload(p):
    return json.loads((ROOT / p).read_text(encoding="utf-8"))


def hours(d0, d1):  # số giờ từ 00:00 ngày d0 đến 23:00 ngày d1 (giờ VN, không có DST)
    return ((d1 - d0).days + 1) * 24


def main():
    lines, rows = [], []

    def check(item, method, expected, observed, src):
        ok = expected == observed
        rows.append({"item": item, "method": method, "expected": expected, "observed": observed,
                     "pass": "PASS" if ok else "FAIL", "source": src, "label": "MEASURED (stored output) + re-derived"})
        lines.append(f"[{'PASS' if ok else 'FAIL'}] {item}: expected={expected} observed={observed}  ({src})")

    sv = jload("reports/official/quality/silver_summary.json")
    gd = jload("reports/official/quality/gold_summary.json")
    nb = jload("reports/official/quality/neighbor_summary.json")
    man = jload("reports/official/model/ablation_manifest.json")
    vol = jload("reports/official/eda/volume.json")
    cells = sv["cells"]

    h_all = hours(date(2015, 1, 1), date(2025, 12, 31))
    check("silver rows = cells x hours 2015-2025 (VN)", "493 x 96,432 h", cells * h_all, sv["silver_rows"],
          "reports/official/quality/silver_summary.json")
    check("bronze - silver = 24 h/cell trimmed by UTC->VN window", "bronze includes 2014-12-31 UTC file; 17 h head + 7 h tail",
          cells * 24, sv["bronze_rows"] - sv["silver_rows"], "silver_summary.json")
    check("gold rows = silver rows (1 row / cell-hour)", "gold_summary.rows vs silver_rows", sv["silver_rows"], gd["rows"],
          "gold_summary.json")
    check("duplicate (cell, hour) keys in silver", "groupBy(grid_id, ts_epoch).count>1", 0, sv["duplicate_cell_hours"], "silver_summary.json")
    check("duplicate (location_id, ts) keys in gold", "groupBy(location_id, ts).count>1", 0, gd["duplicate_keys"], "gold_summary.json")
    check("cell-years incomplete", "count(hours) < 8760/8784 per (cell, year)", 0, sv["cell_years_incomplete"], "silver_summary.json")
    check("cells in silver = cells in map (join coverage)", "distinct grid_id vs cells_land.csv", sv["cells_in_map"], sv["cells"], "silver_summary.json")
    # split: mỗi split mất 24 giờ cuối do purge cửa sổ nhãn
    sp = {"train": (date(2015, 1, 1), date(2022, 12, 31)), "validation": (date(2023, 1, 1), date(2023, 12, 31)),
          "test": (date(2024, 1, 1), date(2025, 12, 31))}
    for k, (a, b) in sp.items():
        check(f"gold split rows {k} = cells x (hours - 24 purged)", "assign_split purge", cells * (hours(a, b) - 24),
              gd["split_rows"][k], "gold_summary.json")
    check("gold excluded rows = cells x 72 (3 boundaries x 24 h)", "split_excluded_reason", cells * 72, gd["split_rows"]["excluded"], "gold_summary.json")
    check("sum of split rows = gold rows", "train+val+test+excluded", gd["rows"], sum(gd["split_rows"].values()), "gold_summary.json")

    # gold_by_split: label null chỉ có ở 24 giờ cuối 2025 (tương lai không có)
    with open(R / "official/quality/gold_by_split.csv", encoding="utf-8") as f:
        gbs = {r["split"]: r for r in csv.DictReader(f)}
    check("label null rows (last 24 h of 2025, no future)", "gold_by_split.label_null(excluded)", cells * 24,
          int(gbs["excluded"]["label_null"]), "gold_by_split.csv")
    check("train null rain_72h = cells x 71 (warm-up at 2015-01-01)", "window (t-72h,t] incomplete -> null", cells * 71,
          int(gbs["train"]["null_rain_72h"]), "gold_by_split.csv")
    pos = {k: (int(float(gbs[k]["positives"])), int(gbs[k]["rows"])) for k in ("train", "validation", "test")}
    for k, (p, n) in pos.items():
        lines.append(f"positives {k}: {p:,} / {n:,} = {p / n:.5f}")

    # cohort ablation (483 ô có đủ M1..M4 + địa hình + lân cận)
    coh = {c["split"]: c for c in man["cohort"]}
    c_cells = man["cells"]
    check("ablation cohort validation ~ 483 x 8,736 minus rows with null k_index", "ablation_manifest.cohort",
          c_cells * 8736 - 2, coh["validation"]["rows"], "ablation_manifest.json + gold_by_split null_k_index=2")
    lines.append(f"cohort test: 483 x 17,520 = {c_cells * 17520:,}; observed {coh['test']['rows']:,} "
                 f"(diff {c_cells * 17520 - coh['test']['rows']} rows with null cape/cape_max_24h/k_index)")
    lines.append(f"cohort train: {coh['train']['rows']:,} rows, positives {coh['train']['positives']:,}; gold train positives {pos['train'][0]:,}")
    lines.append(f"cells: gold {gd['locations']}, cells with >=1 neighbour {nb['cells_with_neighbours']}, ablation cohort {c_cells} "
                 f"(=> {gd['locations'] - c_cells} cells dropped because M3/M2 features null)")
    lines.append(f"train sample: neg_frac {man['train_sample']['neg_frac']}, rows {man['train_sample']['rows']:,}, "
                 f"positives {man['train_sample']['positives']:,}, w+ {man['train_sample']['class_weight_positive']}")

    with open(R / "official/model/ablation_results.csv", encoding="utf-8") as f:
        ar = [r for r in csv.DictReader(f)]
    for r in ar:
        if r["split"] == "test":
            lines.append(f"test {r['model']:<20} PR-AUC {float(r['pr_auc']):.4f}  ROC-AUC {float(r['roc_auc']):.4f}  "
                         f"P {float(r['precision']):.3f} R {float(r['recall']):.3f}  TP {r['tp']} FP {r['fp']} FN {r['fn']}")
            if r["model"] != "persistence_rain24h":
                check(f"{r['model']} test TP+FN = cohort test positives", "confusion consistency",
                      coh["test"]["positives"], int(r["tp"]) + int(r["fn"]), "ablation_results.csv")
    prev = coh["test"]["positives"] / coh["test"]["rows"]
    lines.append(f"no-skill PR-AUC on test = prevalence = {prev:.4f}")

    # NOAA crosscheck: tổng hợp lại từ results_cds.csv (ngày VN)
    with open(ROOT / "validation/results_cds.csv", encoding="utf-8") as f:
        nr = [r for r in csv.DictReader(f) if r["day"] == "VN"]
    heavy = sum(int(r["heavy_station"]) for r in nr)
    hit = sum(int(r["hit"]) for r in nr)
    pm1 = sum(int(r["hit_pm1"]) for r in nr)
    fa = sum(int(r["false_alarm"]) for r in nr)
    corr = sorted(float(r["corr"]) for r in nr)
    med = (corr[len(corr) // 2 - 1] + corr[len(corr) // 2]) / 2 if len(corr) % 2 == 0 else corr[len(corr) // 2]
    check("NOAA: station heavy-rain days (VN day), 14 stations", "sum heavy_station", 1185, heavy, "validation/results_cds.csv")
    check("NOAA: ERA5 same-day hits", "sum hit", 292, hit, "validation/report_cds.md")
    lines.append(f"NOAA VN-day: hit {hit}/{heavy} = {hit / heavy:.1%}, +-1 day {pm1} = {pm1 / heavy:.1%}, false alarms {fa}, "
                 f"median daily corr {med:.2f} ({len(nr)} stations)")
    lines.append(f"Open-Meteo crosscheck (63 points): median daily precip corr {vol['crosscheck_openmeteo']['median_daily_precip_corr']:.4f}, "
                 f"median hourly {vol['crosscheck_openmeteo']['median_hourly_precip_corr']:.4f}")

    # province labels: tỷ lệ dương theo ô trong mỗi tỉnh (để thấy MAX vs MEAN khác nhau)
    with open(R / "official/eda/label_by_cell.csv", encoding="utf-8") as f:
        lc = list(csv.DictReader(f))
    byp = {}
    for r in lc:
        byp.setdefault(r["new_province"], []).append(float(r["positive_rate"]))
    lines.append(f"provinces in label_by_cell: {len(byp)}; cells per province min {min(map(len, byp.values()))}, "
                 f"max {max(map(len, byp.values()))}")
    spread = sorted(((max(v) / max(min(v), 1e-9), k, len(v), min(v), max(v)) for k, v in byp.items() if len(v) > 1), reverse=True)[:5]
    for ratio, k, n, lo, hi in spread:
        lines.append(f"  {k}: {n} cells, cell positive_rate min {lo:.4f} max {hi:.4f} (x{ratio:.1f})")

    mr = jload("reports/mapreduce/validation.json")
    check("MapReduce vs Spark SQL keys mismatch (pilot)", "mr_io validate", 0, mr["mismatches"] + mr["missing_or_extra"], "reports/mapreduce/validation.json")
    check("MapReduce vs Spark SQL rows (pilot)", "sum n", mr["rows_spark"], mr["rows_mapreduce"], "reports/mapreduce/validation.json")
    lines.append(f"expected official MR keys if rerun on official gold: {gd['locations']} x 12 = {gd['locations'] * 12:,}; "
                 f"expected rows = train rows with label & precip not null = {pos['train'][1]:,}")

    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "D_data_validation.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    (OUT / "out" / "derive_validation.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
