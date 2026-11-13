"""Session 9 - Card E2: warning-threshold trade-off computed from saved outputs (no Spark needed).

Reads reports/official/model/m4_threshold_choice.csv (official M4, thresholds chosen on validation 2023, applied to
test 2024-2025) and reports/yagi_replay_backtest.csv (pilot M1, Yagi replay). Writes evidence/session9/out/E2_*.csv/txt.

    python evidence/session9/scripts/s9_e2_threshold.py
"""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "evidence" / "session9" / "out"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    with open(ROOT / "reports/official/model/m4_threshold_choice.csv", encoding="utf-8") as f:
        rows = {r["rule"]: r for r in csv.DictReader(f)}
    lines, table = [], []
    for split in ("validation", "test"):
        for rule in ("f1", "prec30", "recall60"):
            r = rows[rule]
            tp, fp, fn = (int(r[f"{split}_{k}"]) for k in ("tp", "fp", "fn"))
            pos = tp + fn
            alerts = tp + fp
            d = {"split": split, "rule": rule, "threshold": round(float(r["threshold"]), 4), "TP": tp, "FP": fp, "FN": fn,
                 "positives": pos, "alerts": alerts, "precision": round(tp / alerts, 4), "recall": round(tp / pos, 4),
                 "missed_share": round(fn / pos, 4), "FP_per_TP": round(fp / tp, 2)}
            table.append(d)
    f1 = next(d for d in table if d["split"] == "test" and d["rule"] == "f1")
    r60 = next(d for d in table if d["split"] == "test" and d["rule"] == "recall60")
    extra_tp, extra_fp = r60["TP"] - f1["TP"], r60["FP"] - f1["FP"]
    lines.append("Counting unit: (ERA5 cell, issue hour t) in the test cohort 2024-2025, 483 cells; NOT the number of rain events.")
    for d in table:
        lines.append(f"{d['split']:<10} {d['rule']:<8} thr={d['threshold']:.4f} TP={d['TP']:>6} FP={d['FP']:>7} FN={d['FN']:>6} "
                     f"P={d['precision']:.3f} R={d['recall']:.3f} miss={d['missed_share']:.1%} FP/TP={d['FP_per_TP']}")
    lines.append(f"test: f1 -> recall60: +{extra_tp:,} TP, -{f1['FN'] - r60['FN']:,} FN, +{extra_fp:,} FP "
                 f"=> {extra_fp / extra_tp:.2f} extra FP per extra TP; alerts x{r60['alerts'] / f1['alerts']:.2f}")
    lines.append(f"test cohort rows 8,462,129 => alert rate f1 {f1['alerts'] / 8462129:.2%}, recall60 {r60['alerts'] / 8462129:.2%}")
    with open(OUT / "E2_threshold_tradeoff.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(table[0]))
        w.writeheader()
        w.writerows(table)

    # Yagi replay backtest (pilot M1, threshold 0.8478 frozen on validation 2023)
    with open(ROOT / "reports/yagi_replay_backtest.csv", encoding="utf-8") as f:
        yb = list(csv.DictReader(f))
    for loc in sorted({r["location_id"] for r in yb}) + ["ALL"]:
        sub = [r for r in yb if loc == "ALL" or r["location_id"] == loc]
        pred = [r["is_heavy_rain_predicted"] == "True" for r in sub]
        lab = [float(r["label"]) == 1.0 for r in sub]
        tp = sum(p and l for p, l in zip(pred, lab))
        fp = sum(p and not l for p, l in zip(pred, lab))
        fn = sum(l and not p for p, l in zip(pred, lab))
        lines.append(f"Yagi replay (pilot M1) {loc}: n={len(sub)} positives={sum(lab)} TP={tp} FP={fp} FN={fn}")
    (OUT / "E2_threshold_tradeoff.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
