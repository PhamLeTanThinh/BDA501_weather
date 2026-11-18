"""Recall by cell and by province on the test years, and what drives it.

Uses the saved M4 scores (gold/m4_scores, written by official_threshold) and the two thresholds chosen on
validation 2023 (reports/official/model/m4_threshold_choice.csv). No retraining, no threshold read from test.

Outputs (reports/official/model/):
  province_recall_cells.csv       one row per cell: positives, TP/FP/FN, recall/precision at both thresholds
  province_recall_provinces.csv   one row per province: pooled cell-hour recall and province-level recall
                                  (any-area: a province-hour is heavy if >= 1 cell is heavy, alerted if >= 1 cell alerts)
  province_recall_summary.json    correlations and region table

    scripts/run_in_spark.sh src.analytics.province_recall      (pandas only; Spark not started)
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
LAKE = Path(__import__("os").environ.get("BDA_DATA", "D:/BDA501_data")) / "lake"
OUT = ROOT / "reports" / "official" / "model"


def counts(y, alert):
    tp = int((y & alert).sum()); fp = int((~y & alert).sum()); fn = int((y & ~alert).sum())
    return tp, fp, fn


def rate(a, b):
    return a / b if b else float("nan")


def spearman(a, b):
    a, b = pd.Series(a).rank(), pd.Series(b).rank()
    return float(np.corrcoef(a, b)[0, 1])


def main():
    thr = pd.read_csv(OUT / "m4_threshold_choice.csv")
    thr = thr[thr.split == "test"].set_index("rule").threshold.to_dict() if "split" in thr else thr.set_index("rule").threshold.to_dict()
    t_f1, t_rec = float(thr["f1"]), float(thr["recall60"])
    cells = pd.read_csv(ROOT / "data" / "reference" / "cells_land.csv")[["grid_id", "new_province", "region", "land_frac"]]
    clim = pd.read_csv(ROOT / "reports" / "official" / "eda" / "label_by_cell.csv")[["location_id", "positive_rate"]]
    sc = pd.read_parquet(LAKE / "gold" / "m4_scores", columns=["cell", "ts_epoch", "split", "label", "p"])
    sc = sc[sc.split == "test"].drop(columns="split")
    # ablation_input stores cell as the index of location_id in the sorted list of the 493 gold cells
    ids = sorted(pd.read_parquet(LAKE / "gold" / "features_hourly", columns=["location_id"], filters=[("year", "=", 2024)])
                 .location_id.unique())
    sc["cell"] = np.asarray(ids)[sc.cell.to_numpy()]
    sc["y"] = sc.label.astype(bool)
    sc["a1"] = sc.p >= t_f1
    sc["a2"] = sc.p >= t_rec
    sc = sc.merge(cells, left_on="cell", right_on="grid_id", how="left")

    # ---- per cell
    rows = []
    for c, g in sc.groupby("cell", sort=True):
        y = g.y.to_numpy()
        tp1, fp1, fn1 = counts(y, g.a1.to_numpy())
        tp2, fp2, fn2 = counts(y, g.a2.to_numpy())
        rows.append({"cell": c, "province": g.new_province.iat[0], "region": g.region.iat[0], "land_frac": g.land_frac.iat[0],
                     "hours": len(g), "positives": int(y.sum()), "tp_f1": tp1, "fp_f1": fp1, "fn_f1": fn1,
                     "recall_f1": rate(tp1, tp1 + fn1), "precision_f1": rate(tp1, tp1 + fp1),
                     "tp_r60": tp2, "fp_r60": fp2, "fn_r60": fn2, "recall_r60": rate(tp2, tp2 + fn2)})
    cdf = pd.DataFrame(rows).merge(clim, left_on="cell", right_on="location_id", how="left").drop(columns="location_id")
    cdf = cdf.rename(columns={"positive_rate": "heavy_rate_2015_2025"})
    cdf.to_csv(OUT / "province_recall_cells.csv", index=False)

    # ---- per province: pooled cell-hours and province-hours (any-area)
    ph = sc.groupby(["new_province", "ts_epoch"]).agg(y=("y", "max"), a1=("a1", "max"), a2=("a2", "max"),
                                                      n_pos=("y", "sum"), n_cells=("y", "size")).reset_index()
    prow = []
    for pv, g in cdf.groupby("province"):
        h = ph[ph.new_province == pv]
        y = h.y.to_numpy()
        ptp1, pfp1, pfn1 = counts(y, h.a1.to_numpy())
        ptp2, pfp2, pfn2 = counts(y, h.a2.to_numpy())
        tp, fn, fp = g.tp_f1.sum(), g.fn_f1.sum(), g.fp_f1.sum()
        prow.append({"province": pv, "region": g.region.mode().iat[0], "cells": len(g), "cell_positives": int(g.positives.sum()),
                     "cell_recall_f1": rate(tp, tp + fn), "cell_precision_f1": rate(tp, tp + fp),
                     "cell_recall_min": float(g.recall_f1.min()), "cell_recall_max": float(g.recall_f1.max()),
                     "province_hours_heavy": int(y.sum()), "prov_recall_f1": rate(ptp1, ptp1 + pfn1),
                     "prov_precision_f1": rate(ptp1, ptp1 + pfp1), "prov_recall_r60": rate(ptp2, ptp2 + pfn2),
                     "prov_precision_r60": rate(ptp2, ptp2 + pfp2),
                     "heavy_rate_cells": float(np.average(g.heavy_rate_2015_2025, weights=g.hours))})
    pdf = pd.DataFrame(prow).sort_values("cell_recall_f1")
    pdf.to_csv(OUT / "province_recall_provinces.csv", index=False)

    # ---- what drives recall
    ok = cdf[cdf.positives >= 20]
    reg = (cdf.groupby("region").agg(cells=("cell", "size"), positives=("positives", "sum"), tp=("tp_f1", "sum"), fn=("fn_f1", "sum"))
           .assign(recall_f1=lambda d: d.tp / (d.tp + d.fn)).reset_index())
    bins = pd.cut(pdf.cells, [0, 5, 10, 20, 40], labels=["1-5", "6-10", "11-20", "21-40"])
    size = (pdf.groupby(bins, observed=True).agg(provinces=("province", "size"), cell_recall=("cell_recall_f1", "mean"),
                                                 prov_recall=("prov_recall_f1", "mean"), prov_precision=("prov_precision_f1", "mean"))
            .reset_index().rename(columns={"cells": "cells_per_province"}))
    tot_tp, tot_fn = cdf.tp_f1.sum(), cdf.fn_f1.sum()
    summary = {
        "thresholds": {"f1": t_f1, "recall60": t_r if (t_r := t_rec) else None},
        "cells_scored": int(len(cdf)), "cells_in_map": 493,
        "cell_hour_recall_f1": rate(tot_tp, tot_tp + tot_fn),
        "province_hour_recall_f1_any_area": rate(int(ph.y.__and__(ph.a1).sum()), int(ph.y.sum())),
        "province_hour_precision_f1_any_area": rate(int(ph.y.__and__(ph.a1).sum()), int(ph.a1.sum())),
        "spearman_cell_recall_vs_heavy_rate": spearman(ok.recall_f1, ok.heavy_rate_2015_2025),
        "spearman_cell_recall_vs_test_positives": spearman(ok.recall_f1, ok.positives),
        "spearman_prov_recall_vs_cells": spearman(pdf.prov_recall_f1, pdf.cells),
        "spearman_prov_precision_vs_cells": spearman(pdf.prov_precision_f1, pdf.cells),
        "spearman_prov_cell_recall_vs_cells": spearman(pdf.cell_recall_f1, pdf.cells),
        "cells_with_ge20_positives": int(len(ok)),
        "cells_recall_below_25pct": int((ok.recall_f1 < 0.25).sum()),
        "cells_recall_above_60pct": int((ok.recall_f1 > 0.60).sum()),
        "share_of_missed_in_worst_10_provinces": float(pdf.assign(fn=pdf.cell_positives * (1 - pdf.cell_recall_f1))
                                                       .sort_values("fn", ascending=False).fn.head(10).sum() / tot_fn),
        "by_region": reg.to_dict("records"),
        "by_province_size": size.to_dict("records"),
    }
    # ---- cells outside the scored cohort (no 2 land neighbours → dropped by the ablation cohort)
    allc = pd.read_csv(ROOT / "data" / "reference" / "cells_land.csv")
    miss = allc[~allc.grid_id.isin(cdf.cell)][["grid_id", "new_province", "region"]]
    summary["cells_not_scored"] = miss.to_dict("records")

    # ---- regional thresholds: chosen on validation 2023 only (best F1 per region), applied to test
    from src.experiments import metrics as M
    full = pd.read_parquet(LAKE / "gold" / "m4_scores", columns=["cell", "split", "label", "p", "region"])
    full["cell"] = np.asarray(ids)[full.cell.to_numpy()]
    val, test = full[full.split == "validation"], full[full.split == "test"]
    rthr, tp, fp, fn = {}, 0, 0, 0
    for g in sorted(full.region.unique()):
        v, t = val[val.region == g], test[test.region == g]
        rthr[g] = float(M.best_f1_threshold(v.label.to_numpy().astype(float), v.p.to_numpy().astype(float))[0])
        a, b, c = counts(t.label.to_numpy().astype(bool), (t.p >= rthr[g]).to_numpy())
        tp, fp, fn = tp + a, fp + b, fn + c
        summary.setdefault("regional_thresholds", []).append(
            {"region": g, "threshold_val2023": rthr[g], "test_recall": rate(a, a + c), "test_precision": rate(a, a + b),
             "test_recall_global_thr": float(reg.set_index("region").recall_f1[g])})
    summary["regional_threshold_total"] = {"test_recall": rate(tp, tp + fn), "test_precision": rate(tp, tp + fp),
                                           "tp": tp, "fp": fp, "fn": fn,
                                           "global_f1_threshold": {"test_recall": 0.4638, "test_precision": 0.4402}}
    print(json.dumps(summary["regional_thresholds"], indent=1), json.dumps(summary["regional_threshold_total"]), miss.to_string(index=False))

    (OUT / "province_recall_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False, default=float), encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if not isinstance(v, list)}, indent=1, ensure_ascii=False, default=float))
    print(reg.to_string(index=False)); print(size.to_string(index=False))
    print(pdf[["province", "region", "cells", "cell_positives", "cell_recall_f1", "prov_recall_f1", "prov_precision_f1", "heavy_rate_cells"]]
          .to_string(index=False, float_format=lambda v: f"{v:.3f}"))


if __name__ == "__main__":
    main()
