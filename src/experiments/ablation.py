"""Ablation M1 / M2 / M3: thêm feature địa lý và khí tượng lân cận có giúp dự báo mưa lớn 24h không?

Thí nghiệm có kiểm soát: cùng cohort (dòng có đủ feature của M3, nhãn hợp lệ, không bị purge),
cùng split, cùng RandomForest (siêu tham số, seed, trọng số lớp), cùng quy trình chọn ngưỡng
(trên validation 2023, khóa trước khi đánh giá test 2024–2025). Chỉ khác tập feature.

    python -m src.experiments.ablation --profile pilot
Kết quả: reports/ablation_results.csv, ablation_report.md, figures/, manifest.
"""
import argparse
import hashlib
import json
import platform
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark
from src.experiments import metrics as M
from src.experiments.report import write_report
from src.features.geospatial import GEO_FEATURES, join_geography, load_geography
from src.features.spatial_weather import NEIGHBOR_FEATURES

EXPERIMENTS = ("M1", "M2", "M3")
KEEP = ["location_id", "region", "new_province_code", "ts", "month", "split", "label"]


def feature_sets(cfg):
    m1 = list(cfg["features"]["M1"])
    m2 = m1 + list(cfg["features"]["M2_add"])
    m3 = m2 + list(cfg["features"]["M3_add"])
    # M1 không được chứa sẵn feature địa lý/lân cận, nếu không M2/M3 không đo được phần đóng góp thêm
    overlap = set(m1) & (set(GEO_FEATURES) | set(NEIGHBOR_FEATURES))
    if overlap:
        raise ValueError(f"M1 đã chứa feature của nhóm sau: {sorted(overlap)}")
    if len(set(m3)) != len(m3):
        raise ValueError("Có feature bị liệt kê hai lần")
    return {"M1": m1, "M2": m2, "M3": m3}


def cohort_report(gold, sets):
    """Số dòng hợp lệ của từng thí nghiệm nếu mỗi cái dùng cohort riêng, so với cohort chung."""
    base = gold.where(F.col("split").isNotNull() & F.col("label").isNotNull())
    exprs = []
    for name, cols in sets.items():
        ok = F.lit(True)
        for c in cols:
            ok = ok & F.col(c).isNotNull()
        exprs += [F.sum(ok.cast("int")).alias(f"{name}_eligible"),
                  F.sum((ok & (F.col("label") == 1)).cast("int")).alias(f"{name}_positives")]
    return base.groupBy("split").agg(F.count("*").alias("labelled_rows"), *exprs).toPandas()


def common_cohort(gold, cols):
    cond = F.col("split").isNotNull() & F.col("label").isNotNull()
    for c in cols:
        cond = cond & F.col(c).isNotNull()
    return gold.where(cond)


def with_class_weight(train):
    """Cột w: lớp dương nhận N0/N1 tính trên chính tập train truyền vào, lớp âm nhận 1."""
    n, pos = train.agg(F.count("*"), F.sum("label")).first()
    w1 = float((n - pos) / pos)
    return train.withColumn("w", F.when(F.col("label") == 1, F.lit(w1)).otherwise(F.lit(1.0))), w1


def fit_predict(cfg, train, evalset, cols):
    """RF cấu hình cố định từ config; trả về model và xác suất lớp dương trên evalset (pandas)."""
    mcfg = cfg["model"]
    asm = VectorAssembler(inputCols=cols, outputCol="features", handleInvalid="error")
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", weightCol="w",
                                numTrees=mcfg["numTrees"], maxDepth=mcfg["maxDepth"],
                                minInstancesPerNode=mcfg["minInstancesPerNode"],
                                featureSubsetStrategy=mcfg["featureSubsetStrategy"],
                                subsamplingRate=mcfg["subsamplingRate"], maxBins=mcfg["maxBins"],
                                seed=cfg["seed"])
    model = rf.fit(asm.transform(train))
    p = (model.transform(asm.transform(evalset))
         .select(*KEEP, vector_to_array("probability")[1].alias("p")).toPandas())
    return model, p


def leave_location_out(cfg, data, sets):
    """Mỗi fold giữ riêng một điểm: train + chọn ngưỡng trên các điểm còn lại (2019–2023),
    chấm test 2024–2025 trên điểm bị giữ lại. Đo khả năng tổng quát sang nơi model chưa từng thấy,
    nên feature địa lý không thể chỉ đóng vai trò mã nhận diện điểm."""
    locs = sorted(r[0] for r in data.select("location_id").distinct().collect())
    out = {m: [] for m in EXPERIMENTS}
    for i, loc in enumerate(locs, 1):
        train, _ = with_class_weight(data.where((F.col("split") == "train") & (F.col("location_id") != loc)))
        val_others = data.where((F.col("split") == "validation") & (F.col("location_id") != loc))
        test_loc = data.where((F.col("split") == "test") & (F.col("location_id") == loc))
        for m in EXPERIMENTS:
            _, p = fit_predict(cfg, train, val_others.unionByName(test_loc), sets[m])
            val = p[p.split == "validation"]
            thr = M.best_f1_threshold(val["label"].to_numpy(), val["p"].to_numpy())[0]
            out[m].append(p[p.split == "test"].assign(threshold=thr, fold=loc))
        print(f"LOLO {i}/{len(locs)}: giữ riêng {loc}", flush=True)
    key = ["location_id", "ts"]
    return {m: pd.concat(v).sort_values(key).reset_index(drop=True) for m, v in out.items()}


def summarize_lolo(cfg, lolo):
    y = lolo["M1"]["label"].to_numpy()
    rows = []
    for m, p in lolo.items():
        assert p[["location_id", "ts"]].equals(lolo["M1"][["location_id", "ts"]])
        r = evaluate(p, p["threshold"].to_numpy(), m)  # ngưỡng của từng fold, đã khóa trước test
        r["threshold"] = float("nan")
        rows.append(r)
    days = pd.to_datetime(lolo["M1"]["ts"]).dt.strftime("%Y-%m-%d").to_numpy()
    bcfg = cfg["evaluation"]["bootstrap"]
    ci, deltas = M.block_bootstrap(y, {m: lolo[m]["p"].to_numpy() for m in EXPERIMENTS}, days,
                                   n_resamples=bcfg["n_resamples"], seed=cfg["seed"], ci=bcfg["ci"])
    res = pd.DataFrame(rows)
    res["pr_auc_ci_lo"] = [ci[m][0] for m in res.model]
    res["pr_auc_ci_hi"] = [ci[m][1] for m in res.model]
    point = dict(zip(res.model, res.pr_auc))
    d = pd.DataFrame([{"comparison": f"{b} - {a}", "delta_pr_auc": point[b] - point[a], "bootstrap_mean": mean,
                       "ci_lo": lo, "ci_hi": hi, "share_resamples_le_0": p0}
                      for (a, b), (mean, lo, hi, p0) in deltas.items()])
    per_loc = pd.DataFrame([{"location_id": loc, "model": m, "positives": int(g.label.sum()),
                             "pr_auc": M.average_precision(g.label.to_numpy(), g.p.to_numpy())}
                            for m, p in lolo.items() for loc, g in p.groupby("location_id")])
    return res, d, per_loc


def season(region, month, rainy):
    return "rainy" if month in rainy.get(region, []) else "dry"


def evaluate(pred, thr, name):
    y, s = pred["label"].to_numpy(), pred["p"].to_numpy()
    c = M.confusion(y, s, thr)
    return {"model": name, "n": len(y), "positives": int(y.sum()), "prevalence": float(y.mean()),
            "pr_auc": M.average_precision(y, s), "roc_auc": M.roc_auc(y, s), "threshold": thr, **c}


def run(cfg, spark, with_lolo=True):
    lake = path(cfg, "lake")
    out = ROOT / cfg["paths"]["reports"]
    fig_dir = out / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"_{cfg['profile']}"
    sets = feature_sets(cfg)

    gold = spark.read.parquet(str(lake / "gold" / "features_hourly"))
    # bảng địa lý bắt buộc đầy đủ cho M2/M3: thiếu thì dừng, không để RF tự lấp
    join_geography(gold.select("location_id").distinct(), load_geography(spark, path(cfg, "geography")))
    cohorts = cohort_report(gold, sets)
    data = common_cohort(gold, sets["M3"]).select(*KEEP, *sets["M3"]).cache()

    counts = data.groupBy("split").agg(F.count("*").alias("n"), F.sum("label").alias("pos")).toPandas()
    counts = counts.set_index("split")
    train, w1 = with_class_weight(data.where("split = 'train'"))
    evalset = data.where("split in ('validation', 'test')")

    mcfg = cfg["model"]
    preds, importances, thresholds = {}, [], {}
    for name in EXPERIMENTS:
        cols = sets[name]
        model, p = fit_predict(cfg, train, evalset, cols)
        model.write().overwrite().save(str(lake / "models" / "ablation" / run_id / name))
        preds[name] = p
        importances += [{"model": name, "feature": f, "importance": float(v)}
                        for f, v in zip(cols, model.featureImportances.toArray())]
        val = p[p.split == "validation"]
        thresholds[name] = M.best_f1_threshold(val["label"].to_numpy(), val["p"].to_numpy())[0]
        print(f"{name}: {len(cols)} feature, ngưỡng validation = {thresholds[name]:.4f}", flush=True)

    # baseline tham khảo trên cùng cohort (không thuộc ablation)
    base = evalset.select(*KEEP, "rain_24h").toPandas()
    clim = (train.groupBy("location_id", "month").agg(F.avg("label").alias("clim")).toPandas())
    base = base.merge(clim, on=["location_id", "month"], how="left").fillna({"clim": 0.0})
    for name, col in (("persistence_rain24h", "rain_24h"), ("climatology_loc_month", "clim")):
        b = base[KEEP].assign(p=base[col].to_numpy())
        preds[name] = b
        val = b[b.split == "validation"]
        thresholds[name] = M.best_f1_threshold(val["label"].to_numpy(), val["p"].to_numpy())[0]

    # sắp các dataframe dự đoán theo cùng thứ tự để so sánh cặp
    key = ["location_id", "ts"]
    for k in preds:
        preds[k] = preds[k].sort_values(key).reset_index(drop=True)
    ref = preds["M1"][key]
    for k in preds:
        assert preds[k][key].equals(ref), f"{k} không cùng tập dòng với M1"

    rows = []
    for k, p in preds.items():
        for split in ("validation", "test"):
            r = evaluate(p[p.split == split], thresholds[k], k)
            r.update(split=split, role="ablation" if k in EXPERIMENTS else "reference_baseline",
                     n_features=len(sets[k]) if k in EXPERIMENTS else 1)
            rows.append(r)
    results = pd.DataFrame(rows)

    test_mask = (preds["M1"].split == "test").to_numpy()
    y_test = preds["M1"].loc[test_mask, "label"].to_numpy()
    days = pd.to_datetime(preds["M1"].loc[test_mask, "ts"]).dt.strftime("%Y-%m-%d").to_numpy()
    bcfg = cfg["evaluation"]["bootstrap"]
    ci, deltas = M.block_bootstrap(y_test, {k: preds[k].loc[test_mask, "p"].to_numpy() for k in EXPERIMENTS},
                                   days, n_resamples=bcfg["n_resamples"], seed=cfg["seed"], ci=bcfg["ci"])
    results["pr_auc_ci_lo"] = [ci.get(m, (np.nan,))[0] if s == "test" else np.nan
                               for m, s in zip(results.model, results.split)]
    results["pr_auc_ci_hi"] = [ci.get(m, (np.nan, np.nan))[1] if s == "test" else np.nan
                               for m, s in zip(results.model, results.split)]
    point = {m: results[(results.model == m) & (results.split == "test")].pr_auc.iloc[0] for m in EXPERIMENTS}
    delta_rows = []
    for (a, b), (mean, lo, hi, p_le0) in deltas.items():
        delta_rows.append({"comparison": f"{b} - {a}", "delta_pr_auc": point[b] - point[a],
                           "bootstrap_mean": mean, "ci_lo": lo, "ci_hi": hi, "share_resamples_le_0": p_le0,
                           "delta_recall": _get(results, b, "recall") - _get(results, a, "recall"),
                           "delta_precision": _get(results, b, "precision") - _get(results, a, "precision"),
                           "delta_f1": _get(results, b, "f1") - _get(results, a, "f1")})
    deltas_df = pd.DataFrame(delta_rows)

    rainy = cfg["evaluation"]["rainy_season_months"]
    brk = []
    for k in EXPERIMENTS:
        t = preds[k][preds[k].split == "test"].copy()
        t["season"] = [season(r, m, rainy) for r, m in zip(t.region, t.month)]
        for dim in ("region", "season"):
            for g, sub in t.groupby(dim):
                r = evaluate(sub, thresholds[k], k)
                r.update(dimension=dim, group=g)
                brk.append(r)
    breakdown = pd.DataFrame(brk)

    results.to_csv(out / "ablation_results.csv", index=False)
    deltas_df.to_csv(out / "ablation_deltas.csv", index=False)
    breakdown.to_csv(out / "ablation_breakdown.csv", index=False)
    cohorts.to_csv(out / "ablation_cohort.csv", index=False)
    imp = pd.DataFrame(importances)
    imp.to_csv(out / "feature_importance.csv", index=False)
    pd.concat([p.assign(model=k) for k, p in preds.items()]).to_parquet(
        out / "figures" / "_predictions_eval.parquet", index=False)

    manifest = {
        "run_id": run_id, "profile": cfg["profile"], "years": cfg["years"], "seed": cfg["seed"],
        "created_utc": datetime.now(timezone.utc).isoformat(), "python": sys.version.split()[0],
        "platform": platform.platform(), "spark": spark.version, "model": mcfg,
        "class_weight_positive": w1, "splits": cfg["splits"], "label": cfg["label"],
        "feature_sets": sets, "thresholds_from_validation": thresholds,
        "cohort_counts": counts.reset_index().to_dict("records"),
        "locations": sorted(preds["M1"].location_id.unique().tolist()),
        "config_sha256": hashlib.sha256((ROOT / "configs" / "ablation.yaml").read_bytes()).hexdigest(),
        "geography_sha256": hashlib.sha256(path(cfg, "geography").read_bytes()).hexdigest(),
        "grid_plan_sha256": hashlib.sha256(path(cfg, "grid_plan").read_bytes()).hexdigest(),
        "bootstrap": bcfg, "model_dir": str(lake / "models" / "ablation" / run_id),
    }
    (out / "ablation_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False, default=str),
                                                encoding="utf-8")
    lolo = None
    if with_lolo:
        lolo = summarize_lolo(cfg, leave_location_out(cfg, data, sets))
        for name, df in zip(("results", "deltas", "per_location"), lolo):
            df.to_csv(out / f"ablation_lolo_{name}.csv", index=False)
    write_report(cfg, out, results, deltas_df, breakdown, cohorts, imp, preds, thresholds, manifest, lolo)
    return results, deltas_df


def _get(results, model, col):
    return results[(results.model == model) & (results.split == "test")][col].iloc[0]


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="pilot")
    ap.add_argument("--skip-lolo", action="store_true", help="bỏ đánh giá leave-location-out")
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    spark = get_spark("ablation", driver_memory="5g")
    results, deltas = run(cfg, spark, with_lolo=not args.skip_lolo)
    print(results[results.split == "test"][["model", "pr_auc", "recall", "precision", "f1", "threshold"]])
    print(deltas)
    spark.stop()


if __name__ == "__main__":
    main()
