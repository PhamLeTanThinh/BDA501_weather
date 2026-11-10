"""E9 trên dữ liệu chính thức (ERA5 CDS, 493 ô): M4 = M1 + 9 feature khí quyển có giúp dự báo mưa lớn không?

Cùng một Random Forest (tham số trong config), cùng cohort (dòng có đủ mọi feature M1 + M4), cùng mẫu train,
cùng ngưỡng chọn trên validation 2023; chấm trên test 2024–2025 và so cặp bằng block bootstrap theo ngày.

Train trên mẫu: giữ mọi dòng dương (mưa lớn) và một phần dòng âm (--neg-frac), rồi cân bằng lớp bằng trọng số
N0/N1 như pilot. Lấy mẫu chỉ ở train; validation và test giữ nguyên toàn bộ (~13 triệu dòng) nên metric
không bị lệch. PR-AUC chỉ phụ thuộc thứ hạng xác suất, không phụ thuộc tỷ lệ lấy mẫu.

    scripts/run_in_spark.sh src.experiments.official_m4 --profile official
"""
import argparse
import json
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
from src.experiments.ablation import evaluate

KEEP = ["location_id", "region", "ts_epoch", "month", "split", "label"]


def feature_sets(cfg):
    m1 = list(cfg["features"]["M1"])
    return {"M1": m1, "M4": m1 + list(cfg["features"]["M4_add"])}


def train_sample(data, neg_frac, seed):
    """Mọi dòng dương của train + neg_frac dòng âm; trọng số lớp dương = N0/N1 trên chính mẫu này."""
    train = data.where("split = 'train'")
    sample = train.sampleBy("label", fractions={0.0: neg_frac, 1.0: 1.0}, seed=seed)
    n, pos = sample.agg(F.count("*"), F.sum("label")).first()
    w1 = float((n - pos) / pos)
    return sample.withColumn("w", F.when(F.col("label") == 1, F.lit(w1)).otherwise(F.lit(1.0))), w1, n, int(pos)


def fit(cfg, train, cols):
    m = cfg["model"]
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", weightCol="w", numTrees=m["numTrees"],
                                maxDepth=m["maxDepth"], minInstancesPerNode=m["minInstancesPerNode"],
                                featureSubsetStrategy=m["featureSubsetStrategy"], subsamplingRate=m["subsamplingRate"],
                                maxBins=m["maxBins"], seed=cfg["seed"])
    return rf.fit(VectorAssembler(inputCols=cols, outputCol="features").transform(train))


def predict(model, evalset, cols, extra=()):
    df = model.transform(VectorAssembler(inputCols=cols, outputCol="features").transform(evalset))
    return (df.select(*KEEP, *extra, vector_to_array("probability")[1].cast("float").alias("p"))
              .toPandas().sort_values(["location_id", "ts_epoch"]).reset_index(drop=True))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="official")
    ap.add_argument("--neg-frac", type=float, default=0.05, help="tỷ lệ dòng âm giữ lại khi train")
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    spark = get_spark("official_m4", master="local[4]", shuffle_partitions=32)  # RF độ sâu 12: ít luồng hơn để vừa heap
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    out = ROOT / cfg["paths"]["reports"] / "model"
    out.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_official_m4"
    sets = feature_sets(cfg)
    lake = path(cfg, "lake")

    gold = spark.read.parquet(str(lake / "gold" / "features_hourly"))
    cond = F.col("split").isNotNull() & F.col("label").isNotNull()
    for c in sets["M4"] + ["rain_24h"]:
        cond = cond & F.col(c).isNotNull()
    data = gold.where(cond).select(*KEEP, *sets["M4"])
    cohort = data.groupBy("split").agg(F.count("*").alias("rows"), F.sum("label").alias("positives")).toPandas()
    print(cohort, flush=True)

    train, w1, n_train, pos_train = train_sample(data, args.neg_frac, cfg["seed"])
    train = train.cache()
    print(f"mẫu train: {n_train:,} dòng, {pos_train:,} dương, trọng số dương {w1:.2f}", flush=True)
    evalset = data.where("split in ('validation', 'test')")

    preds, thresholds, importances = {}, {}, []
    for name, cols in sets.items():
        model = fit(cfg, train, cols)
        model.write().overwrite().save(str(lake / "models" / "official" / run_id / name))
        importances += [{"model": name, "feature": f, "importance": float(v)}
                        for f, v in zip(cols, model.featureImportances.toArray())]
        p = predict(model, evalset, cols, extra=("rain_24h",) if name == "M1" else ())
        if name == "M1":  # baseline persistence: mưa 24 giờ qua làm điểm số, cùng tập dòng
            preds["persistence_rain24h"] = p.drop(columns="p").rename(columns={"rain_24h": "p"})
            p = p.drop(columns="rain_24h")
        preds[name] = p
        val = p[p.split == "validation"]
        thresholds[name] = M.best_f1_threshold(val.label.to_numpy(), val.p.to_numpy())[0]
        print(f"{name}: {len(cols)} feature, ngưỡng validation {thresholds[name]:.4f}", flush=True)
    b = preds["persistence_rain24h"]
    thresholds["persistence_rain24h"] = M.best_f1_threshold(b[b.split == "validation"].label.to_numpy(),
                                                            b[b.split == "validation"].p.to_numpy())[0]
    key = ["location_id", "ts_epoch"]
    for k in preds:
        assert preds[k][key].equals(preds["M1"][key]), f"{k} không cùng tập dòng với M1"

    rows = []
    for k, p in preds.items():
        for split in ("validation", "test"):
            r = evaluate(p[p.split == split], thresholds[k], k)
            rows.append({**r, "split": split})
    results = pd.DataFrame(rows)

    test = (preds["M1"].split == "test").to_numpy()
    days = (pd.to_datetime(preds["M1"].loc[test, "ts_epoch"] + 7 * 3600, unit="s").dt.strftime("%Y-%m-%d")).to_numpy()
    bcfg = cfg["evaluation"]["bootstrap"]
    ci, deltas = M.block_bootstrap(preds["M1"].loc[test, "label"].to_numpy(),
                                   {k: preds[k].loc[test, "p"].to_numpy() for k in ("M1", "M4")}, days,
                                   n_resamples=bcfg["n_resamples"], seed=cfg["seed"], ci=bcfg["ci"])
    pt = {m: float(results[(results.model == m) & (results.split == "test")].pr_auc.iloc[0]) for m in ("M1", "M4")}
    (mean, lo, hi, p_le0), = deltas.values()
    delta = {"comparison": "M4 - M1", "delta_pr_auc": pt["M4"] - pt["M1"], "bootstrap_mean": mean,
             "ci_lo": lo, "ci_hi": hi, "share_resamples_le_0": p_le0,
             "pr_auc_ci": {m: list(ci[m]) for m in ci}}

    brk = []
    for k in ("M1", "M4"):
        tt = preds[k][preds[k].split == "test"]
        for g, sub in tt.groupby("region"):
            brk.append({**evaluate(sub, thresholds[k], k), "region": g})

    results.to_csv(out / "e9_results.csv", index=False)
    pd.DataFrame(brk).to_csv(out / "e9_by_region.csv", index=False)
    pd.DataFrame(importances).to_csv(out / "e9_feature_importance.csv", index=False)
    manifest = {"run_id": run_id, "created_utc": datetime.now(timezone.utc).isoformat(), "spark": spark.version,
                "python": sys.version.split()[0], "model": cfg["model"], "feature_sets": sets,
                "train_sample": {"neg_frac": args.neg_frac, "rows": n_train, "positives": pos_train,
                                 "class_weight_positive": w1},
                "cohort": cohort.to_dict("records"), "thresholds_from_validation": thresholds,
                "delta": delta, "bootstrap": bcfg, "model_dir": str(lake / "models" / "official" / run_id)}
    (out / "e9_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(results[results.split == "test"][["model", "pr_auc", "roc_auc", "precision", "recall", "f1"]].to_string(index=False))
    print(json.dumps(delta, indent=2), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
