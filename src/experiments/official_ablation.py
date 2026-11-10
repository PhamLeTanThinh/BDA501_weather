"""Ablation đầy đủ trên dữ liệu chính thức (ERA5 CDS, 493 ô đất liền): M1, M2, M3, M4 trên cùng cohort.

  M1 = 24 feature thời tiết tại ô          M2 = M1 + địa hình (độ cao, cách biển, độ dốc tại tâm ô)
  M3 = M2 + 4 feature ô lân cận            M4 = M1 + 9 feature khí quyển (6 biến cột CDS)

Cùng Random Forest (tham số trong config), cùng cohort (dòng có đủ mọi feature của cả bốn bộ), cùng mẫu train,
ngưỡng chọn trên validation 2023, chấm trên test 2024–2025, so từng bộ với M1 bằng block bootstrap theo ngày.

Train trên mẫu: mọi dòng dương + --neg-frac dòng âm, trọng số lớp dương N0/N1 trên mẫu. Validation và test giữ
nguyên. Dự đoán chỉ đưa về driver các cột số (chỉ số ô, giờ, nhãn, split, xác suất) để vừa bộ nhớ.

    scripts/run_in_spark.sh src.experiments.official_ablation --profile official
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
from src.features.geospatial import GEO_FEATURES
from src.features.spatial_weather import NEIGHBOR_FEATURES

SPLIT = {"train": 0, "validation": 1, "test": 2}


def feature_sets(cfg):
    m1 = list(cfg["features"]["M1"])
    return {"M1": m1, "M2": m1 + GEO_FEATURES, "M3": m1 + GEO_FEATURES + NEIGHBOR_FEATURES,
            "M4": m1 + list(cfg["features"]["M4_add"])}


def load_data(spark, cfg, sets, batches=8):
    """Gold + địa hình + ô lân cận, chỉ giữ dòng đủ mọi feature. Ghép theo lô ô rồi ghi ra lake
    (gold/ablation_input) vì ghép 47 triệu × 47 triệu dòng một lần không vừa bộ nhớ container."""
    lake = path(cfg, "lake")
    dest = lake / "gold" / "ablation_input"
    cols = sorted({c for s in sets.values() for c in s})
    gold = spark.read.parquet(str(lake / "gold" / "features_hourly"))
    cells = sorted(r[0] for r in gold.select("location_id").distinct().collect())
    if not (dest / "_SUCCESS").exists():
        geo = (spark.read.csv(str(path(cfg, "geography")), header=True, inferSchema=True)
               .select("location_id", *[F.col(c).cast("float").alias(c) for c in GEO_FEATURES]))
        nb = spark.read.parquet(str(lake / "gold" / "neighbor_features")).select("location_id", "ts_epoch", *NEIGHBOR_FEATURES)
        idx = spark.createDataFrame([(c, i) for i, c in enumerate(cells)], ["location_id", "cell"])
        for i in range(batches):
            part = cells[i::batches]
            df = (gold.where(F.col("location_id").isin(part))
                  .join(nb.where(F.col("location_id").isin(part)), ["location_id", "ts_epoch"], "left")
                  .join(F.broadcast(geo), "location_id", "left").join(F.broadcast(idx), "location_id"))
            cond = F.col("split").isNotNull() & F.col("label").isNotNull()
            for c in cols:
                cond = cond & F.col(c).isNotNull()
            (df.where(cond).select("cell", "region", "ts_epoch", "split", "label", *cols)
               .write.mode("overwrite" if i == 0 else "append").parquet(str(dest)))
            print(f"ghép lô {i + 1}/{batches}", flush=True)
    return spark.read.parquet(str(dest)), cells, cols


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="official")
    ap.add_argument("--neg-frac", type=float, default=0.03)
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    spark = get_spark("official_ablation", master="local[3]", shuffle_partitions=24)
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark_version = spark.version  # đọc sớm: JVM có thể bị container dừng khi bootstrap numpy chiếm bộ nhớ
    out = ROOT / cfg["paths"]["reports"] / "model"
    out.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_official_ablation"
    sets = feature_sets(cfg)
    data, cells, cols = load_data(spark, cfg, sets)
    cohort = data.groupBy("split").agg(F.count("*").alias("rows"), F.sum("label").alias("positives")).toPandas()
    print(cohort, flush=True)

    train = data.where("split = 'train'").sampleBy("label", fractions={0.0: args.neg_frac, 1.0: 1.0}, seed=cfg["seed"])
    n, pos = train.agg(F.count("*"), F.sum("label")).first()
    w1 = float((n - pos) / pos)
    train = train.withColumn("w", F.when(F.col("label") == 1, F.lit(w1)).otherwise(F.lit(1.0))).cache()
    print(f"mẫu train: {n:,} dòng, {int(pos):,} dương, trọng số dương {w1:.2f}", flush=True)
    evalset = (data.where("split in ('validation', 'test')")
               .withColumn("split_code", F.when(F.col("split") == "test", 2).otherwise(1).cast("byte")).cache())

    key = evalset.select("cell", "ts_epoch", "split_code", F.col("label").cast("byte").alias("label"), "region",
                         F.col("rain_24h").alias("persistence")).toPandas().sort_values(["cell", "ts_epoch"]).reset_index(drop=True)
    is_test = (key.split_code == 2).to_numpy()
    is_val = ~is_test
    y = key.label.to_numpy().astype(float)
    scores, thresholds, imp = {"persistence_rain24h": key.persistence.to_numpy()}, {}, []
    m = cfg["model"]
    for name, fs in sets.items():
        rf = RandomForestClassifier(labelCol="label", featuresCol="features", weightCol="w", numTrees=m["numTrees"],
                                    maxDepth=m["maxDepth"], minInstancesPerNode=m["minInstancesPerNode"],
                                    featureSubsetStrategy=m["featureSubsetStrategy"], subsamplingRate=m["subsamplingRate"],
                                    maxBins=m["maxBins"], seed=cfg["seed"])
        asm = VectorAssembler(inputCols=fs, outputCol="features")
        model = rf.fit(asm.transform(train))
        model.write().overwrite().save(str(path(cfg, "lake") / "models" / "official" / run_id / name))
        imp += [{"model": name, "feature": f, "importance": float(v)} for f, v in zip(fs, model.featureImportances.toArray())]
        p = (model.transform(asm.transform(evalset)).select("cell", "ts_epoch", vector_to_array("probability")[1].cast("float").alias("p"))
             .toPandas().sort_values(["cell", "ts_epoch"]).reset_index(drop=True))
        assert np.array_equal(p.ts_epoch.to_numpy(), key.ts_epoch.to_numpy()), f"{name}: lệch thứ tự dòng"
        scores[name] = p.p.to_numpy()
        del p
        print(f"{name}: {len(fs)} feature xong", flush=True)
    train.unpersist()
    evalset.unpersist()
    spark.stop()  # mọi dự đoán đã ở driver; giải phóng JVM trước khi bootstrap trên ~8,5 triệu dòng
    for k, s in scores.items():
        thresholds[k] = M.best_f1_threshold(y[is_val], s[is_val])[0]

    rows = []
    for k, s in scores.items():
        for split, mask in (("validation", is_val), ("test", is_test)):
            c = M.confusion(y[mask], s[mask], thresholds[k])
            rows.append({"model": k, "split": split, "n": int(mask.sum()), "positives": int(y[mask].sum()),
                         "pr_auc": M.average_precision(y[mask], s[mask]), "roc_auc": M.roc_auc(y[mask], s[mask]),
                         "threshold": thresholds[k], **c})
    results = pd.DataFrame(rows)

    days = pd.to_datetime(key.loc[is_test, "ts_epoch"] + 7 * 3600, unit="s").dt.strftime("%Y-%m-%d").to_numpy()
    bcfg = cfg["evaluation"]["bootstrap"]
    ci, deltas = M.block_bootstrap(y[is_test], {k: scores[k][is_test] for k in sets}, days,
                                   n_resamples=bcfg["n_resamples"], seed=cfg["seed"], ci=bcfg["ci"])
    pt = {k: float(results[(results.model == k) & (results.split == "test")].pr_auc.iloc[0]) for k in sets}
    drows = [{"comparison": f"{b} - {a}", "delta_pr_auc": pt[b] - pt[a], "bootstrap_mean": mean, "ci_lo": lo, "ci_hi": hi,
              "share_resamples_le_0": ple0} for (a, b), (mean, lo, hi, ple0) in deltas.items()]
    deltas_df = pd.DataFrame(drows)
    results["pr_auc_ci_lo"] = [ci[k][0] if (k in ci and sp == "test") else np.nan for k, sp in zip(results.model, results.split)]
    results["pr_auc_ci_hi"] = [ci[k][1] if (k in ci and sp == "test") else np.nan for k, sp in zip(results.model, results.split)]

    brk = []
    test_key = key[is_test]
    for k in sets:
        st = scores[k][is_test]
        for g in sorted(test_key.region.unique()):
            mask = (test_key.region == g).to_numpy()
            brk.append({"model": k, "region": g, "positives": int(y[is_test][mask].sum()),
                        "pr_auc": M.average_precision(y[is_test][mask], st[mask])})

    results.to_csv(out / "ablation_results.csv", index=False)
    deltas_df.to_csv(out / "ablation_deltas.csv", index=False)
    pd.DataFrame(brk).to_csv(out / "ablation_by_region.csv", index=False)
    pd.DataFrame(imp).to_csv(out / "feature_importance.csv", index=False)
    manifest = {"run_id": run_id, "created_utc": datetime.now(timezone.utc).isoformat(), "spark": spark_version,
                "python": sys.version.split()[0], "model": m, "feature_sets": sets, "cells": len(cells),
                "train_sample": {"neg_frac": args.neg_frac, "rows": int(n), "positives": int(pos), "class_weight_positive": w1},
                "cohort": cohort.to_dict("records"), "thresholds_from_validation": thresholds, "bootstrap": bcfg,
                "model_dir": str(path(cfg, "lake") / "models" / "official" / run_id)}
    (out / "ablation_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(results[results.split == "test"][["model", "pr_auc", "roc_auc", "precision", "recall", "f1"]].to_string(index=False))
    print(deltas_df.to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
