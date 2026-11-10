"""So sánh thuật toán trên dữ liệu chính thức: Logistic Regression, Random Forest, Gradient-Boosted Trees (cùng feature M4).

Cùng cohort (gold/ablation_input), cùng mẫu train (mọi dòng dương + neg_frac dòng âm, seed cố định → đúng mẫu của
ablation), cùng trọng số lớp, cùng validation 2023 và test 2024–2025. Random Forest dùng lại model M4 đã train ở
ablation (cùng mẫu, cùng seed) thay vì train lại.

Quy tắc chọn: PR-AUC cao nhất trên VALIDATION 2023 (không nhìn test). Sau đó mới chấm test cho cả ba, kèm khoảng tin
cậy 95% của chênh lệch so với Random Forest (block bootstrap theo ngày, ghép cặp).

    scripts/run_in_spark.sh src.experiments.model_compare --profile official
    → reports/official/model/model_compare_results.csv, model_compare_deltas.csv, model_compare_manifest.json
"""
import argparse
import json
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassificationModel
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark
from src.experiments import metrics as M


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="official")
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    out = ROOT / cfg["paths"]["reports"] / "model"
    man = json.loads((out / "ablation_manifest.json").read_text(encoding="utf-8"))
    feats = man["feature_sets"]["M4"]
    lake = path(cfg, "lake")
    spark = get_spark("model_compare", master="local[3]", shuffle_partitions=24)
    spark_version = spark.version
    data = spark.read.parquet(str(lake / "gold" / "ablation_input"))

    neg_frac = man["train_sample"]["neg_frac"]
    train = data.where("split = 'train'").sampleBy("label", fractions={0.0: neg_frac, 1.0: 1.0}, seed=cfg["seed"])
    n, pos = train.agg(F.count("*"), F.sum("label")).first()
    w1 = float((n - pos) / pos)
    asm = VectorAssembler(inputCols=feats, outputCol="raw")
    train = (asm.transform(train.withColumn("w", F.when(F.col("label") == 1, F.lit(w1)).otherwise(F.lit(1.0))))
             .select("label", "w", "raw").cache())
    print(f"mẫu train {n:,} dòng, {int(pos):,} dương, trọng số {w1:.2f}", flush=True)
    # như official_ablation: cache cột gốc, ghép vector lúc chấm (cache vector 12,7 triệu dòng làm hết RAM container)
    evalset = data.where("split in ('validation', 'test')").select("cell", "ts_epoch", "split", "label", *feats).cache()
    key = (evalset.select("cell", "ts_epoch", "split", F.col("label").cast("byte").alias("label")).toPandas()
           .sort_values(["cell", "ts_epoch"]).reset_index(drop=True))
    is_test = (key.split == "test").to_numpy()
    is_val = ~is_test
    y = key.label.to_numpy().astype(float)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_model_compare"
    mdir = lake / "models" / "official" / run_id
    m = cfg["model"]
    models = {}
    t0 = time.time()
    # Logistic Regression: chuẩn hóa feature, phạt L2 nhẹ
    scaler = StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True).fit(train)
    lr = LogisticRegression(labelCol="label", featuresCol="features", weightCol="w", maxIter=100, regParam=1e-4,
                            elasticNetParam=0.0).fit(scaler.transform(train))
    lr.write().overwrite().save(str(mdir / "logistic_regression"))
    models["Logistic regression"] = (lr, scaler, round(time.time() - t0, 1))
    print(f"LR xong {time.time() - t0:.0f}s", flush=True)
    # Random Forest: model M4 của ablation (cùng mẫu train, cùng seed)
    rf = RandomForestClassificationModel.load(str(lake / "models" / "official" / man["run_id"] / "M4"))
    models["Random Forest"] = (rf, None, None)
    # Gradient-Boosted Trees
    t1 = time.time()
    gbt = GBTClassifier(labelCol="label", featuresCol="raw", weightCol="w", maxIter=100, maxDepth=6, stepSize=0.1,
                        subsamplingRate=0.8, featureSubsetStrategy="sqrt", maxBins=m["maxBins"], seed=cfg["seed"]).fit(train)
    gbt.write().overwrite().save(str(mdir / "gbt"))
    models["Gradient-boosted trees"] = (gbt, None, round(time.time() - t1, 1))
    print(f"GBT xong {time.time() - t1:.0f}s", flush=True)

    scores = {}
    for name, (model, sc, _) in models.items():
        ev = asm.transform(evalset)
        if name == "Logistic regression":
            df = sc.transform(ev)                               # cột features đã chuẩn hóa
        elif name == "Random Forest":
            df = ev.withColumnRenamed("raw", "features")        # model ablation đọc cột features
        else:
            df = ev                                             # GBT đọc cột raw
        p = (model.transform(df).select("cell", "ts_epoch", vector_to_array("probability")[1].cast("float").alias("p"))
             .toPandas().sort_values(["cell", "ts_epoch"]).reset_index(drop=True))
        assert np.array_equal(p.ts_epoch.to_numpy(), key.ts_epoch.to_numpy()), name
        scores[name] = p.p.to_numpy()
        print(f"chấm {name} xong", flush=True)
    train.unpersist()
    evalset.unpersist()
    spark.stop()

    rows = []
    for name, s in scores.items():
        thr = M.best_f1_threshold(y[is_val], s[is_val])[0]
        for split, mask in (("validation", is_val), ("test", is_test)):
            c = M.confusion(y[mask], s[mask], thr)
            rows.append({"model": name, "split": split, "pr_auc": M.average_precision(y[mask], s[mask]),
                         "roc_auc": M.roc_auc(y[mask], s[mask]), "threshold": thr, **c,
                         "train_seconds": models[name][2]})
    res = pd.DataFrame(rows)
    val = res[res.split == "validation"].set_index("model").pr_auc
    best = val.idxmax()
    days = pd.to_datetime(key.loc[is_test, "ts_epoch"] + 7 * 3600, unit="s").dt.strftime("%Y-%m-%d").to_numpy()
    ci, deltas = M.block_bootstrap(y[is_test], {k: v[is_test] for k, v in scores.items()}, days,
                                   n_resamples=cfg["evaluation"]["bootstrap"]["n_resamples"], seed=cfg["seed"],
                                   ci=cfg["evaluation"]["bootstrap"]["ci"])
    res["pr_auc_ci_lo"] = [ci[k][0] if sp == "test" else np.nan for k, sp in zip(res.model, res.split)]
    res["pr_auc_ci_hi"] = [ci[k][1] if sp == "test" else np.nan for k, sp in zip(res.model, res.split)]
    drows = [{"comparison": f"{b} - {a}", "bootstrap_mean": mean, "ci_lo": lo, "ci_hi": hi, "share_resamples_le_0": ple0}
             for (a, b), (mean, lo, hi, ple0) in deltas.items()]
    res.to_csv(out / "model_compare_results.csv", index=False)
    pd.DataFrame(drows).to_csv(out / "model_compare_deltas.csv", index=False)
    (out / "model_compare_manifest.json").write_text(json.dumps({
        "run_id": run_id, "created_utc": datetime.now(timezone.utc).isoformat(), "spark": spark_version,
        "features": "M4", "n_features": len(feats), "train_rows": int(n), "train_positives": int(pos),
        "class_weight_positive": w1, "selection_rule": "highest PR-AUC on validation 2023", "best": best,
        "models": {"Logistic regression": {"maxIter": 100, "regParam": 1e-4, "standardized": True},
                   "Random Forest": {"reused": f"{man['run_id']}/M4", **{k: m[k] for k in ("numTrees", "maxDepth", "minInstancesPerNode")}},
                   "Gradient-boosted trees": {"maxIter": 100, "maxDepth": 6, "stepSize": 0.1, "subsamplingRate": 0.8}},
        "model_dir": str(mdir)}, indent=2, ensure_ascii=False), encoding="utf-8")
    print(res[["model", "split", "pr_auc", "roc_auc", "precision", "recall", "f1", "train_seconds"]].to_string(index=False))
    print(pd.DataFrame(drows).to_string(index=False))
    print(f"chọn theo validation: {best}", flush=True)


if __name__ == "__main__":
    main()
