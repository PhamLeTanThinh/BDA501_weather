"""Chọn lại ngưỡng cảnh báo cho model M4 chính thức, ưu tiên recall.

Không train lại: nạp M4 đã lưu từ lần ablation chính thức, chấm validation 2023 + test 2024–2025, lưu xác suất ra
lake (gold/m4_scores) để dùng lại. Ngưỡng chỉ được chọn trên validation 2023, rồi áp nguyên lên test:

  f1         ngưỡng F1 cao nhất (như ablation)
  recall60   ngưỡng cao nhất mà recall validation ≥ 60%
  prec30     ngưỡng có recall cao nhất mà precision validation ≥ 30%

Kèm bảng tham khảo recall/precision trên test ở vài ngưỡng cố định (không dùng để chọn).

    scripts/run_in_spark.sh src.experiments.official_threshold --profile official
"""
import argparse
import json

import numpy as np
import pandas as pd
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark
from src.experiments import metrics as M
from src.experiments.official_ablation import feature_sets


def curve(y, s):
    """recall, precision theo từng ngưỡng (giá trị điểm số phân biệt, giảm dần)."""
    o = np.argsort(-s, kind="mergesort")
    ys, ss = y[o], s[o]
    tp = np.cumsum(ys)
    k = np.r_[np.nonzero(np.diff(ss))[0], len(ss) - 1]  # vị trí cuối của mỗi mức điểm
    tp, n = tp[k], k + 1
    return ss[k], tp / max(ys.sum(), 1), tp / n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="official")
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    out = ROOT / cfg["paths"]["reports"] / "model"
    man = json.loads((out / "ablation_manifest.json").read_text(encoding="utf-8"))
    lake = path(cfg, "lake")
    dest = lake / "gold" / "m4_scores"
    if not (dest / "_SUCCESS").exists():
        spark = get_spark("official_threshold", master="local[3]", shuffle_partitions=24)
        fs = feature_sets(cfg)["M4"]
        model = RandomForestClassificationModel.load(str(lake / "models" / "official" / man["run_id"] / "M4"))
        data = spark.read.parquet(str(lake / "gold" / "ablation_input")).where("split in ('validation', 'test')")
        asm = VectorAssembler(inputCols=fs, outputCol="features")
        (model.transform(asm.transform(data))
         .select("cell", "ts_epoch", "region", "split", F.col("label").cast("byte").alias("label"),
                 vector_to_array("probability")[1].cast("float").alias("p"))
         .write.mode("overwrite").parquet(str(dest)))
        spark.stop()
        print("đã chấm và lưu gold/m4_scores", flush=True)
    sc = pd.read_parquet(str(dest), columns=["split", "label", "p"])
    val, test = sc[sc.split == "validation"], sc[sc.split == "test"]
    yv, sv = val.label.to_numpy().astype(float), val.p.to_numpy().astype(float)
    yt, st = test.label.to_numpy().astype(float), test.p.to_numpy().astype(float)

    thr, rec, prec = curve(yv, sv)
    choice = {"f1": M.best_f1_threshold(yv, sv)[0]}
    ok = rec >= 0.60
    choice["recall60"] = float(thr[ok][0]) if ok.any() else float("nan")  # ngưỡng cao nhất đạt recall ≥ 60%
    ok = prec >= 0.30
    choice["prec30"] = float(thr[ok][-1]) if ok.any() else float("nan")   # ngưỡng thấp nhất còn precision ≥ 30%

    rows = []
    for name, t in choice.items():
        if np.isnan(t):
            rows.append({"rule": name, "threshold": None})
            continue
        r = {"rule": name, "threshold": t}
        for split, y, s in (("validation", yv, sv), ("test", yt, st)):
            c = M.confusion(y, s, t)
            r.update({f"{split}_{k}": c[k] for k in ("precision", "recall", "f1", "tp", "fp", "fn")})
        rows.append(r)
    res = pd.DataFrame(rows)
    ref = []
    for t in (0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.93):
        c = M.confusion(yt, st, t)
        ref.append({"threshold": t, "test_precision": c["precision"], "test_recall": c["recall"], "test_f1": c["f1"]})
    res.to_csv(out / "m4_threshold_choice.csv", index=False)
    pd.DataFrame(ref).to_csv(out / "m4_threshold_reference_test.csv", index=False)
    print(res.to_string(index=False))
    print(pd.DataFrame(ref).to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
