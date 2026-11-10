"""Thí nghiệm hiệu năng có kiểm soát.

E1  spark.sql.shuffle.partitions ∈ {4, 16, 64, 200} cho job dựng feature gold (window theo điểm +
    join ô lân cận: nhiều shuffle). Tắt AQE để số partition không bị tự gộp. 1 lần chạy làm nóng,
    sau đó 3 lần đo mỗi mức.
E2  Chiến lược join của Q2: BroadcastHashJoin (mặc định, bảng địa lý nhỏ) so với SortMergeJoin
    (tắt broadcast), 3 lần đo mỗi cách.

    python -m src.experiments.perf_experiment --profile pilot
Kết quả: reports/performance/*.csv và hình.
"""
import argparse
import json
import os
import platform
import sys
import time

import pandas as pd

from src.analytics.spark_sql_queries import QUERIES, register
from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark
from src.features.geospatial import load_geography
from src.pipeline.gold_features import build_features

REPEATS = 3


def timed(fn):
    t = time.perf_counter()
    fn()
    return time.perf_counter() - t


def run_feature_build(spark, cfg):
    lake = path(cfg, "lake")
    gw = spark.read.parquet(str(lake / "silver" / "grid_weather"))
    plan = spark.read.parquet(str(lake / "silver" / "location_grid"))
    locs = spark.read.parquet(str(lake / "silver" / "locations"))
    geo = load_geography(spark, path(cfg, "geography"))
    # noop sink: tính toàn bộ plan nhưng không tốn I/O ghi
    build_features(spark, cfg, gw, plan, locs, geo).write.format("noop").mode("overwrite").save()


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="pilot")
    cfg = load_config(profile=ap.parse_args().profile)
    out = ROOT / cfg["paths"]["reports"] / "performance"
    out.mkdir(parents=True, exist_ok=True)
    spark = get_spark("perf_experiment", driver_memory="4g")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    cores = spark.sparkContext.defaultParallelism

    rows = []
    for parts in (4, 16, 64, 200):
        spark.conf.set("spark.sql.shuffle.partitions", parts)
        timed(lambda: run_feature_build(spark, cfg))  # làm nóng
        for r in range(REPEATS):
            sec = timed(lambda: run_feature_build(spark, cfg))
            rows.append({"experiment": "shuffle_partitions", "setting": parts, "run": r + 1, "seconds": sec})
            print(f"shuffle.partitions={parts} lần {r + 1}: {sec:.1f}s", flush=True)

    spark.conf.set("spark.sql.shuffle.partitions", 16)
    register(spark, cfg)
    q2 = QUERIES["q2_heavy_days_province_year"]
    for label, thr in (("broadcast_hash_join", "10485760"), ("sort_merge_join", "-1")):
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", thr)
        df = spark.sql(q2)
        plan = df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")
        (out / f"q2_plan_{label}.txt").write_text(plan, encoding="utf-8")
        timed(lambda: spark.sql(q2).write.format("noop").mode("overwrite").save())
        for r in range(REPEATS):
            sec = timed(lambda: spark.sql(q2).write.format("noop").mode("overwrite").save())
            rows.append({"experiment": "join_strategy", "setting": label, "run": r + 1, "seconds": sec})
            print(f"{label} lần {r + 1}: {sec:.1f}s", flush=True)

    df = pd.DataFrame(rows)
    df.to_csv(out / "perf_runs.csv", index=False)
    summary = df.groupby(["experiment", "setting"], sort=False).seconds.agg(["mean", "std", "min"]).round(2)
    summary.to_csv(out / "perf_summary.csv")
    env = {"spark": spark.version, "cores_local": cores, "driver_memory": "4g", "aqe": False,
           "python": sys.version.split()[0], "platform": platform.platform(), "cpu_count": os.cpu_count(),
           "rows_gold": spark.read.parquet(str(path(cfg, "lake") / "gold" / "features_hourly")).count()}
    (out / "environment.json").write_text(json.dumps(env, indent=2), encoding="utf-8")
    print(summary)
    spark.stop()


if __name__ == "__main__":
    main()
