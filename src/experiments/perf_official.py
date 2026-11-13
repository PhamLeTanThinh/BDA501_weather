"""Thí nghiệm hiệu năng có kiểm soát trên dữ liệu CHÍNH THỨC (ERA5 CDS, lake /data/lake).

Thí nghiệm 1 — spark.sql.shuffle.partitions ∈ {4, 16, 64, 200}
    Workload: dựng feature M4 (local_features + atmos_features: cửa sổ trượt theo ô → shuffle theo ô) cho một
    tập con cố định 60 ô × 2015–2025 (~5,8 triệu dòng silver), ghi ra sink noop (tính hết plan, không tốn I/O ghi).
    AQE tắt để Spark không tự gộp partition. 1 lần làm nóng + 2 lần đo mỗi mức.
Thí nghiệm 2 — chiến lược join: BroadcastHashJoin (mặc định) vs SortMergeJoin (tắt broadcast)
    Workload: số ngày mưa lớn theo tỉnh mới × năm trên toàn bộ gold (47,5 triệu dòng): cộng mưa theo ô-ngày
    (shuffle) rồi join bảng ô (493 dòng) để lấy tỉnh, rồi gom theo tỉnh, năm. 1 làm nóng + 3 lần đo mỗi cách.
    Lưu query plan dạng formatted cho cả hai cách.

Mỗi lần đo ghi: thời gian, số task, shuffle write/read (lấy từ REST API của Spark UI theo job group).

    scripts/run_in_spark.sh src.experiments.perf_official --profile official
    → reports/official/performance/{runs.csv, summary.csv, environment.json, plan_broadcast.txt, plan_sortmerge.txt}
"""
import argparse
import json
import os
import platform
import time
import urllib.request

import pandas as pd
from pyspark.sql import functions as F

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark
from src.features.atmos import atmos_features
from src.features.local_weather import local_features

N_CELLS = 60


def stage_metrics(spark, group):
    """Cộng số task và shuffle của mọi stage thuộc các job trong job group (REST API Spark UI)."""
    sc = spark.sparkContext
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    jobs = json.load(urllib.request.urlopen(f"{base}/jobs"))
    stages = {s for j in jobs if j.get("jobGroup") == group for s in j["stageIds"]}
    tasks = sw = sr = 0
    for st in json.load(urllib.request.urlopen(f"{base}/stages?status=complete")):
        if st["stageId"] in stages:
            tasks += st["numTasks"]
            sw += st["shuffleWriteBytes"]
            sr += st["shuffleReadBytes"]
    return tasks, sw, sr


def run(spark, exp, setting, rep, fn, rows):
    group = f"{exp}|{setting}|{rep}"
    spark.sparkContext.setJobGroup(group, group)
    t = time.perf_counter()
    fn()
    sec = time.perf_counter() - t
    time.sleep(1)  # để UI ghi xong số liệu stage
    tasks, sw, sr = stage_metrics(spark, group)
    if rep > 0:  # rep 0 = làm nóng, không tính
        rows.append({"experiment": exp, "setting": setting, "repeat": rep, "seconds": round(sec, 2), "tasks": tasks,
                     "shuffle_write_mb": round(sw / 2**20, 1), "shuffle_read_mb": round(sr / 2**20, 1)})
    print(f"{group}: {sec:.1f}s tasks={tasks} shuffle_w={sw / 2**20:.0f}MB", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="official")
    cfg = load_config(profile=ap.parse_args().profile)
    out = ROOT / cfg["paths"]["reports"] / "performance"
    out.mkdir(parents=True, exist_ok=True)
    spark = get_spark("perf_official", master="local[3]")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    lake = path(cfg, "lake")
    rows = []

    # ---- Thí nghiệm 1: số shuffle partition khi dựng feature
    gw = spark.read.parquet(str(lake / "silver" / "grid_weather"))
    cells = sorted(r.grid_id for r in spark.read.parquet(str(lake / "silver" / "cells")).select("grid_id").collect())[:N_CELLS]
    sub = gw.where(F.col("grid_id").isin(cells)).withColumnRenamed("grid_id", "location_id")
    n_rows1 = sub.count()

    def build():
        atmos_features(local_features(sub, key="location_id"), key="location_id").write.format("noop").mode("overwrite").save()

    for parts in (4, 16, 64, 200):
        spark.conf.set("spark.sql.shuffle.partitions", parts)
        for rep in range(3):
            run(spark, "E1_shuffle_partitions", parts, rep, build, rows)

    # ---- Thí nghiệm 2: broadcast vs sort-merge join
    spark.conf.set("spark.sql.shuffle.partitions", 16)
    gold = spark.read.parquet(str(lake / "gold" / "features_hourly")).select("location_id", "ts", "precipitation")
    meta = spark.read.parquet(str(lake / "silver" / "cells")).select(F.col("grid_id").alias("location_id"), "new_province")
    n_rows2 = gold.count()

    def query():
        daily = (gold.groupBy("location_id", F.to_date("ts").alias("day")).agg(F.sum("precipitation").alias("rain_day")))
        return (daily.join(meta, "location_id")
                .groupBy("new_province", F.year("day").alias("year"))
                .agg(F.sum((F.col("rain_day") > 50).cast("int")).alias("heavy_days"), F.max("rain_day").alias("max_day_mm")))

    for name, thr in (("broadcast", str(10 * 2**20)), ("sortmerge", "-1")):
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", thr)
        q = query()
        (out / f"plan_{name}.txt").write_text(q._jdf.queryExecution().explainString(
            spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")), encoding="utf-8")
        for rep in range(4):
            run(spark, "E2_join_strategy", name, rep, lambda: query().write.format("noop").mode("overwrite").save(), rows)
    result = query().orderBy(F.desc("heavy_days")).limit(5).toPandas()

    df = pd.DataFrame(rows)
    df.to_csv(out / "runs.csv", index=False)
    summ = (df.groupby(["experiment", "setting"], sort=False)
              .agg(runs=("seconds", "size"), mean_s=("seconds", "mean"), min_s=("seconds", "min"), max_s=("seconds", "max"),
                   tasks=("tasks", "max"), shuffle_write_mb=("shuffle_write_mb", "mean")).round(2).reset_index())
    summ.to_csv(out / "summary.csv", index=False)
    env = {"spark": spark.version, "master": spark.sparkContext.master, "cores": spark.sparkContext.defaultParallelism,
           "driver_memory": spark.conf.get("spark.driver.memory"), "python": platform.python_version(),
           "os": platform.platform(), "cpu_count_container": os.cpu_count(), "aqe": "off",
           "E1_input": f"silver grid_weather, {N_CELLS} cells × 2015–2025 = {n_rows1:,} rows",
           "E2_input": f"gold features_hourly, all cells = {n_rows2:,} rows", "repeats": "E1: 1 warm-up + 2; E2: 1 warm-up + 3",
           "E2_top_result": result.to_dict("records")}
    (out / "environment.json").write_text(json.dumps(env, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(summ.to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
