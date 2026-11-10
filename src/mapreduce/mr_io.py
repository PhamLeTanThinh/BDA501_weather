"""Chuẩn bị input cho MapReduce và đối chiếu kết quả với Spark SQL.

    python -m src.mapreduce.mr_io export --profile pilot     # gold train → data/mr/input/*.csv
    python -m src.mapreduce.mr_io validate --profile pilot   # so data/mr/output với Spark SQL
"""
import argparse
import json
import shutil
import sys

from pyspark.sql import functions as F

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark

MR = ROOT / "data" / "mr"


def export(spark, cfg):
    gold = spark.read.parquet(str(path(cfg, "lake") / "gold" / "features_hourly"))
    out = MR / "input"
    shutil.rmtree(out, ignore_errors=True)
    # nhiều file part → nhiều input split → nhiều mapper
    (gold.where("split = 'train' AND label IS NOT NULL")
     .select("location_id", "month", F.col("label").cast("int"), "precipitation")
     .repartition(4).write.option("header", False).csv(str(out)))
    for f in out.glob("*.crc"):
        f.unlink()
    print(f"exported → {out}")


def spark_reference(spark, cfg):
    spark.read.parquet(str(path(cfg, "lake") / "gold" / "features_hourly")).createOrReplaceTempView("gold")
    return spark.sql("""
        SELECT concat(location_id, '|', lpad(cast(month AS string), 2, '0')) AS key,
               count(*) AS n, sum(cast(label AS int)) AS heavy,
               max(precipitation) AS max_p, sum(precipitation) AS sum_p
        FROM gold
        WHERE split = 'train' AND label IS NOT NULL AND precipitation IS NOT NULL
        GROUP BY location_id, month""").toPandas().set_index("key")


def validate(spark, cfg):
    mr = {}
    for f in sorted((MR / "output").glob("part-*")):
        for line in f.read_text(encoding="utf-8").splitlines():
            k, v = line.split("\t")
            n, h, rate, mean_p, max_p = v.split(",")
            mr[k] = (int(n), int(h), float(mean_p), float(max_p))
    ref = spark_reference(spark, cfg)
    mism = []
    for k, r in ref.iterrows():
        got = mr.get(k)
        exp = (int(r.n), int(r.heavy), r.sum_p / r.n, round(r.max_p, 1))
        if got is None or got[:2] != exp[:2] or abs(got[2] - exp[2]) > 1e-5 or abs(got[3] - exp[3]) > 0.051:
            mism.append({"key": k, "mapreduce": got, "spark": exp})
    summary = {"keys_spark": len(ref), "keys_mapreduce": len(mr), "missing_or_extra": len(set(ref.index) ^ set(mr)),
               "mismatches": len(mism), "rows_spark": int(ref.n.sum()), "rows_mapreduce": sum(v[0] for v in mr.values()),
               "examples": mism[:5]}
    out = ROOT / cfg["paths"]["reports"] / "mapreduce"
    out.mkdir(parents=True, exist_ok=True)
    (out / "validation.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    if mism or summary["missing_or_extra"]:
        sys.exit(1)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["export", "validate"])
    ap.add_argument("--profile", default="pilot")
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    spark = get_spark("mapreduce_io", driver_memory="2g")
    {"export": export, "validate": validate}[args.mode](spark, cfg)
    spark.stop()


if __name__ == "__main__":
    main()
