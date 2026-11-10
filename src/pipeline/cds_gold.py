"""silver/grid_weather (CDS) → gold/features_hourly: một dòng / (ô đất liền, giờ) với nhãn, split, M1 và M4.

Khóa location_id = grid_id của ô (bất biến), kèm tỉnh cũ, tỉnh mới, vùng và các điểm cũ nằm trong ô
(location_ids). Feature và nhãn dùng lại local_features / add_label / assign_split của pilot, nên định
nghĩa giống hệt bản đã kiểm thử chống rò rỉ; M4 thêm từ src.features.atmos.

Chạy theo từng lô ô (các cửa sổ đều phân vùng theo ô nên tách lô không đổi kết quả) để giới hạn bộ nhớ.

    scripts/run_in_spark.sh src.pipeline.cds_gold --profile official
"""
import argparse
import json

from pyspark.sql import functions as F

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark
from src.features.atmos import ATMOS_FEATURES, atmos_features
from src.features.local_weather import M1_FEATURES, add_label, assign_split, local_features

FEATURES = M1_FEATURES + ATMOS_FEATURES
META = ["location_id", "lat", "lon", "land_frac", "old_province", "new_province", "new_province_code",
        "region", "location_ids"]


def build(cfg, grid_weather, cells):
    cells = cells.select(F.col("grid_id").alias("location_id"), F.col("grid_lat").alias("lat"),
                         F.col("grid_lon").alias("lon"), *[c for c in META[3:]])
    wx = grid_weather.withColumnRenamed("grid_id", "location_id").join(F.broadcast(cells), "location_id")
    df = local_features(wx, key="location_id")
    df = atmos_features(df, key="location_id")
    df = add_label(df, key="location_id", horizon_hours=cfg["label"]["horizon_hours"],
                   threshold_mm=cfg["label"]["threshold_mm"])
    df = assign_split(df, cfg["splits"], horizon_hours=cfg["label"]["horizon_hours"])
    # feature lưu float32: đủ chính xác cho dữ liệu khí tượng, gold nhỏ gần một nửa (~47 triệu dòng)
    return df.select(*META, "ts", "ts_epoch", "year", F.month("ts").alias("month"), "split",
                     "split_excluded_reason", "precipitation", "rain_next_24h", "label",
                     *[F.col(c).cast("float").alias(c) for c in FEATURES])


def report(gold, out):
    out.mkdir(parents=True, exist_ok=True)
    by_split = (gold.groupBy(F.coalesce("split", F.lit("excluded")).alias("split"))
                .agg(F.count("*").alias("rows"), F.sum("label").alias("positives"),
                     F.avg("label").alias("positive_rate"),
                     F.sum(F.col("label").isNull().cast("int")).alias("label_null"),
                     *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in FEATURES])
                .toPandas())
    by_split.to_csv(out / "gold_by_split.csv", index=False)
    dup = gold.groupBy("location_id", "ts").count().where("count > 1").count()
    summary = {"rows": int(by_split.rows.sum()), "locations": gold.select("location_id").distinct().count(),
               "duplicate_keys": dup,
               "split_rows": dict(zip(by_split.split, by_split.rows.astype(int))),
               "positive_rate": dict(zip(by_split.split, by_split.positive_rate.round(5)))}
    (out / "gold_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False, default=str),
                                           encoding="utf-8")
    assert dup == 0, f"gold có {dup} khóa (location_id, ts) trùng"
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="official")
    ap.add_argument("--batches", type=int, default=12, help="số lô ô xử lý tuần tự (~40 ô, ~4 triệu dòng/lô)")
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    spark = get_spark("cds_gold", master="local[8]", shuffle_partitions=32)
    lake = path(cfg, "lake")
    gw = spark.read.parquet(str(lake / "silver" / "grid_weather"))
    cells = spark.read.parquet(str(lake / "silver" / "cells"))
    ids = sorted(r.grid_id for r in cells.select("grid_id").collect())
    dest = str(lake / "gold" / "features_hourly")
    for i in range(args.batches):
        batch = ids[i::args.batches]
        part = build(cfg, gw.where(F.col("grid_id").isin(batch)), cells.where(F.col("grid_id").isin(batch)))
        part.write.mode("overwrite" if i == 0 else "append").partitionBy("year").parquet(dest)
        print(f"lô {i + 1}/{args.batches}: {len(batch)} ô", flush=True)
    gold = spark.read.parquet(dest)
    print(json.dumps(report(gold, ROOT / cfg["paths"]["reports"] / "quality"), indent=2, ensure_ascii=False,
                     default=str))
    spark.stop()


if __name__ == "__main__":
    main()
