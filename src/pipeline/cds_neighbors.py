"""Feature ô lân cận (M3) cho bản chính thức → gold/neighbor_features (location_id = grid_id, ts_epoch).

Dùng lại định nghĩa của pilot (src.features.spatial_weather): 4 ô Bắc/Nam/Đông/Tây cách 0,25°, rolling theo
từng ô, ghép đúng giờ t, cần ít nhất min_neighbors ô hợp lệ. Ô lân cận phải nằm trong 493 ô đất liền (silver chỉ
có các ô này), nên ô ven biển/biên giới có ít hàng xóm hơn; dưới ngưỡng thì feature = null và dòng ra khỏi cohort.

Hai bước để vừa bộ nhớ: (1) rolling cho mọi ô, ghi tạm ra lake; (2) ghép hàng xóm theo từng lô ô.

    scripts/run_in_spark.sh src.pipeline.cds_neighbors --profile official
"""
import argparse
import json

from pyspark.sql import functions as F

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark
from src.features.spatial_weather import NEIGHBOR_FEATURES, OFFSETS, grid_id, grid_rolling


def neighbor_plan(cells):
    """(location_id, grid_id lân cận) cho các ô lân cận N/S/E/W có trong tập ô đất liền."""
    ids = {r["grid_id"]: (r["grid_lat"], r["grid_lon"]) for r in cells}
    return [(g, grid_id(la + di * 0.25, lo + dj * 0.25)) for g, (la, lo) in ids.items()
            for di, dj in OFFSETS["nsew"] if grid_id(la + di * 0.25, lo + dj * 0.25) in ids]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="official")
    ap.add_argument("--batches", type=int, default=6)
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    spark = get_spark("cds_neighbors", master="local[6]", shuffle_partitions=48)
    lake = path(cfg, "lake")
    gw = spark.read.parquet(str(lake / "silver" / "grid_weather"))
    cells = [r.asDict() for r in spark.read.parquet(str(lake / "silver" / "cells")).select("grid_id", "grid_lat", "grid_lon").collect()]
    plan = neighbor_plan(cells)
    counts = {}
    for loc, _ in plan:
        counts[loc] = counts.get(loc, 0) + 1
    print(f"{len(plan)} cặp ô–hàng xóm; số ô có ≥ {cfg['neighbors']['min_neighbors']} hàng xóm: "
          f"{sum(c >= cfg['neighbors']['min_neighbors'] for c in counts.values())}/{len(cells)}", flush=True)

    rolled_dir = str(lake / "tmp" / "grid_rolled")
    grid_rolling(gw).write.mode("overwrite").parquet(rolled_dir)
    rolled = spark.read.parquet(rolled_dir)
    plan_df = spark.createDataFrame(plan, ["location_id", "grid_id"])
    locs = sorted(counts)
    dest = str(lake / "gold" / "neighbor_features")
    for i in range(args.batches):
        batch = plan_df.where(F.col("location_id").isin(locs[i::args.batches]))
        agg = (batch.join(rolled, "grid_id").groupBy("location_id", "ts_epoch")
               .agg(F.avg("g_rain_6h").alias("neighbor_rain_sum_6h"), F.avg("g_rain_24h").alias("neighbor_rain_sum_24h"),
                    F.avg("g_pressure_change_24h").alias("neighbor_pressure_change_24h"),
                    F.avg("g_rh_mean_6h").alias("neighbor_humidity_mean_6h"),
                    F.sum(F.when(F.col("g_rain_24h").isNotNull() & F.col("g_rain_6h").isNotNull()
                                 & F.col("g_pressure_change_24h").isNotNull() & F.col("g_rh_mean_6h").isNotNull(), 1)
                          .otherwise(0)).alias("neighbor_count")))
        ok = F.col("neighbor_count") >= cfg["neighbors"]["min_neighbors"]
        (agg.select("location_id", "ts_epoch", "neighbor_count",
                    *[F.when(ok, F.col(c)).cast("float").alias(c) for c in NEIGHBOR_FEATURES])
            .write.mode("overwrite" if i == 0 else "append").parquet(dest))
        print(f"lô {i + 1}/{args.batches}", flush=True)
    nb = spark.read.parquet(dest)
    summary = {"rows": nb.count(), "cells_with_neighbours": len(counts),
               "rows_with_features": nb.where(F.col(NEIGHBOR_FEATURES[0]).isNotNull()).count()}
    (ROOT / cfg["paths"]["reports"] / "quality" / "neighbor_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(summary, flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
