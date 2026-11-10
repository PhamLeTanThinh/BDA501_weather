"""Silver → gold/features_hourly: một dòng / (điểm, giờ) với nhãn, split và đủ feature cho M1, M2, M3.

Thời tiết lấy từ ô trung tâm của từng điểm; mỗi điểm giữ nguyên location_id, tọa độ, tỉnh cũ và
tỉnh mới. Không gộp chuỗi thời tiết của các điểm thuộc cùng tỉnh mới.

    python -m src.pipeline.gold_features --profile pilot
"""
import argparse
import json

from pyspark.sql import functions as F

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark
from src.features.geospatial import GEO_FEATURES, join_geography, load_geography
from src.features.local_weather import M1_FEATURES, add_label, assign_split, local_features
from src.features.spatial_weather import NEIGHBOR_FEATURES, neighbor_features

ALL_FEATURES = M1_FEATURES + GEO_FEATURES + NEIGHBOR_FEATURES


def point_features(cfg, grid_weather, plan, locations, geo, required_geo=False):
    """Feature M1 + M2 + M3 cho từng (điểm, giờ) từ bảng thời tiết theo ô lưới.

    Dùng chung cho gold (toàn bộ lịch sử) và streaming (bộ đệm vài ngày gần nhất): cùng một hàm thì
    cùng định nghĩa feature giữa train và serve.
    """
    center = plan.where(F.col("role") == "center").select("location_id", F.col("grid_id").alias("center_grid_id"))
    loc = locations.select("location_id", "old_province", "new_province", "new_province_code", "region",
                           "lat", "lon").join(center, "location_id")
    wx = loc.join(grid_weather.withColumnRenamed("grid_id", "center_grid_id"), "center_grid_id")
    df = local_features(wx, key="location_id")
    nb = neighbor_features(grid_weather, plan, min_neighbors=cfg["neighbors"]["min_neighbors"])
    df = df.join(nb, ["location_id", "ts"], "left")
    return join_geography(df, geo, required=required_geo)


def build_features(spark, cfg, grid_weather, plan, locations, geo):
    df = point_features(cfg, grid_weather, plan, locations, geo)
    df = add_label(df, key="location_id", horizon_hours=cfg["label"]["horizon_hours"],
                   threshold_mm=cfg["label"]["threshold_mm"])
    df = assign_split(df, cfg["splits"], horizon_hours=cfg["label"]["horizon_hours"])
    return df.select("location_id", "old_province", "new_province", "new_province_code", "region",
                     "lat", "lon", "center_grid_id", "ts", "ts_epoch", F.year("ts").alias("year"),
                     F.month("ts").alias("month"), "split", "split_excluded_reason",
                     "precipitation", "rain_next_24h", "label", "neighbor_count", *ALL_FEATURES)


def missingness_report(gold, out):
    """Tỷ lệ null của từng feature theo split: cho thấy M1/M2/M3 mất bao nhiêu dòng vì thiếu dữ liệu."""
    rows = (gold.groupBy(F.coalesce("split", F.lit("excluded")).alias("split"))
            .agg(F.count("*").alias("rows"), F.sum(F.col("label").isNull().cast("int")).alias("label_null"),
                 *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ALL_FEATURES])
            .toPandas().set_index("split").T)
    rows.to_csv(out / "gold_missingness_by_split.csv")
    excl = gold.groupBy("split_excluded_reason").count().toPandas()
    excl.to_csv(out / "gold_split_exclusions.csv", index=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="pilot")
    cfg = load_config(profile=ap.parse_args().profile)
    spark = get_spark("gold_features")
    lake = path(cfg, "lake")
    gw = spark.read.parquet(str(lake / "silver" / "grid_weather"))
    plan = spark.read.parquet(str(lake / "silver" / "location_grid"))
    locs = spark.read.parquet(str(lake / "silver" / "locations"))
    geo = load_geography(spark, path(cfg, "geography"))
    gold = build_features(spark, cfg, gw, plan, locs, geo)
    gold.write.mode("overwrite").partitionBy("year").parquet(str(lake / "gold" / "features_hourly"))
    gold = spark.read.parquet(str(lake / "gold" / "features_hourly"))
    dup = gold.groupBy("location_id", "ts").count().where("count > 1").count()
    assert dup == 0, f"gold có {dup} khóa (location_id, ts) trùng"
    out = ROOT / cfg["paths"]["reports"] / "quality"
    out.mkdir(parents=True, exist_ok=True)
    missingness_report(gold, out)
    (out / "gold_summary.json").write_text(json.dumps(
        {"rows": gold.count(), "locations": gold.select("location_id").distinct().count(),
         "duplicate_keys": dup}, indent=2), encoding="utf-8")
    spark.stop()


if __name__ == "__main__":
    main()
