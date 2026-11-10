"""Raw JSON → Bronze → Silver (Parquet) và báo cáo chất lượng.

Bronze  bronze/era5_hourly: tách mảng hourly thành dòng (ô lưới × giờ), giữ nguyên giá trị và provenance.
Silver  silver/grid_weather: khử trùng (grid_id, ts), giá trị phi vật lý → null (không xóa dòng,
        không fillna 0), thêm ts_epoch. silver/locations, silver/location_grid: bảng điểm và lưới.
Báo cáo reports/quality/: phễu số dòng, null theo biến, độ đủ giờ theo ô × năm.

    spark-submit -m src.pipeline.bronze_silver --profile pilot   (hoặc python -m ...)
"""
import argparse
import csv
import json

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark

VARS = ["precipitation", "temperature_2m", "relative_humidity_2m", "dew_point_2m",
        "pressure_msl", "cloud_cover", "wind_speed_10m", "wind_direction_10m"]

# Khoảng hợp lệ (POC mục 4②). Ngoài khoảng → null và được đếm trong báo cáo.
VALID = {"precipitation": (0, 300), "temperature_2m": (-5, 45), "relative_humidity_2m": (0, 100),
         "dew_point_2m": (-30, 35), "pressure_msl": (900, 1060), "cloud_cover": (0, 100),
         "wind_speed_10m": (0, 250), "wind_direction_10m": (0, 360)}

RAW_SCHEMA = StructType([
    StructField("latitude", DoubleType()), StructField("longitude", DoubleType()),
    StructField("elevation", DoubleType()), StructField("utc_offset_seconds", LongType()),
    StructField("timezone", StringType()),
    StructField("hourly", StructType([StructField("time", ArrayType(StringType()))]
                                     + [StructField(v, ArrayType(DoubleType())) for v in VARS])),
    StructField("_provenance", StructType([StructField("url", StringType()),
                                           StructField("fetched_at_utc", StringType()),
                                           StructField("grid_id", StringType()),
                                           StructField("year", LongType())])),
])


def bronze(spark, cfg):
    raw = (spark.read.schema(RAW_SCHEMA).option("multiLine", True)
           .json(str(path(cfg, "raw") / "grid=*" / "year=*.json")))
    rows = raw.select(
        F.col("_provenance.grid_id").alias("grid_id"), F.col("_provenance.year").alias("year"),
        F.col("latitude").alias("grid_lat"), F.col("longitude").alias("grid_lon"),
        "utc_offset_seconds", F.col("_provenance.fetched_at_utc").alias("fetched_at_utc"),
        F.explode(F.arrays_zip(*[F.col(f"hourly.{c}").alias(c) for c in ["time"] + VARS])).alias("r"))
    return rows.select("grid_id", "year", "grid_lat", "grid_lon", "utc_offset_seconds", "fetched_at_utc",
                       F.col("r.time").alias("time_local"), *[F.col(f"r.{v}").alias(v) for v in VARS])


def silver(bronze_df):
    df = bronze_df.withColumn("ts", F.to_timestamp("time_local", "yyyy-MM-dd'T'HH:mm"))
    # trùng (ô, giờ): giữ bản tải mới nhất
    w = Window.partitionBy("grid_id", "ts").orderBy(F.col("fetched_at_utc").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    for v, (lo, hi) in VALID.items():
        df = df.withColumn(v, F.when((F.col(v) >= lo) & (F.col(v) <= hi), F.col(v)))
    # điểm sương không thể cao hơn nhiệt độ quá sai số làm tròn
    df = df.withColumn("dew_point_2m", F.when(F.col("dew_point_2m") <= F.col("temperature_2m") + 0.5,
                                              F.col("dew_point_2m")))
    return df.withColumn("ts_epoch", F.unix_timestamp("ts")).select(
        "grid_id", "ts", "ts_epoch", "year", *VARS)


def quality(spark, cfg, bronze_df, silver_df, out_dir):
    out_dir.mkdir(parents=True, exist_ok=True)
    n_bronze = bronze_df.count()
    n_dup = n_bronze - bronze_df.select("grid_id", "time_local").distinct().count()
    n_silver = silver_df.count()
    raw_nulls = bronze_df.select([F.sum(F.col(v).isNull().cast("int")).alias(v) for v in VARS]).first().asDict()
    silver_nulls = silver_df.select([F.sum(F.col(v).isNull().cast("int")).alias(v) for v in VARS]).first().asDict()
    with open(out_dir / "silver_funnel.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["step", "rows"])
        w.writerow(["bronze_rows", n_bronze])
        w.writerow(["duplicate_grid_hour_removed", n_dup])
        w.writerow(["silver_rows", n_silver])
    with open(out_dir / "silver_nulls.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["variable", "null_in_source", "null_after_range_rules", "set_null_by_rules"])
        for v in VARS:
            w.writerow([v, raw_nulls[v], silver_nulls[v], silver_nulls[v] - raw_nulls[v]])
    comp = (silver_df.groupBy("grid_id", "year")
            .agg(F.count("*").alias("hours"), F.count("precipitation").alias("precip_hours"))
            .withColumn("expected", F.when((F.col("year") % 4 == 0), 8784).otherwise(8760))
            .orderBy("grid_id", "year").toPandas())
    comp.to_csv(out_dir / "grid_year_completeness.csv", index=False)
    incomplete = comp[comp.precip_hours < comp.expected]
    offsets = [r[0] for r in bronze_df.select("utc_offset_seconds").distinct().collect()]
    summary = {"bronze_rows": n_bronze, "duplicates_removed": n_dup, "silver_rows": n_silver,
               "grid_years": len(comp), "grid_years_incomplete": int(len(incomplete)),
               "utc_offsets_seen": offsets}
    (out_dir / "silver_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    assert offsets == [cfg["source"]["expected_utc_offset_seconds"]], offsets
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="pilot")
    cfg = load_config(profile=ap.parse_args().profile)
    spark = get_spark("bronze_silver")
    lake = path(cfg, "lake")
    b = bronze(spark, cfg)
    b.write.mode("overwrite").partitionBy("year").parquet(str(lake / "bronze" / "era5_hourly"))
    b = spark.read.parquet(str(lake / "bronze" / "era5_hourly"))
    s = silver(b)
    s.write.mode("overwrite").partitionBy("year").parquet(str(lake / "silver" / "grid_weather"))
    s = spark.read.parquet(str(lake / "silver" / "grid_weather"))
    pts = spark.read.csv(str(path(cfg, "points")), header=True, inferSchema=True)
    pts.write.mode("overwrite").parquet(str(lake / "silver" / "locations"))
    plan = spark.read.csv(str(path(cfg, "grid_plan")), header=True, inferSchema=True)
    plan.write.mode("overwrite").parquet(str(lake / "silver" / "location_grid"))
    print(quality(spark, cfg, b, s, ROOT / cfg["paths"]["reports"] / "quality"))
    spark.stop()


if __name__ == "__main__":
    main()
