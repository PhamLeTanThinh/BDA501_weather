"""bronze/era5_cds_hourly → silver/grid_weather (Spark) và báo cáo chất lượng.

Silver dùng cùng tên cột và đơn vị với silver của pilot (Open-Meteo), nên local_features() và phần
còn lại của pipeline dùng lại được, cộng thêm 6 biến khí quyển:

  precipitation       tp × 1000, mm trong giờ (t-1h, t]; giá trị âm rất nhỏ do làm tròn của ERA5 → 0
  temperature_2m      t2m − 273,15 (°C)          dew_point_2m   d2m − 273,15 (°C)
  relative_humidity_2m  Magnus từ t2m và d2m (%)  pressure_msl   msl / 100 (hPa)
  cloud_cover         tcc × 100 (%)
  wind_speed_10m      √(u10² + v10²) × 3,6 (km/h)
  wind_direction_10m  hướng gió thổi tới theo khí tượng: atan2(−u, −v) (°, 0 = gió bắc)
  tcwv (mm), ivt_east / ivt_north (viwve / viwvn, kg m⁻¹ s⁻¹), vimd (kg m⁻² trong giờ, âm = hội tụ ẩm),
  cape (J/kg), k_index (kx, K)

Thời gian: valid_time (UTC) → ts theo giờ Việt Nam (phiên Spark đặt Asia/Ho_Chi_Minh), chỉ giữ
2015-01-01 00:00 … 2025-12-31 23:00 giờ VN. Giá trị ngoài khoảng vật lý → null, không xóa dòng.

    scripts/run_in_spark.sh src.pipeline.cds_silver --profile official
"""
import argparse
import json

from pyspark.sql import functions as F

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark
from src.pipeline.bronze_silver import VALID as VALID_SURFACE

SURFACE = ["precipitation", "temperature_2m", "relative_humidity_2m", "dew_point_2m",
           "pressure_msl", "cloud_cover", "wind_speed_10m", "wind_direction_10m"]
ATMOS = ["tcwv", "ivt_east", "ivt_north", "vimd", "cape", "k_index"]
VARS = SURFACE + ATMOS
VALID = {**VALID_SURFACE, "tcwv": (0, 100), "ivt_east": (-3000, 3000), "ivt_north": (-3000, 3000),
         "vimd": (-100, 100), "cape": (0, 10000), "k_index": (-60, 60)}
CELL_COLS = ["grid_id", "grid_lat", "grid_lon", "land_frac", "old_province", "new_province",
             "new_province_code", "region", "location_ids"]


def magnus_rh(t_c, td_c):
    """Độ ẩm tương đối (%) theo công thức Magnus (Alduchov & Eskridge 1996: a = 17,625, b = 243,04 °C)."""
    a, b = 17.625, 243.04
    return 100 * F.exp(a * td_c / (b + td_c)) / F.exp(a * t_c / (b + t_c))


def convert(bronze_df):
    """Đổi đơn vị CDS → đơn vị silver, thêm ts (giờ VN) và ts_epoch. Chưa lọc khoảng thời gian."""
    t = F.col("t2m") - 273.15
    td = F.col("d2m") - 273.15
    u, v = F.col("u10"), F.col("v10")
    return bronze_df.select(
        "grid_id", F.col("valid_time").alias("ts"), "ingested_at",
        (F.greatest(F.col("tp"), F.lit(0.0)) * 1000).alias("precipitation"),
        t.alias("temperature_2m"),
        F.least(magnus_rh(t, td), F.lit(100.0)).alias("relative_humidity_2m"),
        td.alias("dew_point_2m"),
        (F.col("msl") / 100).alias("pressure_msl"),
        (F.col("tcc") * 100).alias("cloud_cover"),
        (F.sqrt(u * u + v * v) * 3.6).alias("wind_speed_10m"),
        F.pmod(F.degrees(F.atan2(-u, -v)), F.lit(360.0)).alias("wind_direction_10m"),
        F.col("tcwv"), F.col("viwve").alias("ivt_east"), F.col("viwvn").alias("ivt_north"),
        F.col("vimd"), F.col("cape"), F.col("kx").alias("k_index"))


def silver(bronze_df, start, end):
    df = convert(bronze_df)
    df = df.where((F.col("ts") >= F.to_timestamp(F.lit(start))) & (F.col("ts") <= F.to_timestamp(F.lit(end))))
    # bronze có đúng một file cho mỗi khối tháng nên không khử trùng ở đây (một window trên ~50 triệu dòng
    # tốn bộ nhớ); quality() đếm khóa (ô, giờ) trùng và dừng nếu có
    df = df.drop("ingested_at")
    for c, (lo, hi) in VALID.items():
        df = df.withColumn(c, F.when((F.col(c) >= lo) & (F.col(c) <= hi), F.col(c)))
    return (df.withColumn("ts_epoch", F.unix_timestamp("ts"))
              .withColumn("year", F.year("ts"))
              .select("grid_id", "ts", "ts_epoch", "year", *VARS))


def quality(bronze_df, silver_df, cells, out_dir):
    out_dir.mkdir(parents=True, exist_ok=True)
    n_bronze = bronze_df.count()
    n_silver = silver_df.count()
    nulls = silver_df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in VARS]).first().asDict()
    comp = (silver_df.groupBy("grid_id", "year").agg(F.count("*").alias("hours"),
                                                      F.count("precipitation").alias("precip_hours"))
            .withColumn("expected", F.when(F.col("year") % 4 == 0, 8784).otherwise(8760)).toPandas())
    comp.to_csv(out_dir / "silver_cell_year_completeness.csv", index=False)
    bounds = silver_df.agg(F.min("ts").alias("first_ts"), F.max("ts").alias("last_ts")).first()
    dup = silver_df.groupBy("grid_id", "ts_epoch").count().where("count > 1").count()
    summary = {
        "bronze_rows": n_bronze, "silver_rows": n_silver, "duplicate_cell_hours": dup,
        "cells": silver_df.select("grid_id").distinct().count(), "cells_in_map": cells.count(),
        "first_ts_vn": str(bounds.first_ts), "last_ts_vn": str(bounds.last_ts),
        "cell_years": len(comp), "cell_years_incomplete": int((comp.hours < comp.expected).sum()),
        "nulls_after_rules": nulls,
    }
    (out_dir / "silver_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False),
                                                 encoding="utf-8")
    assert dup == 0, f"silver có {dup} khóa (ô, giờ) trùng"
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="official")
    cfg = load_config(profile=ap.parse_args().profile)
    spark = get_spark("cds_silver", master="local[8]", shuffle_partitions=64)
    lake = path(cfg, "lake")
    bronze_df = spark.read.parquet(str(lake / "bronze" / "era5_cds_hourly"))
    start, end = cfg["splits"]["train"][0], cfg["splits"]["test"][1]
    s = silver(bronze_df, start, end)
    s.write.mode("overwrite").partitionBy("year").parquet(str(lake / "silver" / "grid_weather"))
    cells = spark.read.csv(str(path(cfg, "cells")), header=True, inferSchema=True).select(*CELL_COLS)
    cells.write.mode("overwrite").parquet(str(lake / "silver" / "cells"))
    s = spark.read.parquet(str(lake / "silver" / "grid_weather"))
    print(json.dumps(quality(bronze_df, s, cells, ROOT / cfg["paths"]["reports"] / "quality"),
                     indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
