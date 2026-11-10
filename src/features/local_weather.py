"""Feature khí tượng tại điểm (M1) và nhãn. Đây là "features.py" dùng chung cho batch và streaming.

Quy ước thời gian: precipitation tại mốc t là tổng mưa trong giờ (t-1h, t].
  * Feature tại t dùng cửa sổ (t - k giờ, t]: chỉ các mốc <= t.
  * Nhãn tại t dùng các mốc t+1h .. t+24h, tức mưa trong (t, t+24h].
Cửa sổ tính theo event time (rangeBetween trên ts_epoch), phân vùng theo điểm: không bao giờ
trộn hai điểm, và giờ bị thiếu làm feature thành null thay vì cho tổng thấp giả.
"""
import math

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

H = 3600
M1_FEATURES = ["rain_3h", "rain_24h", "rain_72h", "rain_max_1h_24h", "rain_hours_24h",
               "rh", "rh_mean_6h", "dew_point", "dewpoint_depression",
               "pressure_msl", "pressure_change_3h", "pressure_change_24h",
               "wind_speed", "wind_speed_change_24h", "wind_dir_sin", "wind_dir_cos",
               "temperature", "temp_change_24h", "cloud_cover", "cloud_mean_6h",
               "month_sin", "month_cos", "hour_sin", "hour_cos"]
RAIN_HOUR_MM = 0.1  # dưới mức này coi là mưa phùn/nhiễu khi đếm số giờ mưa


def _past(key, hours):
    """Cửa sổ `hours` mốc gần nhất kết thúc tại t: (t - hours, t]."""
    return Window.partitionBy(key).orderBy("ts_epoch").rangeBetween(-(hours - 1) * H, 0)


def _at(key, hours_ago):
    return Window.partitionBy(key).orderBy("ts_epoch").rangeBetween(-hours_ago * H, -hours_ago * H)


def _complete(col, key, hours, agg) -> Column:
    """agg(col) trên cửa sổ quá khứ, null nếu cửa sổ không đủ `hours` giá trị."""
    w = _past(key, hours)
    return F.when(F.count(col).over(w) == hours, agg(col).over(w))


def local_features(df: DataFrame, key="location_id") -> DataFrame:
    """Thêm M1_FEATURES. df cần: key, ts (timestamp), ts_epoch (giây), 8 biến ERA5."""
    p = F.col("precipitation")
    lag = lambda c, h: F.first(c).over(_at(key, h))
    rad = F.radians("wind_direction_10m")
    month = F.month("ts")
    hour = F.hour("ts")
    return (df
            .withColumn("rain_3h", _complete("precipitation", key, 3, F.sum))
            .withColumn("rain_24h", _complete("precipitation", key, 24, F.sum))
            .withColumn("rain_72h", _complete("precipitation", key, 72, F.sum))
            .withColumn("rain_max_1h_24h", _complete("precipitation", key, 24, F.max))
            .withColumn("_rain_hour", F.when(p.isNull(), None).otherwise((p > RAIN_HOUR_MM).cast("int")))
            .withColumn("rain_hours_24h", _complete("_rain_hour", key, 24, F.sum).cast("double"))
            .withColumn("rh", F.col("relative_humidity_2m"))
            .withColumn("rh_mean_6h", _complete("relative_humidity_2m", key, 6, F.avg))
            .withColumn("dew_point", F.col("dew_point_2m"))
            .withColumn("dewpoint_depression", F.col("temperature_2m") - F.col("dew_point_2m"))
            .withColumn("pressure_change_3h", F.col("pressure_msl") - lag("pressure_msl", 3))
            .withColumn("pressure_change_24h", F.col("pressure_msl") - lag("pressure_msl", 24))
            .withColumn("wind_speed", F.col("wind_speed_10m"))
            .withColumn("wind_speed_change_24h", F.col("wind_speed_10m") - lag("wind_speed_10m", 24))
            .withColumn("wind_dir_sin", F.sin(rad))
            .withColumn("wind_dir_cos", F.cos(rad))
            .withColumn("temperature", F.col("temperature_2m"))
            .withColumn("temp_change_24h", F.col("temperature_2m") - lag("temperature_2m", 24))
            .withColumn("cloud_mean_6h", _complete("cloud_cover", key, 6, F.avg))
            .withColumn("month_sin", F.sin(2 * math.pi * month / 12))
            .withColumn("month_cos", F.cos(2 * math.pi * month / 12))
            .withColumn("hour_sin", F.sin(2 * math.pi * hour / 24))
            .withColumn("hour_cos", F.cos(2 * math.pi * hour / 24))
            .drop("_rain_hour"))


def add_label(df: DataFrame, key="location_id", horizon_hours=24, threshold_mm=50.0) -> DataFrame:
    """rain_next_24h = tổng mưa (t, t+24h]; label = 1 nếu > threshold. Thiếu giờ tương lai → null."""
    w = Window.partitionBy(key).orderBy("ts_epoch").rangeBetween(H, horizon_hours * H)
    n = F.count("precipitation").over(w)
    total = F.sum("precipitation").over(w)
    return (df.withColumn("rain_next_24h", F.when(n == horizon_hours, total))
              .withColumn("label", F.when(F.col("rain_next_24h").isNull(), None)
                          .otherwise((F.col("rain_next_24h") > threshold_mm).cast("double"))))


def assign_split(df: DataFrame, splits: dict, horizon_hours=24) -> DataFrame:
    """Gắn split theo t. Dòng có cửa sổ nhãn vượt ranh giới split bị purge (split = null).

    splits: {"train": [start, end], ...} theo giờ VN, end là mốc giờ cuối cùng thuộc split.
    """
    split = F.lit(None).cast("string")
    reason = F.lit(None).cast("string")
    label_end = F.col("ts") + F.expr(f"INTERVAL {horizon_hours} HOURS")
    for name, (start, end) in splits.items():
        in_split = (F.col("ts") >= F.to_timestamp(F.lit(start))) & (F.col("ts") <= F.to_timestamp(F.lit(end)))
        inside = label_end <= F.to_timestamp(F.lit(end))
        split = F.when(in_split & inside, F.lit(name)).otherwise(split)
        reason = F.when(in_split & ~inside, F.lit(f"purged_{name}_boundary")).otherwise(reason)
    return df.withColumn("split", split).withColumn("split_excluded_reason", reason)
