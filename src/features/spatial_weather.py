"""Feature khí tượng xung quanh (M3): ô lưới ERA5 lân cận của từng điểm đo.

Hai phần:
  * Lập kế hoạch lưới (Python thuần, dùng khi thu thập): ô lân cận theo offset, khử trùng ô,
    không để ô trung tâm thành hàng xóm của chính nó.
  * Spark: rolling theo từng ô (không trộn giữa các ô), rồi gộp các ô lân cận tại đúng giờ t.

Mọi feature tại t chỉ dùng giờ <= t. Ô lân cận dùng đúng bảng silver/grid_weather như ô trung tâm,
nên cùng múi giờ và đơn vị.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

NEIGHBOR_FEATURES = ["neighbor_rain_sum_6h", "neighbor_rain_sum_24h",
                     "neighbor_pressure_change_24h", "neighbor_humidity_mean_6h"]

OFFSETS = {
    "none": [],  # chỉ ô trung tâm (bản chính thức, feature M1)
    "nsew": [(1, 0), (-1, 0), (0, 1), (0, -1)],
    "ring": [(i, j) for i in (-1, 0, 1) for j in (-1, 0, 1) if (i, j) != (0, 0)],
}


def grid_id(lat, lon):
    """Khóa ô lưới từ tọa độ tâm ô API trả về (làm tròn để 20.999998 và 21.0 là một ô)."""
    return f"g{round(lat, 2):.2f}_{round(lon, 2):.2f}"


def neighbor_candidates(center_lat, center_lon, pattern="nsew", step=0.25):
    """Tọa độ tâm các ô lân cận, tính từ tâm ô trung tâm (không phải tọa độ điểm đo)."""
    return [(di, dj, round(center_lat + di * step, 4), round(center_lon + dj * step, 4))
            for di, dj in OFFSETS[pattern]]


def build_grid_plan(location_id, center, neighbors):
    """Bảng điểm → ô lưới.

    center: (lat, lon) ô trung tâm do API trả về.
    neighbors: list (di, dj, lat, lon) ô lân cận đã được API xác nhận.
    Bỏ ô lân cận trùng ô trung tâm và ô trùng nhau; trả về (rows, dropped).
    """
    cid = grid_id(*center)
    rows = [{"location_id": location_id, "grid_id": cid, "role": "center", "di": 0, "dj": 0,
             "grid_lat": round(center[0], 4), "grid_lon": round(center[1], 4)}]
    seen, dropped = {cid}, []
    for di, dj, lat, lon in neighbors:
        gid = grid_id(lat, lon)
        if gid in seen:
            dropped.append({"location_id": location_id, "grid_id": gid, "di": di, "dj": dj,
                            "reason": "center" if gid == cid else "duplicate"})
            continue
        seen.add(gid)
        rows.append({"location_id": location_id, "grid_id": gid, "role": "neighbor", "di": di, "dj": dj,
                     "grid_lat": round(lat, 4), "grid_lon": round(lon, 4)})
    return rows, dropped


def _hour_range(seconds_back):
    # cửa sổ (t - k giờ, t] theo event time, không theo số dòng: thiếu giờ không làm cửa sổ trượt sai
    return Window.partitionBy("grid_id").orderBy("ts_epoch").rangeBetween(-seconds_back, 0)


def grid_rolling(grid_weather: DataFrame) -> DataFrame:
    """Rolling theo từng ô lưới. Cửa sổ thiếu giờ → null, không coi là 0 mm."""
    w6, w24 = _hour_range(5 * 3600), _hour_range(23 * 3600)
    lag24 = Window.partitionBy("grid_id").orderBy("ts_epoch").rangeBetween(-24 * 3600, -24 * 3600)
    return (grid_weather
            .withColumn("_n6", F.count("precipitation").over(w6))
            .withColumn("_n24", F.count("precipitation").over(w24))
            .withColumn("g_rain_6h", F.when(F.col("_n6") == 6, F.sum("precipitation").over(w6)))
            .withColumn("g_rain_24h", F.when(F.col("_n24") == 24, F.sum("precipitation").over(w24)))
            .withColumn("_nrh6", F.count("relative_humidity_2m").over(w6))
            .withColumn("g_rh_mean_6h", F.when(F.col("_nrh6") == 6, F.avg("relative_humidity_2m").over(w6)))
            .withColumn("g_p_24h_ago", F.first("pressure_msl").over(lag24))
            .withColumn("g_pressure_change_24h", F.col("pressure_msl") - F.col("g_p_24h_ago"))
            .select("grid_id", "ts", "ts_epoch", "g_rain_6h", "g_rain_24h",
                    "g_rh_mean_6h", "g_pressure_change_24h"))


def neighbor_features(grid_weather: DataFrame, grid_plan: DataFrame, min_neighbors=2) -> DataFrame:
    """(location_id, ts) → trung bình các ô lân cận tại cùng giờ t.

    Join theo (grid_id, ts) bằng nhau: chỉ ghép giờ t của hàng xóm với giờ t của điểm, nên
    không thể kéo giờ tương lai vào. Ô trung tâm đã bị loại ở grid_plan (role = neighbor).
    """
    rolled = grid_rolling(grid_weather)
    nb = grid_plan.where(F.col("role") == "neighbor").select("location_id", "grid_id")
    agg = (nb.join(rolled, "grid_id")
           .groupBy("location_id", "ts")
           .agg(F.avg("g_rain_6h").alias("neighbor_rain_sum_6h"),
                F.avg("g_rain_24h").alias("neighbor_rain_sum_24h"),
                F.avg("g_pressure_change_24h").alias("neighbor_pressure_change_24h"),
                F.avg("g_rh_mean_6h").alias("neighbor_humidity_mean_6h"),
                # số ô có đủ cả 4 giá trị: dùng để loại giờ có quá ít hàng xóm hợp lệ
                F.sum(F.when(F.col("g_rain_24h").isNotNull() & F.col("g_rain_6h").isNotNull()
                             & F.col("g_pressure_change_24h").isNotNull()
                             & F.col("g_rh_mean_6h").isNotNull(), 1).otherwise(0)).alias("neighbor_count")))
    ok = F.col("neighbor_count") >= min_neighbors
    return agg.select("location_id", "ts", "neighbor_count",
                      *[F.when(ok, F.col(c)).alias(c) for c in NEIGHBOR_FEATURES])
