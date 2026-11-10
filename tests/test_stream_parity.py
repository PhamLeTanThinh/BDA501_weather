"""Train–serve consistency: feature tính trên bộ đệm 96 giờ của streaming phải bằng feature tính trên
toàn bộ lịch sử ở batch (cùng hàm point_features)."""
import math

from pyspark.sql import functions as F

from conftest import hourly_rows, to_df
from src.features.local_weather import M1_FEATURES
from src.features.spatial_weather import NEIGHBOR_FEATURES
from src.pipeline.gold_features import point_features
from src.streaming.stream_forecast import BUFFER_HOURS

CFG = {"neighbors": {"min_neighbors": 2}}


def test_buffer_features_equal_full_history(spark):
    n = 240
    wave = lambda phase: (lambda k: max(0.0, 8 * math.sin(k / 9 + phase)))
    rows = []
    for g, ph in (("C", 0.0), ("N1", 1.0), ("N2", 2.0), ("N3", 3.0)):
        rows += hourly_rows("grid_id", g, n, precip={k: wave(ph)(k) for k in range(n)},
                            pressure_msl=lambda k, ph=ph: 1005 + 3 * math.cos(k / 11 + ph),
                            relative_humidity_2m=lambda k, ph=ph: 70 + 20 * math.sin(k / 5 + ph))
    gw = to_df(spark, rows)
    plan = spark.createDataFrame([("A", "C", "center"), ("A", "N1", "neighbor"), ("A", "N2", "neighbor"),
                                  ("A", "N3", "neighbor")], "location_id string, grid_id string, role string")
    locs = spark.createDataFrame([("A", "x", "y", "Y", "Bac", 21.0, 105.8)],
                                 "location_id string, old_province string, new_province string, "
                                 "new_province_code string, region string, lat double, lon double")
    geo = spark.createDataFrame([("A", 10.0, 50.0, 0.5)], "location_id string, elevation_m double, "
                                "distance_to_coast_km double, terrain_slope_deg double")
    cols = M1_FEATURES + NEIGHBOR_FEATURES + ["elevation_m", "distance_to_coast_km", "terrain_slope_deg"]

    full = point_features(CFG, gw, plan, locs, geo, required_geo=True)
    last = full.agg(F.max("ts")).first()[0]
    buffer = gw.where(F.col("ts") > F.lit(last) - F.expr(f"INTERVAL {BUFFER_HOURS} HOURS"))
    served = point_features(CFG, buffer, plan, locs, geo, required_geo=True)

    target = F.col("ts") > F.lit(last) - F.expr("INTERVAL 6 HOURS")   # các giờ streaming vừa nhận
    a = {r["ts"]: r for r in full.where(target).select("ts", *cols).collect()}
    b = {r["ts"]: r for r in served.where(target).select("ts", *cols).collect()}
    assert len(a) == 6 and a.keys() == b.keys()
    for ts in a:
        for c in cols:
            assert a[ts][c] is not None, (ts, c)
            assert math.isclose(a[ts][c], b[ts][c], rel_tol=1e-12, abs_tol=1e-12), (ts, c)
