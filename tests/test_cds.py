"""Bản chính thức (CDS): đổi đơn vị silver, feature M4 không nhìn tương lai, bảng ô đất liền."""
import math
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from src.features.atmos import ATMOS_FEATURES, atmos_features
from src.pipeline.cds_silver import convert, silver

BRONZE_VARS = ["tp", "vimd", "t2m", "d2m", "msl", "tcc", "u10", "v10", "tcwv", "viwve", "viwvn", "cape", "kx"]


def bronze_df(spark, rows):
    schema = StructType([StructField("grid_id", StringType()), StructField("valid_time", TimestampType()),
                         StructField("ingested_at", TimestampType())]
                        + [StructField(v, DoubleType()) for v in BRONZE_VARS])
    base = {"tp": 0.0, "vimd": 0.0, "t2m": 300.15, "d2m": 295.15, "msl": 101000.0, "tcc": 0.5,
            "u10": 0.0, "v10": 0.0, "tcwv": 50.0, "viwve": 0.0, "viwvn": 0.0, "cape": 100.0, "kx": 30.0}
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return spark.createDataFrame([[r.get("grid_id", "g1"), r["valid_time"], now]
                                  + [float(r.get(v, base[v])) for v in BRONZE_VARS] for r in rows], schema)


def test_unit_conversion_and_wind_direction(spark):
    t = datetime(2020, 6, 1, 0, tzinfo=timezone.utc)
    rows = [{"valid_time": t, "tp": 0.0125, "v10": -5.0},           # gió thổi về nam = gió bắc (0°)
            {"valid_time": t + timedelta(hours=1), "u10": -5.0},     # gió thổi về tây = gió đông (90°)
            {"valid_time": t + timedelta(hours=2), "tp": -1e-9, "u10": 3.0, "v10": 4.0}]
    out = convert(bronze_df(spark, rows)).orderBy("ts").collect()
    assert math.isclose(out[0]["precipitation"], 12.5)
    assert math.isclose(out[0]["temperature_2m"], 27.0, abs_tol=1e-6)
    assert math.isclose(out[0]["pressure_msl"], 1010.0)
    assert math.isclose(out[0]["cloud_cover"], 50.0)
    assert math.isclose(out[0]["wind_direction_10m"], 0.0, abs_tol=1e-6)
    assert math.isclose(out[1]["wind_direction_10m"], 90.0, abs_tol=1e-6)
    assert out[2]["precipitation"] == 0.0                            # âm do làm tròn → 0
    assert math.isclose(out[2]["wind_speed_10m"], 18.0)              # 5 m/s × 3,6
    # Magnus: T = 27 °C, Td = 22 °C → RH ≈ 74 %
    assert 73 < out[0]["relative_humidity_2m"] < 75


def test_silver_uses_vietnam_time_and_window(spark):
    # 17:00 UTC 31/12/2014 = 00:00 giờ VN 01/01/2015 (mốc đầu tiên được giữ)
    t = datetime(2014, 12, 31, 16, tzinfo=timezone.utc)
    rows = [{"valid_time": t + timedelta(hours=k)} for k in range(3)]
    out = silver(bronze_df(spark, rows), "2015-01-01 00:00", "2025-12-31 23:00").orderBy("ts").collect()
    assert [str(r["ts"]) for r in out] == ["2015-01-01 00:00:00", "2015-01-01 01:00:00"]
    assert out[0]["year"] == 2015
    assert out[1]["ts_epoch"] - out[0]["ts_epoch"] == 3600


def test_atmos_features_ignore_future(spark):
    t = datetime(2020, 6, 1, 0, tzinfo=timezone.utc)
    rows = [{"valid_time": t + timedelta(hours=k), "tcwv": 40 + k % 7, "vimd": -0.1 * (k % 5),
             "cape": 50.0 * (k % 9), "viwve": 100.0 + k, "viwvn": -50.0} for k in range(100)]
    future = [dict(r) for r in rows]
    for k, r in enumerate(future):
        if k > 60:
            r.update(tcwv=90.0, vimd=-50.0, cape=9000.0, viwve=-2000.0)
    feats = lambda rs: {r["ts"]: r for r in atmos_features(
        silver(bronze_df(spark, rs), "2020-01-01 00:00", "2020-12-31 23:00").withColumnRenamed("grid_id", "location_id"),
        key="location_id").select("ts", *ATMOS_FEATURES).collect()}
    a, b = feats(rows), feats(future)
    cut = sorted(a)[60]
    assert all(a[ts].asDict() == b[ts].asDict() for ts in a if ts <= cut)
    assert a[sorted(a)[61]]["tcwv"] != b[sorted(b)[61]]["tcwv"]
    first = a[sorted(a)[0]]
    assert first["vimd_sum_6h"] is None and first["cape_max_24h"] is None   # cửa sổ chưa đủ giờ → null


def test_cell_map_covers_all_points():
    import csv
    from src.common.config import ROOT
    with open(ROOT / "data" / "reference" / "cells_land.csv", encoding="utf-8") as f:
        cells = list(csv.DictReader(f))
    ids = [i for c in cells for i in c["location_ids"].split("|") if i]
    assert len(ids) == 63 and len(set(ids)) == 63
    assert len({c["grid_id"] for c in cells}) == len(cells)
    assert len({c["new_province"] for c in cells}) == 34
    assert all(23.5 >= float(c["grid_lat"]) >= 8.5 and 102 <= float(c["grid_lon"]) <= 110 for c in cells)
