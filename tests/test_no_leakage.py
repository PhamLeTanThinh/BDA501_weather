"""Chống rò rỉ tương lai và căn thời gian: feature (t-k, t], nhãn (t, t+24h], purge ranh giới split."""
import math
from datetime import datetime

from pyspark.sql import functions as F

from conftest import hourly_rows, to_df
from src.features.local_weather import M1_FEATURES, add_label, assign_split, local_features


def _collect(df, cols):
    return {r["ts"]: r for r in df.orderBy("ts").select("ts", *cols).collect()}


def test_features_do_not_change_when_future_changes(spark):
    wave = lambda k: 5.0 * (1 + math.sin(k / 7))
    base = hourly_rows("location_id", "A", 200, precip={k: wave(k) for k in range(200)})
    t0 = 150
    future_changed = [dict(r) for r in base]
    for k, r in enumerate(future_changed):
        if k > t0:
            r.update(precipitation=99.0, pressure_msl=950.0, relative_humidity_2m=10.0,
                     temperature_2m=40.0, wind_speed_10m=200.0, cloud_cover=0.0)
    a = _collect(local_features(to_df(spark, base)), M1_FEATURES)
    b = _collect(local_features(to_df(spark, future_changed)), M1_FEATURES)
    cutoff = sorted(a)[t0]
    for ts in a:
        if ts <= cutoff:
            assert a[ts].asDict() == b[ts].asDict(), ts
    assert a[sorted(a)[t0 + 1]]["rain_3h"] != b[sorted(b)[t0 + 1]]["rain_3h"]


def test_label_uses_strictly_next_24_hours(spark):
    spike = 100
    df = add_label(local_features(to_df(spark, hourly_rows("location_id", "A", 200, precip={spike: 60.0}))))
    rows = df.orderBy("ts").select("label", "rain_next_24h", "rain_24h").collect()
    positive = [k for k, r in enumerate(rows) if r["label"] == 1.0]
    assert positive == list(range(spike - 24, spike))   # mưa ở mốc 100 = (99h, 100h]: thuộc tương lai của t=76..99
    assert rows[spike]["label"] == 0.0                   # tại t=100 cơn mưa đã xảy ra, không còn là tương lai
    with_rain = [k for k, r in enumerate(rows) if r["rain_24h"]]
    assert with_rain == list(range(spike, spike + 24))   # feature thấy cơn mưa từ t=100, không sớm hơn


def test_label_null_when_future_incomplete(spark):
    df = add_label(to_df(spark, hourly_rows("location_id", "A", 100, skip={60})))
    rows = df.orderBy("ts").select("label").collect()
    labels = [r["label"] for r in rows]                  # dòng thứ i là giờ i, trừ giờ 60 bị thiếu
    assert all(x is None for x in labels[-24:])          # 24 giờ cuối không đủ tương lai
    hours = [k for k in range(100) if k != 60]
    for i, k in enumerate(hours):
        if 36 <= k <= 59:
            assert labels[i] is None, k                  # cửa sổ (k, k+24] chứa giờ thiếu 60


def test_missing_hour_makes_rolling_null_not_small(spark):
    df = local_features(to_df(spark, hourly_rows("location_id", "A", 120, precip={k: 1.0 for k in range(120)},
                                                 skip={50})))
    r = {row["ts"].hour + 24 * (row["ts"].day - 20): row for row in df.select("ts", "rain_24h").collect()}
    for k in range(51, 74):
        assert r[k]["rain_24h"] is None, k
    assert r[74]["rain_24h"] == 24.0


def test_rolling_never_crosses_locations(spark):
    rows = (hourly_rows("location_id", "A", 80, precip={k: 10.0 for k in range(80)})
            + hourly_rows("location_id", "B", 80))
    df = local_features(to_df(spark, rows))
    b = df.where("location_id = 'B'").select(F.max("rain_72h"), F.max("rain_24h")).first()
    assert b[0] == 0.0 and b[1] == 0.0
    a = df.where("location_id = 'A'").agg(F.max("rain_72h")).first()[0]
    assert a == 720.0


def test_pressure_change_uses_exact_lag(spark):
    df = local_features(to_df(spark, hourly_rows("location_id", "A", 60)))
    vals = [r["pressure_change_24h"] for r in df.orderBy("ts").select("pressure_change_24h").collect()]
    assert all(v is None for v in vals[:24])
    assert all(abs(v - (-2.4)) < 1e-9 for v in vals[24:])


def test_split_purges_label_window_crossing_boundary(spark):
    splits = {"train": ["2005-01-01 00:00", "2022-12-31 23:00"],
              "validation": ["2023-01-01 00:00", "2023-12-31 23:00"]}
    df = assign_split(to_df(spark, hourly_rows("location_id", "A", 24 * 20)), splits)
    got = {r["ts"]: (r["split"], r["split_excluded_reason"]) for r in df.collect()}
    assert got[datetime(2022, 12, 30, 23)] == ("train", None)
    assert got[datetime(2022, 12, 31, 0)] == (None, "purged_train_boundary")
    assert got[datetime(2022, 12, 31, 23)] == (None, "purged_train_boundary")
    assert got[datetime(2023, 1, 1, 0)] == ("validation", None)
    train_max = max(ts for ts, (s, _) in got.items() if s == "train")
    assert train_max == datetime(2022, 12, 30, 23)


def test_threshold_is_chosen_from_validation_only():
    from src.experiments.metrics import best_f1_threshold
    import inspect
    params = list(inspect.signature(best_f1_threshold).parameters)
    assert params == ["y_val", "s_val"]
    thr, f1 = best_f1_threshold([0, 0, 1, 1], [0.1, 0.2, 0.7, 0.9])
    assert thr == 0.7 and f1 == 1.0


def test_leave_location_out_never_trains_on_held_out_location(spark, monkeypatch):
    import pandas as pd
    from src.experiments import ablation

    rows = [(loc, split, float(k % 7 == 0), float(k)) for loc in ("A", "B", "C")
            for split in ("train", "validation", "test") for k in range(30)]
    data = spark.createDataFrame(rows, "location_id string, split string, label double, x double")
    seen = []

    def fake_fit_predict(cfg, train, evalset, cols):
        held = {r[0] for r in evalset.where("split = 'test'").select("location_id").distinct().collect()}
        trained = {r[0] for r in train.select("location_id").distinct().collect()}
        val_locs = {r[0] for r in evalset.where("split = 'validation'").select("location_id").distinct().collect()}
        seen.append((held, trained, val_locs, {r[0] for r in train.select("split").distinct().collect()}))
        p = evalset.toPandas()
        p["ts"] = p["x"]
        return None, p.assign(p=p["x"] / 30)

    monkeypatch.setattr(ablation, "fit_predict", fake_fit_predict)
    out = ablation.leave_location_out({}, data, {m: ["x"] for m in ablation.EXPERIMENTS})
    assert len(seen) == 9
    for held, trained, val_locs, splits in seen:
        assert len(held) == 1 and held.isdisjoint(trained) and held.isdisjoint(val_locs)
        assert splits == {"train"}
    assert set(out["M1"].location_id) == {"A", "B", "C"} and set(out["M1"].split) == {"test"}
