"""Ô lưới lân cận: khử trùng, không lấy ô trung tâm, căn đúng giờ, không dùng giờ tương lai."""
from pyspark.sql import functions as F

from conftest import hourly_rows, to_df
from src.features.spatial_weather import (build_grid_plan, grid_id, neighbor_candidates,
                                          neighbor_features)


def test_grid_id_rounds_float_noise():
    assert grid_id(20.999998, 105.750001) == grid_id(21.0, 105.75) == "g21.00_105.75"


def test_candidates_are_offsets_of_center_cell():
    nsew = neighbor_candidates(21.0, 105.75, "nsew", 0.25)
    assert {(lat, lon) for _, _, lat, lon in nsew} == {(21.25, 105.75), (20.75, 105.75), (21.0, 106.0), (21.0, 105.5)}
    ring = neighbor_candidates(21.0, 105.75, "ring", 0.25)
    assert len(ring) == 8 and (21.0, 105.75) not in {(lat, lon) for _, _, lat, lon in ring}


def test_plan_drops_center_and_duplicate_cells():
    # API có thể trả cùng một ô cho hai tọa độ (ô biển bị đẩy về đất liền, hoặc lưới thô)
    nbs = [(1, 0, 21.25, 105.75), (-1, 0, 21.0, 105.75), (0, 1, 21.0, 106.0), (0, -1, 21.0, 106.0)]
    rows, dropped = build_grid_plan("HNOI", (21.0, 105.75), nbs)
    assert [r["grid_id"] for r in rows] == ["g21.00_105.75", "g21.25_105.75", "g21.00_106.00"]
    assert [r["role"] for r in rows] == ["center", "neighbor", "neighbor"]
    assert {(d["grid_id"], d["reason"]) for d in dropped} == {("g21.00_105.75", "center"),
                                                              ("g21.00_106.00", "duplicate")}


def _plan(spark, rows):
    return spark.createDataFrame(rows, "location_id string, grid_id string, role string")


def test_neighbor_features_exclude_center_and_align_in_time(spark):
    spike = 40
    gw = to_df(spark, hourly_rows("grid_id", "C", 100, precip={k: 50.0 for k in range(100)})   # ô trung tâm mưa to
                      + hourly_rows("grid_id", "N1", 100, precip={spike: 12.0})
                      + hourly_rows("grid_id", "N2", 100))
    plan = _plan(spark, [("A", "C", "center"), ("A", "N1", "neighbor"), ("A", "N2", "neighbor")])
    nb = {r["ts"].hour + 24 * (r["ts"].day - 20): r for r in neighbor_features(gw, plan, min_neighbors=2).collect()}
    assert max(r["neighbor_rain_sum_24h"] or 0 for r in nb.values()) == 6.0   # (12 + 0) / 2, không có 50 mm của ô giữa
    six = [k for k, r in nb.items() if r["neighbor_rain_sum_6h"]]
    assert sorted(six) == list(range(spike, spike + 6))                     # không thấy trước giờ spike
    assert all(nb[k]["neighbor_rain_sum_6h"] is None for k in range(5))     # chưa đủ 6 giờ lịch sử


def test_neighbor_features_ignore_future_neighbor_values(spark):
    plan = _plan(spark, [("A", "C", "center"), ("A", "N1", "neighbor"), ("A", "N2", "neighbor")])
    def build(future):
        rows = []
        for g in ("C", "N1", "N2"):
            rr = hourly_rows("grid_id", g, 80, precip={k: 1.0 for k in range(80)})
            if future:
                for k, r in enumerate(rr):
                    if k > 50 and g != "C":
                        r.update(precipitation=80.0, pressure_msl=960.0, relative_humidity_2m=5.0)
            rows += rr
        out = neighbor_features(to_df(spark, rows), plan).orderBy("ts").collect()
        return [r.asDict() for r in out]
    a, b = build(False), build(True)
    assert a[:51] == b[:51]
    assert a[51] != b[51]


def test_min_neighbors_sets_features_null(spark):
    gw = to_df(spark, hourly_rows("grid_id", "N1", 40) + hourly_rows("grid_id", "N2", 40, skip=set(range(10, 40))))
    plan = _plan(spark, [("A", "N1", "neighbor"), ("A", "N2", "neighbor")])
    # giờ thứ 36: N1 đủ 24h lịch sử, N2 đã mất từ giờ 10 → chỉ còn 1 ô hợp lệ
    rows = neighbor_features(gw, plan, min_neighbors=2).where(F.dayofmonth("ts") == 21).orderBy("ts").collect()
    assert rows and all(r["neighbor_count"] == 1 and r["neighbor_rain_sum_24h"] is None for r in rows)
