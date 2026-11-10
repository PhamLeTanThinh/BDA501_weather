"""Độ dốc Horn, khoảng cách tới bờ biển, join bảng địa lý theo location_id."""
import math

import pytest

from src.features.geospatial import (coastline_segments, distance_to_coast_km, haversine_km,
                                     horn_slope_deg, join_geography, slope_stencil)


def test_horn_flat_and_planes():
    assert horn_slope_deg([100] * 9, 1000) == 0
    east = [-100, 0, 100] * 3                      # tăng 100 m mỗi 1000 m về phía đông
    assert horn_slope_deg(east, 1000) == pytest.approx(math.degrees(math.atan(0.1)))
    north = [100] * 3 + [0] * 3 + [-100] * 3       # hàng đầu là phía bắc
    assert horn_slope_deg(north, 1000) == pytest.approx(math.degrees(math.atan(0.1)))
    both = [e + n for e, n in zip(east, north)]
    assert horn_slope_deg(both, 1000) == pytest.approx(math.degrees(math.atan(math.hypot(0.1, 0.1))))


def test_stencil_spacing_is_in_metres_at_any_latitude():
    for lat in (9.0, 16.0, 23.0):
        pts = slope_stencil(lat, 106.0, 1000)
        c = pts[4]
        assert haversine_km(*c, *pts[5]) == pytest.approx(1.0, rel=0.01)   # đông
        assert haversine_km(*c, *pts[1]) == pytest.approx(1.0, rel=0.01)   # bắc


def test_haversine_one_degree_latitude():
    assert haversine_km(10, 106, 11, 106) == pytest.approx(111.19, rel=1e-3)


def test_distance_to_straight_coast():
    seg = [((20.0, 106.0), (21.0, 106.0))]
    expected = haversine_km(20.5, 106.5, 20.5, 106.0)
    assert distance_to_coast_km(20.5, 106.5, seg) == pytest.approx(expected, rel=5e-3)
    # điểm nằm ngoài hai đầu đoạn: khoảng cách là tới đầu mút
    assert distance_to_coast_km(22.0, 106.0, seg) == pytest.approx(haversine_km(22.0, 106.0, 21.0, 106.0), rel=1e-3)
    with pytest.raises(ValueError):
        distance_to_coast_km(20.5, 106.5, [])


def test_coastline_bbox_filter():
    gj = {"features": [{"geometry": {"type": "LineString", "coordinates": [[106, 20], [106, 21]]}},
                       {"geometry": {"type": "MultiLineString", "coordinates": [[[0, 0], [1, 1]]]}}]}
    segs = coastline_segments(gj, (97, 4, 117, 26))
    assert segs == [((20, 106), (21, 106))]


def test_join_geography_requires_every_location(spark):
    geo = spark.createDataFrame([("A", 10.0, 5.0, 0.3)],
                                "location_id string, elevation_m double, distance_to_coast_km double, terrain_slope_deg double")
    df = spark.createDataFrame([("A", 1), ("A", 2), ("B", 1)], "location_id string, x int")
    with pytest.raises(ValueError, match="1 điểm không có"):
        join_geography(df, geo)
    out = join_geography(df.where("location_id = 'A'"), geo)
    assert out.count() == 2 and out.first()["distance_to_coast_km"] == 5.0
