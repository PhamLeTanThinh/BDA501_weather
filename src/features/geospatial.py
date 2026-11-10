"""Feature địa lý tĩnh (M2): elevation_m, distance_to_coast_km, terrain_slope_deg.

Phần tính toán (Python thuần, kiểm thử được không cần Spark):
  * terrain_slope_deg: công thức Horn trên lưới 3×3 lấy từ DEM (Copernicus GLO-90 qua Open-Meteo
    Elevation API), khoảng cách lưới tính bằng mét nên không phụ thuộc vĩ độ.
  * distance_to_coast_km: khoảng cách trắc địa từ điểm tới đường bờ biển Natural Earth 1:10m.
    Tìm đỉnh gần nhất bằng haversine (chính xác trên mặt cầu), rồi tính khoảng cách tới các đoạn
    gần đó trên mặt phẳng phương vị cục bộ đặt tâm tại điểm (sai số < 0,5% trong bán kính 100 km).

Phần Spark: đọc bảng tra cứu, kiểm tra khóa, join theo location_id bất biến.
Thiếu dữ liệu GIS thì để null và báo lỗi khi chạy M2, không tự điền giá trị.
"""
import math

EARTH_R_KM = 6371.0088
M_PER_DEG_LAT = 111_320.0
GEO_FEATURES = ["elevation_m", "distance_to_coast_km", "terrain_slope_deg"]


# ---------- Độ dốc ----------

def slope_stencil(lat, lon, spacing_m):
    """9 tọa độ lưới 3×3 quanh điểm, thứ tự hàng bắc → nam, cột tây → đông."""
    dlat = spacing_m / M_PER_DEG_LAT
    dlon = spacing_m / (M_PER_DEG_LAT * math.cos(math.radians(lat)))
    return [(lat + r * dlat, lon + c * dlon) for r in (1, 0, -1) for c in (-1, 0, 1)]


def horn_slope_deg(z, spacing_m):
    """Độ dốc (độ) tại ô giữa từ 9 độ cao z1..z9 (m) theo thứ tự của slope_stencil."""
    z1, z2, z3, z4, _, z6, z7, z8, z9 = z
    dzdx = ((z3 + 2 * z6 + z9) - (z1 + 2 * z4 + z7)) / (8 * spacing_m)
    dzdy = ((z1 + 2 * z2 + z3) - (z7 + 2 * z8 + z9)) / (8 * spacing_m)
    return math.degrees(math.atan(math.hypot(dzdx, dzdy)))


# ---------- Khoảng cách tới bờ biển ----------

def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * EARTH_R_KM * math.asin(math.sqrt(a))


def coastline_segments(geojson, bbox):
    """Danh sách đoạn ((lat, lon), (lat, lon)) có ít nhất một đầu nằm trong bbox."""
    lon0, lat0, lon1, lat1 = bbox
    inside = lambda p: lon0 <= p[0] <= lon1 and lat0 <= p[1] <= lat1
    segs = []
    for feat in geojson["features"]:
        g = feat["geometry"]
        lines = g["coordinates"] if g["type"] == "MultiLineString" else [g["coordinates"]]
        for line in lines:
            for a, b in zip(line, line[1:]):
                if inside(a) or inside(b):
                    segs.append(((a[1], a[0]), (b[1], b[0])))
    return segs


def _to_local(lat0, lon0, lat, lon):
    """Mặt phẳng cục bộ đặt tâm tại (lat0, lon0), đơn vị km."""
    x = math.radians(lon - lon0) * math.cos(math.radians(lat0)) * EARTH_R_KM
    y = math.radians(lat - lat0) * EARTH_R_KM
    return x, y


def _point_segment(px, py, ax, ay, bx, by):
    dx, dy = bx - ax, by - ay
    L2 = dx * dx + dy * dy
    t = 0.0 if L2 == 0 else max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / L2))
    return math.hypot(px - (ax + t * dx), py - (ay + t * dy))


def distance_to_coast_km(lat, lon, segments):
    if not segments:
        raise ValueError("Không có đoạn bờ biển nào trong bbox")
    d_vertex = min(min(haversine_km(lat, lon, *a), haversine_km(lat, lon, *b)) for a, b in segments)
    # chỉ các đoạn có thể gần hơn đỉnh gần nhất mới cần tính chính xác
    best = d_vertex
    for a, b in segments:
        seg_len = haversine_km(*a, *b)
        if haversine_km(lat, lon, *a) - seg_len > d_vertex:
            continue
        ax, ay = _to_local(lat, lon, *a)
        bx, by = _to_local(lat, lon, *b)
        best = min(best, _point_segment(0.0, 0.0, ax, ay, bx, by))
    return best


# ---------- Spark ----------

def load_geography(spark, csv_path):
    """Bảng tra cứu silver/location_geography: 1 dòng / location_id."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType
    schema = StructType([StructField("location_id", StringType(), False)]
                        + [StructField(c, DoubleType(), True) for c in GEO_FEATURES]
                        + [StructField("source", StringType(), True)])
    geo = spark.read.csv(str(csv_path), header=True, schema=schema)
    dup = geo.groupBy("location_id").count().where(F.col("count") > 1).count()
    if dup:
        raise ValueError(f"location_geography có {dup} location_id trùng")
    return geo


def join_geography(df, geo, required=True):
    """Gắn feature địa lý theo location_id. Thiếu giá trị thì báo lỗi thay vì để model tự xử lý."""
    from pyspark.sql import functions as F
    missing_ids = df.select("location_id").distinct().join(geo, "location_id", "left_anti").count()
    nulls = geo.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in GEO_FEATURES]).first().asDict()
    if required and (missing_ids or any(nulls.values())):
        raise ValueError(f"Thiếu dữ liệu địa lý: {missing_ids} điểm không có trong bảng, null theo cột {nulls}. "
                         "Chạy src.ingest.collect_geography trước (cần Elevation API và file coastline).")
    return df.join(F.broadcast(geo.select("location_id", *GEO_FEATURES)), "location_id", "left")
