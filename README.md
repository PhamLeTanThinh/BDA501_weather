# BDA501 — Dự báo mưa lớn 24 giờ tới: pipeline và Ablation M1/M2/M3

Bài toán: tại giờ t, với mỗi điểm đo, tổng mưa ERA5 trong (t, t+24h] có vượt 50 mm không
(QĐ 18/2021/QĐ-TTg, Điều 5, khoản 17). Thiết kế tổng thể nằm ở `../BDA501_Rain_Forecast_POC_Spatial_Ablation.html`, mục 10–12.

Ablation so sánh ba tập feature trên cùng Random Forest (Spark MLlib), cùng cohort, cùng split:

| Thí nghiệm | Feature |
|---|---|
| M1 | 24 feature khí tượng tại điểm: mưa 3/24/72h, độ ẩm, điểm sương, áp suất và biến thiên, gió, nhiệt, mây, mùa, giờ. **Không có độ cao.** |
| M2 | M1 + `elevation_m`, `distance_to_coast_km`, `terrain_slope_deg` |
| M3 | M2 + `neighbor_rain_sum_6h`, `neighbor_rain_sum_24h`, `neighbor_pressure_change_24h`, `neighbor_humidity_mean_6h` (trung bình các ô ERA5 lân cận N/S/E/W) |

## Cấu trúc

```
configs/ablation.yaml            split, feature set, RF, seed, bootstrap, profile full/pilot, streaming
data/reference/points.csv        63 điểm (tỉnh cũ → tỉnh mới, vùng, tọa độ); cột pilot
data/reference/grid_plan.csv     điểm → ô trung tâm + ô lân cận (sinh tự động, đã khử trùng)
data/reference/location_geography.csv   bảng tra cứu địa lý (sinh tự động)
src/ingest/collect_era5.py       Open-Meteo ERA5 → data/raw/era5/grid=*/year=*.json + collect_log.csv
src/ingest/collect_geography.py  DEM (Elevation API) + Natural Earth coastline → bảng địa lý
src/pipeline/bronze_silver.py    raw → bronze → silver (Parquet) + báo cáo chất lượng
src/pipeline/gold_features.py    silver → gold/features_hourly (nhãn, split, M1+M2+M3)
src/features/local_weather.py    feature M1, nhãn, split (dùng chung batch + streaming)
src/features/geospatial.py       độ dốc Horn, khoảng cách bờ biển, join theo location_id
src/features/spatial_weather.py  kế hoạch ô lân cận, rolling theo ô, gộp hàng xóm tại giờ t
src/experiments/ablation.py      train/đánh giá M1–M3, bootstrap, breakdown, manifest
src/experiments/metrics.py       PR-AUC (average precision), ROC-AUC, ngưỡng, block bootstrap
src/streaming/replay_producer.py phát lại Yagi 09/2024 thành file mới (ghi staging rồi rename)
src/streaming/stream_forecast.py Structured Streaming (file source, checkpoint) → RF → MongoDB upsert
tests/                           rò rỉ tương lai, căn thời gian, ô lân cận, địa lý, metric, train–serve parity
reports/                         kết quả sinh tự động
```

`data/lake/` đóng vai trò HDFS khi chạy local. Trên cụm, đổi `paths.lake` thành `hdfs://namenode:9000/rain-forecast`.

## Cài đặt

Cần Docker. Image dựa trên image Spark của bài lab (`bda501-spark-lab-spark:latest`, Spark 4.2.0):

```bash
docker build -t bda501-rain-spark:1 -f docker/Dockerfile.spark docker
```

Thu thập dữ liệu chỉ cần Python 3 với `pyyaml`. Spark chạy qua `scripts/run_in_spark.sh`.

## Chạy

```bash
P=pilot   # hoặc full: 63 điểm × 2015–2025
python -m src.ingest.collect_era5 --profile $P          # chạy lại để resume; dừng khi hết lượt/ngày
python -m src.ingest.collect_geography --profile $P
scripts/run_in_spark.sh pytest -q tests
scripts/run_in_spark.sh src.pipeline.bronze_silver --profile $P
scripts/run_in_spark.sh src.pipeline.gold_features --profile $P
scripts/run_in_spark.sh src.experiments.ablation --profile $P
```

Demo streaming Yagi (Hải Phòng, Quảng Ninh, 01–10/09/2024, 72 giờ đầu để warm-up):

```bash
docker network create bda501-rain
docker run -d --name mongo --network bda501-rain mongo:7
scripts/run_in_spark.sh src.streaming.stream_forecast --profile pilot &      # đợi file mới
scripts/run_in_spark.sh src.streaming.replay_producer --profile pilot        # 5 giây = 1 giờ lịch sử
scripts/run_in_spark.sh src.streaming.stream_forecast --profile pilot --evaluate   # hậu kiểm sau t+24h
```

## Quy mô và lượt API

Open-Meteo miễn phí: 600/phút, 5.000/giờ, 10.000/ngày; 1 ô × 1 năm ≈ 26 lượt.

| Profile | Điểm | Ô lưới (sau khử trùng) | Năm | Request | Lượt ≈ | Thời gian tải |
|---|---|---|---|---|---|---|
| pilot | 8 | 38 | 2019–2025 | 266 | 6.900 | ~1,7 giờ |
| full | 63 | ~250 (ước tính, chạy `--plan-only` để biết đúng) | 2005–2025 | ~5.250 | ~137.000 | ~14 ngày theo giới hạn ngày |

Profile full giữ nguyên split. Với pilot, train chỉ còn 2019–2022.

## Chống rò rỉ (được kiểm thử trong `tests/`)

- Feature tại t dùng cửa sổ (t−k, t] theo event time, phân vùng theo điểm/ô; thiếu giờ thì null, không coi là 0.
- Nhãn dùng mưa ở các mốc t+1..t+24, tức mưa trong (t, t+24h]; tương lai thiếu giờ thì nhãn null.
- Dòng có cửa sổ nhãn vượt ranh giới split bị purge (`split_excluded_reason`).
- Ô lân cận chỉ ghép giờ t với giờ t; ô trung tâm không bao giờ là hàng xóm.
- Trọng số lớp và climatology baseline tính trên train; ngưỡng chọn trên validation và khóa trước test.
- Streaming dùng chung `point_features()` với batch; test parity so sánh bộ đệm 96 giờ với toàn bộ lịch sử.

## Phụ thuộc dữ liệu

| Dữ liệu | Nguồn | Giấy phép |
|---|---|---|
| ERA5 theo giờ | Open-Meteo Historical Weather API | CC BY 4.0 (ghi nguồn Open-Meteo, ECMWF) |
| DEM | Open-Meteo Elevation API (Copernicus GLO-90) | theo điều khoản Copernicus DEM |
| Đường bờ biển | Natural Earth 1:10m v5.1.2 | public domain |
