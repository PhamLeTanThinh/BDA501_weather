# BDA501 — Dự báo mưa lớn 24 giờ tới cho 34 tỉnh Việt Nam

Bài toán: tại giờ t, với mỗi ô lưới ERA5 0,25° trên đất liền Việt Nam (493 ô), tổng mưa trong (t, t+24h]
có vượt 50 mm không (QĐ 18/2021/QĐ-TTg). Dự báo theo ô, gom lên 34 tỉnh mới bằng xác suất lớn nhất của các ô.

**Bản chính thức** (mục này): ERA5 tải từ Copernicus CDS, 13 biến, theo giờ 2015–2025, 47,5 triệu ô-giờ.
Bản pilot cũ (Open-Meteo, 12 điểm, 2019–2025) vẫn còn trong code với `--profile pilot`; MapReduce và
Spark SQL hiện mới chạy trên pilot (xem "Tình trạng").

## Kết quả chính (test 2024–2025, model chọn trên validation 2023)

| | PR-AUC test | Ghi chú |
|---|---|---|
| Đoán ngẫu nhiên | 0,012 | = tỷ lệ mưa lớn |
| Persistence (24 h qua > 50 mm) | 0,147 | baseline |
| Random Forest M1 (thời tiết tại chỗ) | 0,361 | |
| **Random Forest M4 (+ khí quyển), model cuối** | **0,420** | KTC 95% 0,34–0,49; recall 46%, precision 44% ở ngưỡng 0,932 |
| Logistic regression / GBT (cùng M4) | 0,368 / 0,412 | RF chọn theo PR-AUC validation |

Streaming (Kafka → Spark → MongoDB, có dữ liệu rác + tắt Spark giữa chừng): 125.939/125.939 bản tin đọc đúng,
0 dự báo trùng, stream = batch trên 79.993 ô-giờ (`reports/streaming/stream_report.md`).

## Cấu trúc

```
configs/ablation.yaml              split, feature, model, seed=42, bootstrap; profile official/pilot
data/reference/cells_land.csv      493 ô đất liền → tỉnh cũ / tỉnh mới / vùng (build_cell_map)
src/ingest/cds_jobs.py             gửi và tải request ERA5 từ Copernicus CDS (nhiều tài khoản)
src/ingest/build_cell_map.py       gán ô → tỉnh: 5×5 điểm mẫu / ô so với đa giác 63 tỉnh, rồi tra 63 → 34
src/pipeline/cds_bronze.py         NetCDF (zip) → bronze Parquet, chỉ 493 ô, giữ nguyên đơn vị CDS và UTC
src/pipeline/cds_silver.py         bronze → silver: đổi đơn vị, giờ VN, khoảng hợp lệ, báo cáo chất lượng
src/pipeline/cds_gold.py           silver → gold: nhãn, split theo năm, 33 feature (M1 + khí quyển)
src/pipeline/cds_neighbors.py      feature ô lân cận (M3)
src/features/                      local_weather (M1, nhãn, split), atmos (M4) — dùng chung batch + streaming
src/analytics/eda_official.py      EDA trên silver/gold → reports/official/eda
src/experiments/official_ablation.py   ablation M1–M4 (RF), bootstrap theo ngày
src/experiments/model_compare.py       Logistic regression vs Random Forest vs GBT trên M4
src/experiments/official_threshold.py  chọn ngưỡng cảnh báo theo F1 / recall / precision trên 2023
src/experiments/perf_official.py       thí nghiệm hiệu năng: shuffle partitions, broadcast vs sort-merge
src/mapreduce/                     Hadoop Streaming: climatology (mapper, combiner, reducer), export/validate
src/analytics/spark_sql_queries.py 3 truy vấn Spark SQL + query plan
src/streaming/kafka_producer.py    phát lại silver vào Kafka + chèn dữ liệu rác có kiểm soát
src/streaming/kafka_stream.py      Structured Streaming: kiểm tra, watermark 6 h, khử trùng, M4, MongoDB
src/streaming/stream_report.py     đo lường + parity stream vs batch
src/web/                           FastAPI + bản đồ Leaflet (http://localhost:8000)
docker/                            Dockerfile.spark, Dockerfile.stream, stream-compose.yml
deck/                              mã nguồn slide (pptxgenjs) → BDA501_Heavy_Rain_Forecast.pptx
evidence/session9/                 bằng chứng cho Session 9 (A–I)
tests/                             chống rò rỉ, metric, MapReduce, parity
```

Dữ liệu lớn nằm ngoài repo: `D:/BDA501_data/` (`raw/`, `lake/{bronze,silver,gold,models}`), được mount vào
container thành `/data`. Đổi bằng biến `BDA_DATA_HOST`.

## Phiên bản

Spark 4.2.0 (Scala 2.13, Java 17), Python 3.11, image `bda501-rain-spark:1` (từ image Spark của bài lab);
streaming: `bda501-rain-stream:1` (+ connector `spark-sql-kafka-0-10_2.13:4.2.0`, kafka-python-ng, FastAPI),
Kafka `apache/kafka:3.9.1` (KRaft, 1 broker), MongoDB `mongo:7`. Thư viện Python: `requirements.txt`. Seed 42.

## Cài đặt

```bash
docker build -t bda501-rain-spark:1 -f docker/Dockerfile.spark docker
bash scripts/stream_demo.sh build                     # image streaming
```

Tải CDS cần tài khoản Copernicus và file `~/.cdsapirc` (không đưa vào repo).

## Chạy lại kết quả chính thức (từ dữ liệu thô)

```bash
# 1. Tải ERA5 từ CDS: gửi request rồi tải khi CDS xử lý xong (mỗi request = khung VN × 2 tháng × 13 biến)
python -m src.ingest.cds_jobs submit --years 2015 2025 --months-per-request 2 --manifest jobs_main.csv
python -m src.ingest.cds_jobs download --manifest jobs_main.csv
# 2. Gán ô → tỉnh, địa hình
python -m src.ingest.build_cell_map
python -m src.ingest.collect_geography --profile official
# 3. Lake: bronze → silver → gold (+ ô lân cận), EDA
python -m src.pipeline.cds_bronze --profile official
scripts/run_in_spark.sh src.pipeline.cds_silver --profile official
scripts/run_in_spark.sh src.pipeline.cds_gold --profile official
scripts/run_in_spark.sh src.pipeline.cds_neighbors --profile official
scripts/run_in_spark.sh src.analytics.eda_official --profile official
scripts/hdfs_put_official.sh                          # raw + bronze lên HDFS (cụm lab)
# 4. Model: ablation, so sánh thuật toán, chọn ngưỡng, hiệu năng
scripts/run_in_spark.sh src.experiments.official_ablation --profile official
scripts/run_in_spark.sh src.experiments.model_compare --profile official
scripts/run_in_spark.sh src.experiments.official_threshold --profile official
scripts/run_in_spark.sh src.experiments.perf_official --profile official
# 5. Kiểm thử
scripts/run_in_spark.sh pytest -q tests
```

Kết quả: `reports/official/` (chất lượng, EDA, model, hiệu năng). Model lưu ở `lake/models/official/<run_id>/`.

## Streaming (Kafka) và web

```bash
bash scripts/stream_demo.sh up       # Kafka, MongoDB, Spark, web; producer phát dồn 72 giờ làm nóng rồi chờ
# mở http://localhost:8000 → bấm "▶ Bắt đầu stream" (1 giờ dữ liệu / 5 giây; Spark micro-batch 10 giây)
bash scripts/stream_demo.sh reset    # xóa topic, checkpoint, MongoDB để chạy lại từ đầu
bash scripts/stream_demo.sh down

bash scripts/stream_scenarios.sh     # kịch bản đo: dữ liệu rác + tắt Spark giữa chừng → reports/streaming/
```

Đây là **phát lại dữ liệu lịch sử (hindcast)**: ERA5 công bố trễ khoảng 5 ngày nên không dùng được để cảnh báo
thật; vận hành thật cần nguồn dự báo số (xem `evidence/session9/H_project_card.md`).

## MapReduce và Spark SQL (hiện trên dữ liệu pilot)

```bash
scripts/run_in_spark.sh src.mapreduce.mr_io export --profile pilot
scripts/run_mapreduce.sh                              # cần cụm BDA501-Hadoop-Lab
scripts/run_in_spark.sh src.mapreduce.mr_io validate --profile pilot
scripts/run_in_spark.sh src.analytics.spark_sql_queries --profile pilot
```

## Slide

```bash
cd deck && npm install
DECK_NAME=BDA501_Heavy_Rain_Forecast.pptx NODE_PATH=$PWD/node_modules PPTX_SKILL=<thư mục skill pptx> node build_bilingual.js
```

## Chống rò rỉ (kiểm thử trong `tests/`)

- Feature tại t dùng cửa sổ (t−k, t] theo event time, phân vùng theo ô; thiếu giờ thì null, không coi là 0.
- Nhãn dùng mưa trong (t, t+24h]; dòng có cửa sổ nhãn vượt ranh giới split bị loại.
- Chia train 2015–2022 / validation 2023 / test 2024–2025; trọng số lớp tính trên train; model và ngưỡng chọn
  trên validation, khóa trước khi chấm test.
- Streaming dùng chung hàm feature với batch; parity kiểm tra trên toàn bộ ô-giờ của lượt chạy.

## Tình trạng

| Hạng mục | Trạng thái |
|---|---|
| Thu thập CDS, lake, EDA, ablation, so sánh model, ngưỡng, streaming + web | đã chạy trên dữ liệu chính thức |
| MapReduce, Spark SQL | đã chạy và kiểm chứng trên **pilot**; chưa chạy lại trên dữ liệu chính thức |
| Triển khai cụm/cloud (YARN, Kafka nhiều broker) | **thiết kế** |

## Nguồn dữ liệu

| Dữ liệu | Nguồn | Giấy phép |
|---|---|---|
| ERA5 single levels theo giờ | Copernicus Climate Data Store | Copernicus licence (ghi nguồn C3S/ECMWF) |
| Ranh giới tỉnh | geoBoundaries ADM1 | CC BY 4.0 |
| Độ cao | OpenTopoData (SRTM 90 m) | theo điều khoản SRTM |
| Trạm đo mưa đối chiếu | NOAA GHCN-Daily | công cộng |
