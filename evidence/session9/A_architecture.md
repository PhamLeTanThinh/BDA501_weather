# A — End-to-end integration truth audit

Diagram: deck slides "Architecture overview" (29), "Batch lane in detail" (31), "Streaming lane in detail" (35),
in `deck/BDA501_Heavy_Rain_Forecast.pptx`. Solid boxes/arrows = implemented, dashed = design only.

## Components: [I] implemented · [P] proposed / design-only · [N/A] not used

| Component | Status | Evidence |
|---|---|---|
| Copernicus CDS download (`cds_jobs`, 6 accounts, 67 requests) | [I] | `D:/BDA501_data/raw/era5_cds` 68 zip, 4.7 GB |
| Cell → province map (point-in-polygon, 63 → 34) | [I] | `data/reference/cells_land.csv` (493 cells) |
| Bronze (Python, NetCDF → Parquet) | [I] | 47,552,808 rows, `reports/official/quality/silver_summary.json` |
| HDFS (raw + bronze on lab cluster) | [I] | `reports/official/hdfs_ls.txt` |
| Silver / gold on Spark (local volume `/data/lake`) | [I] | 47,540,976 rows, 33 features, `reports/official/quality/` |
| silver / gold / models on HDFS | [P] | design: lake on HDFS / object store |
| EDA + quality checks (Spark) | [I] | `reports/official/eda/`, `notebooks/eda_official.ipynb` |
| Spark ML: RF M1–M4, LR, GBT | [I] | `reports/official/model/*.csv` |
| MapReduce climatology (Hadoop Streaming on YARN) | [I] on **pilot data** | `reports/mapreduce/job.log`, `validation.json` (0/144 diff) |
| Spark SQL analytical queries | [I] on **pilot data**; join query on official data in `C_query_plan` | `reports/spark_sql/`, `reports/official/performance/plan_*.txt` |
| MongoDB serving (predictions, province_risk, rejects) | [I] | `reports/streaming/stream_report.md` |
| Kafka (KRaft, 1 broker, 3 partitions) | [I] | `docker/stream-compose.yml` |
| Spark Structured Streaming (watermark, dedup, M4) | [I] | `src/streaming/kafka_stream.py` |
| Web dashboard (FastAPI + Leaflet) | [I] | `src/web/`, http://localhost:8000 |
| Live data source (Forecast API / NWP analysis) | [P] | ERA5 is ~5 days late; see `H_project_card.md` E1 |
| Cluster deployment (YARN/K8s, 3 Kafka brokers, Mongo replica set) | [P] | deck slide "Cluster and cloud deployment (design)" |
| Online learning | [N/A] | model is retrained offline only |

## 2.1 One data item traced (cell g20.75_106.75, Hai Phong, 06/09/2024 19:00 VN)

| Stage | Artifact | Technology | Evidence |
|---|---|---|---|
| Source / ingestion | ERA5 single levels, request 2024 Sep–Oct, 13 vars | CDS API (`cds_jobs.py`) | zip in `raw/era5_cds/` |
| Raw / distributed storage | NetCDF zip | local disk + HDFS `/rain-forecast/raw/era5_cds` | `hdfs_ls.txt` |
| Cleaning / transformation | bronze row (m, K, Pa, UTC) → silver row (mm, °C, hPa, VN time) | Python (bronze), Spark (silver) | `cds_bronze.py`, `cds_silver.py` |
| Curated / columnar | gold row: label, 33 features, split=test; `rain_next_24h` = 146.4 mm, label 1 | Parquet, folders by year | `lake/gold/features_hourly/year=2024` |
| Spark SQL / analytics | contributes to "heavy days per province × year" (Hải Phòng 2024) | Spark SQL | `C_query_plan.md` |
| ML | M4 RF probability 0.959 ≥ 0.932 → alert | Spark ML | model `lake/models/official/20261107T050443Z_official_ablation/M4` |
| NoSQL serving / read-back | document `_id = g20.75_106.75|2024-09-06 19:00`, p, alert, observed 146.4 mm | MongoDB upsert | `reports/streaming/stream_deck.json` (`sample_doc`) |
| Streaming path | JSON message in Kafka topic `weather.silver.hourly` → watermark/dedup → features → M4 | Kafka + Structured Streaming | `stream_report.md` |
| Final user | red cell on the web map, Hải Phòng in "top provinces" | FastAPI + Leaflet | live demo |

## 2.2 Challenge questions

- **Most important flow:** silver → gold features → RF M4 → threshold → MongoDB → map. Every member can explain it from the deck.
- **Present only as a "Big Data name"?** MapReduce: its climatology output is not an input to the model or the stream (the
  evaluation baseline is recomputed in Spark); it is a batch analytics path validated against Spark SQL. Kafka is justified
  only partly: the source is a replay, so latency gains are not real (see E1); its value here is a durable log for replay,
  restart and junk handling.
- **Boundaries:** batch = bronze/silver/gold, EDA, training, MapReduce, Spark SQL. Streaming ingestion = Kafka → Structured
  Streaming. Online serving = MongoDB + web. No online learning.
- **Design-only labelled?** Yes: dashed shapes on the diagrams; deployment slide is titled "(design)".
- **What breaks if a component fails:** see `F_serving_stream.md` (Spark killed mid-run: 0 lost, 0 duplicates) and
  `G_deployment.md` (single Kafka broker = single point of failure for un-consumed data).

## 2.3 One-technology deletion test: remove MapReduce

Functionality: unchanged for forecasting (model, stream, web do not read it); the climatology table can be produced by
Spark SQL with identical results (0/144 differences). Scalability/latency: none affected. Complexity: one less cluster
(Hadoop/YARN) and a CSV export step. Assessment evidence: we would lose the required "project-related MapReduce" item,
so it stays — honestly framed as a batch analytics + equivalence check, not a production path.
