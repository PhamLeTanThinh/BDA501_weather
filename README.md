# BDA501 — 24-hour heavy-rain forecasting for Vietnam's 34 provinces

Task: at hour t, for every 0.25° ERA5 grid cell over Vietnam's land (493 cells), will the total rain in (t, t+24h]
exceed 50 mm (Decision 18/2021/QĐ-TTg)? Forecasts are made per cell and rolled up to the 34 new provinces using the
maximum probability over each province's cells.

**Official run** (this section): ERA5 downloaded from the Copernicus CDS, 13 variables, hourly 2015–2025, 47.5 million
cell-hours. The earlier pilot (Open-Meteo, 12 points, 2019–2025) is still in the code under `--profile pilot`;
MapReduce on YARN and Spark SQL Q1–Q3 have so far been run on the pilot (see "Status").

Team 1: Trần Quốc Việt (25MS23304) · Nguyễn Phạm Thành Luân (25MS23265) · Phạm Lê Tấn Thịnh (25MS23273).

## Main results (test 2024–2025; algorithm and threshold chosen on validation 2023)

| | Test PR-AUC | Note |
|---|---|---|
| Random guess | 0.012 | = heavy-rain rate |
| Persistence (last 24 h > 50 mm) | 0.147 | baseline |
| Random Forest M1 (local weather) | 0.361 | |
| **Random Forest M4 (+ atmosphere), final model** | **0.420** | 95% CI 0.34–0.49; recall 46%, precision 44% at threshold 0.932 |
| Logistic regression / GBT (same M4 features) | 0.368 / 0.412 | RF chosen on validation PR-AUC |

Streaming (Kafka → Spark → MongoDB, with injected junk data and Spark killed mid-run): 125,939/125,939 messages
read exactly once, 0 duplicate forecasts, stream = batch on 81,710 cell-hours and on 21,714 feature values
(`reports/streaming/stream_report.md`).

## Layout

```
configs/ablation.yaml              split, features, model, seed=42, bootstrap; profiles official/pilot
data/reference/cells_land.csv      493 land cells → old province / new province / region (build_cell_map)
src/ingest/cds_jobs.py             submit and download ERA5 requests from the Copernicus CDS (several accounts)
src/ingest/build_cell_map.py       cell → province: 5×5 sample points per cell vs 63 province polygons, then 63 → 34 lookup
src/pipeline/cds_bronze.py         NetCDF (zip) → bronze Parquet, 493 cells only, CDS units and UTC unchanged
src/pipeline/cds_silver.py         bronze → silver: unit conversion, Vietnam time, valid ranges, quality report
src/pipeline/cds_gold.py           silver → gold: label, split by year, 33 features (M1 + atmosphere)
src/pipeline/cds_neighbors.py      neighbouring-cell features (M3)
src/features/                      local_weather (M1, label, split), atmos (M4) — shared by batch and streaming
src/analytics/eda_official.py      EDA on silver/gold → reports/official/eda
src/experiments/official_ablation.py   ablation M1–M4 (RF), daily block bootstrap
src/experiments/model_compare.py       logistic regression vs Random Forest vs GBT on M4
src/experiments/official_threshold.py  alert threshold by F1 / recall / precision on 2023
src/experiments/perf_official.py       performance experiment: shuffle partitions, broadcast vs sort-merge
src/mapreduce/                     Hadoop Streaming: climatology (mapper, combiner, reducer), export/validate
src/analytics/spark_sql_queries.py 3 Spark SQL queries + query plans
src/streaming/kafka_producer.py    replays silver into Kafka + injects controlled junk data
src/streaming/kafka_stream.py      Structured Streaming: checks, 6 h watermark, dedup, M4, MongoDB
src/streaming/stream_report.py     measurements + stream vs batch parity
src/web/                           FastAPI + Leaflet map (http://localhost:8000)
docker/                            Dockerfile.spark, Dockerfile.stream, stream-compose.yml
deck/                              slide source (pptxgenjs) → BDA501_Heavy_Rain_Forecast.pptx
report/                            final report (build_report.py → BDA501_Final_Report.docx/.md)
evidence/session9/                 Session 9 evidence (A–I, MANIFEST.md, team_audit/)
tests/                             leakage, metrics, MapReduce, parity
```

Large data lives outside the repository in `D:/BDA501_data/` (`raw/`, `lake/{bronze,silver,gold,models}`), mounted
into the containers as `/data`. Change it with `BDA_DATA_HOST`.

## Versions

Spark 4.2.0 (Scala 2.13, Java 17), Python 3.11, image `bda501-rain-spark:1` (built from the lab's Spark image);
streaming: `bda501-rain-stream:1` (+ connector `spark-sql-kafka-0-10_2.13:4.2.0`, kafka-python-ng, FastAPI),
Kafka `apache/kafka:3.9.1` (KRaft, 1 broker), MongoDB `mongo:7`. Python libraries: `requirements.txt`. Seed 42.

## Setup

```bash
docker build -t bda501-rain-spark:1 -f docker/Dockerfile.spark docker
bash scripts/stream_demo.sh build                     # streaming image
```

Downloading from CDS needs a Copernicus account and a `~/.cdsapirc` file (never committed).

## Reproduce the official results (from raw data)

```bash
# 1. Download ERA5 from CDS: submit requests, then download when CDS has processed them (one request = Vietnam box × 2 months × 13 variables)
python -m src.ingest.cds_jobs submit --years 2015 2025 --months-per-request 2 --manifest jobs_main.csv
python -m src.ingest.cds_jobs download --manifest jobs_main.csv
# 2. Cell → province mapping, terrain
python -m src.ingest.build_cell_map
python -m src.ingest.collect_geography --profile official
# 3. Lake: bronze → silver → gold (+ neighbours), EDA
python -m src.pipeline.cds_bronze --profile official
scripts/run_in_spark.sh src.pipeline.cds_silver --profile official
scripts/run_in_spark.sh src.pipeline.cds_gold --profile official
scripts/run_in_spark.sh src.pipeline.cds_neighbors --profile official
scripts/run_in_spark.sh src.analytics.eda_official --profile official
scripts/hdfs_put_official.sh                          # raw + bronze to HDFS (lab cluster)
# 4. Model: ablation, algorithm comparison, threshold, performance
scripts/run_in_spark.sh src.experiments.official_ablation --profile official
scripts/run_in_spark.sh src.experiments.model_compare --profile official
scripts/run_in_spark.sh src.experiments.official_threshold --profile official
scripts/run_in_spark.sh src.experiments.perf_official --profile official
# 5. Tests
scripts/run_in_spark.sh pytest -q tests
```

Outputs: `reports/official/` (quality, EDA, model, performance). Models are saved in `lake/models/official/<run_id>/`.

## Streaming (Kafka) and web

```bash
bash scripts/stream_demo.sh up       # Kafka, MongoDB, Spark, web; the producer sends a 72-hour warm-up, then waits
# open http://localhost:8000 → click "▶ Start stream" (1 data hour every 5 s; Spark micro-batch every 10 s)
bash scripts/stream_demo.sh reset    # delete topic, checkpoints and MongoDB to start again
bash scripts/stream_demo.sh down

bash scripts/stream_scenarios.sh     # measured scenario: junk data + Spark killed mid-run → reports/streaming/
```

This is a **replay of historical data (hindcast)**: ERA5 is published about 5 days late, so it cannot drive real
warnings; operation needs a numerical forecast source (see `evidence/session9/H_project_card.md`).

## MapReduce and Spark SQL (pilot data)

```bash
scripts/run_in_spark.sh src.mapreduce.mr_io export --profile pilot
scripts/run_mapreduce.sh                              # needs the BDA501-Hadoop-Lab cluster
scripts/run_in_spark.sh src.mapreduce.mr_io validate --profile pilot
scripts/run_in_spark.sh src.analytics.spark_sql_queries --profile pilot
```

The same mapper/combiner/reducer were also run locally on the full official training set
(`evidence/session9/team_audit/`: 34,561,272 rows, 5,916 keys, 0 differences).

## Slides and report

```bash
cd deck && npm install
DECK_NAME=BDA501_Heavy_Rain_Forecast.pptx NODE_PATH=$PWD/node_modules PPTX_SKILL=<pptx skill folder> node build_bilingual.js
python report/build_report.py
```

## Leakage controls (tested in `tests/`)

- Features at t use the window (t−k, t] in event time, partitioned by cell; a missing hour gives null, never 0.
- The label uses rain in (t, t+24h]; rows whose label window crosses a split boundary are dropped.
- Split train 2015–2022 / validation 2023 / test 2024–2025; class weights from train; algorithm and threshold chosen
  on validation and frozen before scoring test. The feature-set rule was applied on test; validation alone gives
  the same choice (M4).
- Streaming uses the same feature functions as batch; parity is checked on every cell-hour of the run.

## Status

| Item | Status |
|---|---|
| CDS download, lake, EDA, ablation, model comparison, threshold, streaming + web | run on the official data |
| MapReduce on YARN, Spark SQL Q1–Q3 | run and validated on the **pilot**; MapReduce also run locally on the official training set |
| Cluster/cloud deployment (YARN, multi-broker Kafka) | **design** |

## Data sources

| Data | Source | Licence |
|---|---|---|
| ERA5 hourly single levels | Copernicus Climate Data Store | Copernicus licence (credit C3S/ECMWF) |
| Province boundaries | geoBoundaries ADM1 | CC BY 4.0 |
| Elevation | OpenTopoData (SRTM 90 m) | SRTM terms |
| Reference rain gauges | NOAA GHCN-Daily | public domain |
