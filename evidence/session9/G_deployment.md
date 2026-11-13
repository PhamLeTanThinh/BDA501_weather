# G — Deployment, reproducibility and limitations

## 6.1 Deployable Spark architecture (DESIGN REVIEW — not run on a cluster)

| Element | Design |
|---|---|
| Cluster manager | Spark on YARN (course Hadoop cluster) or Kubernetes; managed option Dataproc / EMR |
| Driver placement | cluster mode on a worker node (no laptop driver); streaming driver supervised and restarted automatically |
| Executors / scaling | batch: dynamic allocation, 4 cores × 8 GB executors, scale with number of cells × years (work partitions by cell/year); streaming: 2 fixed executors |
| Storage / access path | lake (bronze/silver/gold/models) on HDFS or object store (s3a://), Parquet partitioned by year; streaming checkpoint on HDFS; MongoDB 3-node replica set |
| Fault tolerance | lineage recomputation for batch; HDFS replication 3; Kafka 3 brokers, replication factor 3; Structured Streaming checkpoint (offsets + state) + idempotent upsert by `cell|hour`; Mongo `w: majority`. Measured locally: Spark killed mid-run → 0 lost, 0 duplicates (`F_serving_stream.md`) |
| Observability | Spark UI / History Server, YARN UI, per-batch counters in MongoDB (`batch_stats`, `reject_stats`, `progress`), quality reports, web live counters |
| Security | CDS keys only in `~/.cdsapirc` (not in repo; repo scanned before push); Kerberos/IAM on cluster; Mongo auth + TLS; least-privilege service accounts for producer/stream/web |
| Cost / performance trade-off | batch on spot/preemptible workers (cheap, restartable from lineage); streaming on on-demand nodes (must stay up) |

Implemented locally today: Docker containers on one laptop (Spark local[3], Kafka 1 broker, MongoDB single node);
raw + bronze also on the lab HDFS.

## 6.2 Reproducibility smoke test

| Check | Status | Evidence |
|---|---|---|
| Software versions recorded | [x] | README "Phiên bản": Spark 4.2.0, Java 17, Python 3.11, Kafka 3.9.1, Mongo 7; `environment.json` of perf run |
| Dependencies / configuration recorded | [x] | `requirements.txt`, `docker/Dockerfile.spark`, `docker/Dockerfile.stream`, `docker/stream-compose.yml`, `configs/ablation.yaml` |
| Exact run commands | [x] | README "Chạy lại kết quả chính thức", "Streaming" |
| Deterministic seed | [x] | `seed: 42` (sampling, RF, GBT, bootstrap, producer junk) |
| README starts from raw data | [x] | step 1 = CDS submit/download |
| Key output reproducible without hidden steps | [~] | yes for lake → model → stream; CDS download needs personal accounts and queue time (hours); MapReduce needs the lab Hadoop cluster |
| No real credentials / private data | [x] | `.gitignore` excludes `.cdsapirc*`, `.env`, `data/`; repo scanned for keys/tokens before each push |

## 6.3 Limitations and risks

| Limitation / risk | Why it matters | Evidence / impact | Mitigation / future work |
|---|---|---|---|
| ERA5 is ~5 days late | no live warning possible with these inputs | 29 / 33 features unavailable at issue time (E1) | retrain on NWP analysis + station data; re-validate |
| Recall 46 % at F1 threshold | misses about half of heavy-rain cell-hours | 55,164 FN on test | recall-first threshold (74 % / 19 %), calibration, upwind features |
| Rain after landfall missed | Yagi replay: alerts on 6–7 Sep, misses much of 8–9 Sep | stream Yagi slides | features from 100–300 km / upwind |
| Label = ERA5 cell, not gauge | 25 % of gauge heavy days reach 50 mm in ERA5 | NOAA check | more gauges / radar for verification |
| Year-to-year shift | threshold tuned on dry 2023 | 0.66 % vs 1.21 % heavy | periodic re-tuning, drift monitoring |
| Single-node infrastructure | no proof of cluster scaling | Spark local, 1 Kafka broker, 1 Mongo | YARN + 3 brokers + replica set (design above) |
| MapReduce / Spark SQL on pilot data | not yet on official data | `reports/mapreduce/`, `reports/spark_sql/` dated 31/10 | rerun on official gold |
| Hyper-parameters not tuned | RF/GBT settings fixed | model_compare manifest | small grid search on validation |
