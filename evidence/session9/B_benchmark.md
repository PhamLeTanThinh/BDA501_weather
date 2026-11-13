# B — Controlled performance experiment (measured, official data)

Command: `SPARK_DRIVER_MEMORY=4g scripts/run_in_spark.sh src.experiments.perf_official --profile official`
Outputs: `reports/official/performance/{runs.csv, summary.csv, environment.json, plan_*.txt}` (copy: `B_benchmark_runs.csv`).

| Field | Entry |
|---|---|
| Question / hypothesis | H1: more shuffle partitions speed up the window-heavy feature build up to the core count, then add overhead. H2: broadcasting the small cell table avoids a shuffle and speeds up the province × year query. |
| Environment | 1 laptop, Docker (WSL2), Spark 4.2.0 `local[3]`, driver 4 GB, AQE **off**, Python 3.11 |
| Dataset / input size | E1: silver, 60 cells × 2015–2025 = **5,785,920 rows** (M4 features: windows 3–72 h, lags 24 h). E2: gold, all 493 cells = **47,540,976 rows** |
| Factor changed | E1: `spark.sql.shuffle.partitions` ∈ {4, 16, 64, 200}. E2: `spark.sql.autoBroadcastJoinThreshold` 10 MB (BroadcastHashJoin) vs −1 (SortMergeJoin), partitions fixed at 16 |
| Kept fixed | same input, same code, same container, AQE off, noop sink (computation only, no write I/O), 1 warm-up run discarded |
| Metric | wall time per run; tasks; shuffle write/read bytes (Spark REST API, per job group) |
| Expected trend | E1: minimum around 4–16 partitions (3 cores). E2: broadcast faster (no shuffle of the big side) |

## Measured results

| Run | Configuration | Input | Mean time (s) | Min–max (s) | Tasks | Shuffle write | Observation |
|---|---|---|---|---|---|---|---|
| E1 baseline | 200 partitions (Spark default) | 5.79 M rows | 36.9 | 36.0–37.8 | 232 | 508 MB | default too fine for 3 cores |
| E1 | 64 | 5.79 M | 38.0 | 37.9–38.0 | 96 | 508 MB | |
| E1 | 16 | 5.79 M | 39.9 | 33.6–46.2 | 48 | 508 MB | high variance (laptop noise) |
| E1 best | **4** | 5.79 M | **29.2** | 26.0–32.4 | 36 | 508 MB | ~21 % faster than default |
| E2 baseline | SortMergeJoin | 47.5 M rows | **30.4** | 29.8–31.4 | 127 | 45.5 MB | |
| E2 variant | BroadcastHashJoin | 47.5 M rows | 32.9 | 30.5–36.3 | 111 | **23.2 MB** | half the shuffle, not faster |

## Interpretation

1. **What changed and why.** Shuffle volume is identical (508 MB) for every partition count — only how it is split changes.
   With 3 cores, 4 partitions keep every core busy with the least scheduling overhead; 64–200 partitions create many
   small tasks (96–232) whose start-up cost outweighs any parallel gain. In E2, broadcasting removes the shuffle of the
   aggregated big side (45.5 → 23.2 MB) but the join happens *after* aggregation to ~2 M cell-days, so the run time is
   dominated by scanning and aggregating 47.5 M rows; the difference is within run-to-run noise — **H2 not confirmed**
   for this query.
2. **When would this configuration stop scaling well.** 4 partitions only fits 3 cores and ~6 M rows; on the full
   47.5 M rows or a cluster with dozens of cores, 4 partitions would leave cores idle and make each task's state too
   large (spill). Rule of thumb: partitions ≈ 2–3 × total executor cores, tuned by shuffle size (~100–200 MB per partition).
   Broadcast stops being safe when the small side no longer fits in executor memory.
3. **Why this cannot prove production scale.** One machine, 3 cores, shared disk and OS noise (16 → 46 s spread on one
   setting); no network shuffle between nodes, no HDFS reads, 2–3 repeats only. It shows the *direction* of the effect on
   our workload, not cluster throughput.

Status: **measured** (not design-only).
