# Evidence manifest — Session 9 (Card E)

Labels: **MEASURED** = produced by a run, file listed · **INFERRED** = reasoning from code/docs · **DESIGN-ONLY** = not built.
Paths are relative to the repository root. Data: `D:/BDA501_data` mounted as `/data`. Spark 4.2.0 in Docker.

| File | Lab section | Content | Data / period | Command | Label |
|---|---|---|---|---|---|
| `A_architecture.md` + `A_architecture*.png` | 2 | corrected diagram [I]/[I*]/[P], data-item trace, challenge answers, deletion test | official run | deck build (`deck/build_bilingual.js`) | MEASURED + INFERRED |
| `B_benchmark.md`, `B_benchmark_runs.csv` | 3.1 | E1 shuffle partitions 4/16/64/200, E2 broadcast vs sort-merge; time, tasks, shuffle bytes | silver 60 cells 2015–2025 (5.79 M rows); gold 47.5 M rows | `scripts/run_in_spark.sh src.experiments.perf_official --profile official` | MEASURED |
| `C_query_plan.md`, `C_query_plan_{broadcast,sortmerge}.txt` | 3.2 | formatted plans: Exchanges, BroadcastHashJoin vs SortMergeJoin, column pruning | gold 47.5 M rows | same as B | MEASURED |
| `D_data_validation.md` | 4.1 | row counts bronze → silver → gold, duplicates, nulls, Open-Meteo cross-check, NOAA gauges | 2015–2025, 493 cells | `src.pipeline.cds_silver`, `cds_gold`, `src.analytics.eda_official` (README) | MEASURED |
| `E_model_validation.md` | 4.2, 4.3 | task, split, PR-AUC, baselines, ablation M1–M4, LR/RF/GBT, claims vs evidence | train 2015–2022, val 2023, test 2024–2025 | `src.experiments.official_ablation`, `official_threshold`, `model_compare` | MEASURED |
| `F_serving_stream.md`, `F_stream_report.md` | 5.1, 5.2 | MongoDB data model + read-back; Kafka/Structured Streaming with junk, crash at hour 110; parity; E3 | replay 01–10/09/2024, 493 cells, 125,939 messages | `bash scripts/stream_scenarios.sh` | MEASURED |
| `G_deployment.md` | 6.1–6.3 | YARN/K8s design, reproducibility check, limitations | — | — | DESIGN-ONLY (6.1) + MEASURED (6.2) |
| `H_project_card.md` | 7 (Card E) | E1 as-of table, E2 two thresholds + false-alarm cost, E3 parity, province labels, expver, watermark, MapReduce scope, 1-minute defense | test 2024–2025 | see rows above + `team_audit/` | E1 INFERRED · E2, E3 MEASURED |
| `I_readiness.md` | 8–10 | completeness checklist, red flags, self-score 82/100, top-3 fixes, Q&A | — | — | summary |
| `team_audit/` | 4.1, 7 | labels recomputed from raw (diff 0), expver = 0001, province-label definitions, MapReduce on full train set (5,916 keys, 0 diff), E2 trade-off, 23/23 reconciliation | raw CDS 2015–2025 | `python evidence/session9/team_audit/scripts/s9_*.py` (see `team_audit/README.md`) | MEASURED |
| `BDA501_Session9_Lab_Manual-v2_FILLED.docx` | 1–10 | filled worksheet (Card E) | — | — | summary |
| `reports/streaming/stream_report.{md,json}` | 5.2, 7 | full streaming measurements | as F | `stream_scenarios.sh` | MEASURED |
| `reports/official/model/*` | 4.2, 7 | ablation, thresholds, model comparison, manifests | as E | as E | MEASURED |
| `reports/mapreduce/validation.json` | 8 | Hadoop Streaming on YARN vs Spark SQL: 144/144 keys, 0 diff | **pilot** 12 points, 2019–2022 | `bash scripts/run_mapreduce.sh` | MEASURED (pilot) |
| `deck/BDA501_Heavy_Rain_Forecast.pptx` | 10 | includes slide "Session 9 Technical Readiness Summary" | — | `deck/build_bilingual.js` | — |
| `report/BDA501_Final_Report.docx` | 8 | final written report | — | `python report/build_report.py` | — |
