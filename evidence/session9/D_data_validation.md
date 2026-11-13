# D — Data-processing validation (measured, official data 2015–2025, 493 cells)

Sources: `reports/official/quality/silver_summary.json`, `gold_summary.json`, `silver_cell_year_completeness.csv`,
`reports/official/eda/crosscheck_openmeteo.csv`, `reports/official/deck_official.json` (e0), `reports/official/model/ablation_manifest.json`.
Commands: `scripts/run_in_spark.sh src.pipeline.cds_silver --profile official` (and `cds_gold`, `eda_official`).

| Validation item | Method | Evidence / result | Pass? |
|---|---|---|---|
| Schema / data types | silver written with explicit columns and units (mm, °C, %, hPa, km/h, °); gold features cast to float32; Spark reads Parquet schema | 14 weather columns + grid_id, ts, ts_epoch, year; gold 51 columns | [x] |
| Null / invalid values | physical range per variable → out of range set to null, row kept; nulls counted after rules | 0 nulls except `cape` 2 and `k_index` 243 of 47.5 M rows; missing hour ⇒ window feature null, never 0 mm | [x] |
| Duplicate handling | count (cell, hour) keys with count > 1; `assert dup == 0` stops the job | silver 0, gold 0 duplicate keys | [x] |
| Row-count reconciliation | bronze → silver → gold counts | bronze 47,552,808 → silver 47,540,976 (−11,832 = 493 cells × 24 h outside 2015-01-01 00:00 … 2025-12-31 23:00 VN after UTC→VN shift) → gold 47,540,976; 5,423 cell-years, 0 incomplete | [x] |
| Join correctness / unmatched keys | cells map join: 493 cells in data = 493 in map; ablation cohort requires every M1–M4 feature | 0 unmatched cells; 10 edge cells dropped from the ablation cohort because neighbour features need ≥ 2 land neighbours (483 cells) | [x] |
| Independent sample check / known answer | (1) same ERA5 via Open-Meteo at 63 points; (2) 14 NOAA rain gauges | (1) daily rain correlation 0.9998, hourly 0.9996; correlation is highest at lag 0 h (0.989) vs ±1 h (0.70) ⇒ UTC→VN shift correct. (2) gauge/ERA5 totals ratio 1.04; ERA5 > 50 mm on 292 of 1,185 gauge heavy days (25%) ⇒ label = ERA5 cell heavy rain, not point rain | [x] |
| Curated output can be read back reproducibly | gold read by EDA, ablation, model_compare, threshold, perf and stream report jobs; same counts each time | gold 47,540,976 rows / split counts identical across jobs (`gold_summary.json`, ablation cohort) | [x] |

Split check (gold): train 2015–2022 34,561,272 rows (0.906 % heavy), validation 2023 4,306,848 (0.666 %),
test 2024–2025 8,637,360 (1.209 %), 35,496 rows excluded because their 24 h label window crosses a split boundary.

Unit tests: `scripts/run_in_spark.sh pytest -q tests` — future data cannot change past features, exact label window,
split purge, metric correctness, MapReduce mapper/reducer, stream–batch feature parity.
