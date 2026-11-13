# Team audit checks (merged from the Card E pack prepared by a team member, 13/11/2026)

Measured, pure Python / numpy on the official raw CDS files and saved outputs (no Spark). Run from repo root with
`BDA_DATA=D:/BDA501_data`. That pack audited commit `2e3c5c3` (before Kafka, watermark, LR/GBT and the web map were
added in `1a760ec`); its statements about missing Kafka/watermark are superseded by `F_serving_stream.md`.

| Script | Output | Result |
|---|---|---|
| `scripts/s9_raw_label_check.py` | `out/raw_label_check.txt`, `out/province_label_definitions_test.csv` | labels recomputed from raw = Spark (diff 0 for train/val/test, 493/493 cells); `expver` = 0001 (ERA5 final) for every hour 2015–2025; 3 province-label definitions measured |
| `scripts/s9_mr_official_local.py` | `out/mapreduce_official_local.txt` | repo mapper/combiner/reducer on the full official train set (34,561,272 rows): 5,916 keys, 0 mismatches vs numpy — local Python, **not** Hadoop/YARN |
| `scripts/s9_e2_threshold.py` | `out/E2_threshold_tradeoff.*` | both thresholds, FP per extra TP = 9.67 |
| `scripts/s9_derive_validation.py` | `out/derive_validation.txt` | 23/23 reconciliation checks pass |
