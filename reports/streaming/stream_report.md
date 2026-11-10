# Streaming run: measurements and parity

Kafka messages in topic: 125,939 · read by forecast query: 125,939 · read by rejects query: 125,939 → no loss, no double read: **True**

| Case | Injected | Handled by Spark | How |
|---|---|---|---|
| duplicate (immediate + later) | 5,766 | 5,766 | watermark + dropDuplicatesWithinWatermark |
| late within 6 h + out of order | 6,984 | 6,889 | accepted, affected hours recomputed |
| late beyond 6 h | 89 | 44 | dropped by watermark |
| malformed JSON | 599 | 599 | quarantined |
| missing field | 383 | 383 | quarantined |
| out-of-range value | 575 | 575 | quarantined |
| unknown cell | 326 | 326 | quarantined |
| missing hours (never sent) | 30 | – | features null → missing_history |

Rows: {"sent": 125939, "valid_into_forecast": 118246, "rejected_records": 1883, "dropped_duplicates": 5766, "dropped_late": 44, "dropped_total": 7693}
MongoDB: {"prediction_docs": 82750, "status_ok": 79993, "status_missing_history": 2757, "duplicate_cell_hours": 0}
Accepted rows: {"rows": 118246, "distinct_cell_hours": 118246, "duplicates_reaching_foreachBatch": 0}
Parity (stream vs batch on the same accepted rows; p is stored rounded to 4 decimals, so |diff| ≤ 5e-5 means equal): {"cell_hours_both_scored": 79993, "max_abs_diff_p": 4.9998850551680185e-05, "identical_p": 79993, "alert_mismatch": 0, "scored_only_stream": 0, "scored_only_batch": 0}
Stream vs clean silver: {"cell_hours_both_scored": 79993, "max_abs_diff_p": 4.9998850551680185e-05, "identical_p": 79993, "alert_mismatch": 0, "scored_only_stream": 0, "scored_only_silver": 2831}
Cell-hours missing from accepted vs silver: 74
