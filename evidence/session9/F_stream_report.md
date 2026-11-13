# Streaming run: measurements and parity

Kafka messages in topic: 125,939 · read by forecast query: 125,939 · read by rejects query: 125,939 → no loss, no double read: **True**

| Case | Injected | Handled by Spark | How |
|---|---|---|---|
| duplicate (immediate + later) | 5,766 | 5,766 | watermark + dropDuplicatesWithinWatermark |
| late within 6 h + out of order | 6,984 | 6,914 | accepted, affected hours recomputed |
| late beyond 6 h | 89 | 20 | dropped by watermark |
| malformed JSON | 599 | 599 | quarantined |
| missing field | 383 | 383 | quarantined |
| out-of-range value | 575 | 575 | quarantined |
| unknown cell | 326 | 326 | quarantined |
| missing hours (never sent) | 30 | – | features null → missing_history |

Rows: {"sent": 125939, "valid_into_forecast": 118270, "rejected_records": 1883, "dropped_duplicates": 5766, "dropped_late": 20, "dropped_total": 7669}
MongoDB: {"prediction_docs": 82774, "status_ok": 81710, "status_missing_history": 1064, "duplicate_cell_hours": 0}
Accepted rows: {"rows": 118270, "distinct_cell_hours": 118270, "duplicates_reaching_foreachBatch": 0}
Parity (stream vs batch on the same accepted rows; p is stored rounded to 4 decimals, so |diff| ≤ 5e-5 means equal): {"cell_hours_both_scored": 81710, "max_abs_diff_p": 4.9998850551680185e-05, "identical_p": 81710, "alert_mismatch": 0, "scored_only_stream": 0, "scored_only_batch": 0}
Stream vs clean silver: {"cell_hours_both_scored": 81710, "max_abs_diff_p": 4.9998850551680185e-05, "identical_p": 81710, "alert_mismatch": 0, "scored_only_stream": 0, "scored_only_silver": 1114}
Cell-hours missing from accepted vs silver: 50

## E3 feature-vector parity (sample cells)

```
{
 "cells": [
  "g16.50_107.50",
  "g19.25_105.50",
  "g20.75_106.75",
  "g21.00_107.00"
 ],
 "stream_cell_hours": 658,
 "recomputed_cell_hours": 0,
 "same_accepted_rows": {
  "cell_hours_compared": 658,
  "features_per_vector": 33,
  "values_compared": 21714,
  "cell_hours_with_any_mismatch": 0,
  "max_abs_diff": 0.0,
  "features_with_mismatch": [],
  "examples_mismatch": []
 },
 "clean_silver": {
  "cell_hours_compared": 658,
  "features_per_vector": 33,
  "values_compared": 21714,
  "cell_hours_with_any_mismatch": 0,
  "max_abs_diff": 6.775767815270228e-05,
  "features_with_mismatch": [],
  "examples_mismatch": []
 },
 "junk_log_events": 115,
 "junk_log_by_kind": {
  "duplicate": 35,
  "duplicate_later_1h": 10,
  "duplicate_later_2h": 7,
  "duplicate_later_3h": 4,
  "late_1h": 3,
  "late_2h": 9,
  "late_3h": 6,
  "late_4h": 11,
  "out_of_order": 28,
  "too_late_12h": 2
 },
 "junk_log_sample": [
  {
   "cell": "g16.50_107.50",
   "event_time": "2024-09-02T06:00:00+07:00",
   "kind": "duplicate",
   "sent_at_replay_index": 30
  },
  {
   "cell": "g16.50_107.50",
   "event_time": "2024-09-03T18:00:00+07:00",
   "kind": "duplicate_later_1h",
   "sent_at_replay_index": 67
  },
  {
   "cell": "g16.50_107.50",
   "event_time": "2024-09-03T23:00:00+07:00",
   "kind": "duplicate",
   "sent_at_replay_index": 71
  },
  {
   "cell": "g16.50_107.50",
   "event_time": "2024-09-04T02:00:00+07:00",
   "kind": "late_2h",
   "sent_at_replay_index": 76
  },
  {
   "cell": "g16.50_107.50",
   "event_time": "2024-09-04T08:00:00+07:00",
   "kind": "out_of_order",
   "sent_at_replay_index": 81
  },
  {
   "cell": "g16.50_107.50",
   "event_time": "2024-09-04T08:00:00+07:00",
   "kind": "duplicate_later_2h",
   "sent_at_replay_index": 83
  },
  {
   "cell": "g16.50_107.50",
   "event_time": "2024-09-04T09:00:00+07:00",
   "kind": "duplicate",
   "sent_at_replay_index": 80
  },
  {
   "cell": "g16.50_107.50",
   "event_time": "2024-09-04T10:00:00+07:00",
   "kind": "late_2h",
   "sent_at_replay_index": 84
  },
  {
   "cell": "g16.50_107.50",
   "event_time": "2024-09-04T21:00:00+07:00",
   "kind": "duplicate",
   "sent_at_replay_index": 93
  },
  {
   "cell": "g16.50_107.50",
   "event_time": "2024-09-05T02:00:00+07:00",
   "kind": "duplicate_later_2h",
   "sent_at_replay_index": 100
  },
  {
   "cell": "g16.50_107.50",
   "event_time": "2024-09-05T08:00:00+07:00",
   "kind": "late_2h",
   "sent_at_replay_index": 106
  },
  {
   "cell": "g16.50_107.50",
   "event_time": "2024-09-05T09:00:00+07:00",
   "kind": "out_of_order",
   "sent_at_replay_index": 106
  }
 ]
}
```
