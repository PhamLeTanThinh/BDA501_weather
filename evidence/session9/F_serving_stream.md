# F — NoSQL serving and streaming audit (measured, clean run 13/11/2026)

Command: `bash scripts/stream_scenarios.sh` (reset → Kafka + MongoDB + Spark + web → producer replays silver
01–10/09/2024 for 493 cells with injected junk → **Spark killed with SIGKILL at replay hour 110 for ~45 s, restarted
from checkpoint** → report). Full output: `F_stream_report.md` (= `reports/streaming/stream_report.md`), JSON in
`reports/streaming/stream_report.json`, slide data `stream_deck.json`.

## 5.1 NoSQL serving (MongoDB, database `rain_stream`)

| Question | Answer |
|---|---|
| Object written | one document per (cell, hour): province, region, lat/lon, probability `p`, threshold, `alert`, status (`ok` / `missing_history`), model version, `revisions`, and later `observed_24h_mm` + `label`. Also `province_risk` (max p and alert cells per province × hour), `stream_rejects` (quarantine, capped 2 MB), per-batch counters `batch_stats`, `reject_stats`, `progress` |
| Primary read-back queries | (1) all cells at the latest hour → map; (2) one cell over time → chart; (3) top provinces at hour t; (4) latest quarantined records |
| Key | `_id = "<cell>|<YYYY-MM-DD HH:MM>"`; indexes `(ts)` and `(cell, ts)` on predictions, `(ts)` on province_risk |
| Why it fits | every web request is a point/range lookup by hour or by cell; a self-contained document per cell-hour needs no join; upsert by a deterministic `_id` makes a replayed micro-batch overwrite instead of duplicating |
| Consistency / scalability trade-off | single-node Mongo here; in production a replica set with `w: majority` (CP: no lost acknowledged write on failover, reads pause ~10 s during an election); shard by cell if needed |
| Evidence write + read-back | 82,774 prediction documents, **0 duplicate (cell, hour)**; the web reads them every 2 s; sample document `g20.75_106.75|2024-09-06 19:00`: p 0.959, alert, observed 146.4 mm |

## 5.2 Streaming design

| Diagnostic | Answer |
|---|---|
| Nature of source | **replayed static data** (silver ERA5 hindcast), 1 hour of data / 5 s; not a live feed (ERA5 is ~5 days late) |
| Kafka topic and schema | `weather.silver.hourly`, 3 partitions, key = cell; JSON `{grid_id, event_time (ISO, +07:00), 14 silver variables, msg_id}` |
| Source → processing → sink | producer → Kafka → Spark (2 queries: `rejects` → MongoDB quarantine; `forecast` → watermark + dedup → foreachBatch: 120 h history buffer, M4 features, RF M4 → MongoDB upsert) → FastAPI web |
| Checkpoint and recovery | `/data/lake/stream_kafka/checkpoints/{forecast,rejects}`: offsets + dedup state + commit log. Killed mid-run: **125,939 messages in topic = 125,939 read by each query**, 0 duplicate forecasts |
| Event time vs processing time | event time (`event_time` of the weather hour) — features and labels are defined on it; processing time only drives the 10 s trigger |
| Late data | yes (injected 1–4 h late, out-of-order hours, 12 h late). ≤ 6 h: accepted and the affected hours recomputed (`revisions` + 1). > 6 h: dropped if the watermark has advanced (measured 20 of 89 dropped — during bursts the watermark lags, see note) |
| Duplicates / replay | `dropDuplicatesWithinWatermark(cell, event_time)` + dedup again when merging into the history buffer + upsert by `_id`; per-batch counters keyed by batch id. Measured: **5,766 / 5,766** duplicates removed, 0 duplicates reaching foreachBatch |
| State / window / watermark | needed: dedup state bounded by a 6 h watermark; features need 72 h windows, kept in our own 120 h buffer |
| Latency requirement | a heavy-rain warning is useful hours ahead; hourly freshness is enough. With the replay, freshness is bounded by the source: in production by the NWP/observation publication delay, not by Kafka |

Watermark note: Spark's watermark = max event time of **previous** micro-batches − 6 h. When many hours arrive in one
batch (72 h warm-up, 20 h burst, catch-up after the crash) a "12 h late" message can land before the watermark has
moved and is accepted. Spark only guarantees to keep data < 6 h late. Correctness is protected by recomputation + upsert.

## E3 — stream vs batch (Card E)

| Check | Result |
|---|---|
| Probability parity, all cell-hours | **81,710 / 81,710** identical (stored to 4 decimals), 0 alert differences, 0 scored by only one side |
| Feature-vector parity, 4 sample cells (Hải Phòng, Quảng Ninh, Huế, Nghệ An) | **658 cell-hours × 33 features = 21,714 values**, 0 mismatches vs batch on the same accepted rows (max |diff| = 0); vs clean silver 0 mismatches (max |diff| 6.8e−5, float rounding) |
| Data the stream lost | 50 cell-hours (30 never sent + 20 dropped as too late) ⇒ 1,114 cell-hours that batch on clean silver can score but the stream marks `missing_history` (72 h windows incomplete) |
| Documented junk events, sample cells | 115 events with event times in MongoDB `junk_log`: 35 duplicate, 21 duplicate 1–3 h later, 29 late 1–4 h, 28 out of order, 2 late 12 h |

Example (cell g20.75_106.75, Hải Phòng):

| Event time | Injected | Sent with replay hour # | Outcome |
|---|---|---|---|
| 2024-09-01 03:00 | duplicate | 3 | second copy dropped by dedup |
| 2024-09-01 07:00 | late 3 h | 10 (event = hour 7) | accepted, hours ≥ 07:00 recomputed |
| 2024-09-04 08:00 | out of order (hour 81 sent before 80) | 81 | accepted |
| 2024-09-06 19:00 | duplicate 1 h later | 140 | dropped; document stays p 0.959, alert |
