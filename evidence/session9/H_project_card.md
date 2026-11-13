# H — Card E: Heavy-Rain Forecasting for 34 Vietnam Provinces

Status labels: **measured** = computed from our data/run; **inferred** = reasoning from documented facts; **design-only** = not built.

## E1 — Forecast clock (as-of table)

Proposed issue time **T** (any hour), horizon **h = 24 h**: forecast "rain in (T, T+24h] > 50 mm" for each of 493 cells.
Our features only use hours ≤ T (unit-tested), so the model is **admissible as a hindcast**. The question here is
whether each input is *published* by T in real operation. ERA5 (and its preliminary ERA5T) is released about
**5 days after** the valid time, so no ERA5 input is available at T.

| Feature (count) | Event time used | Publication / availability | Horizon | Admissible at T (operational)? | Replacement source for production |
|---|---|---|---|---|---|
| Rain windows: `rain_3h`, `rain_24h`, `rain_72h`, `rain_max_1h_24h`, `rain_hours_24h` (5) | (T−72h, T] | ERA5/ERA5T: ≈ T + 5 days | 24 h | **No** | gauge/radar rain (VNMHA), or NWP analysis precipitation (GFS/ECMWF IFS, ≈ T + 4–6 h) |
| Surface state now: `rh`, `dew_point`, `dewpoint_depression`, `pressure_msl`, `wind_speed`, `wind_dir_sin/cos`, `temperature`, `cloud_cover` (9) | T | ≈ T + 5 days | 24 h | **No** | station observations (synoptic, hourly) or NWP analysis |
| Surface trends: `rh_mean_6h`, `cloud_mean_6h`, `pressure_change_3h/24h`, `wind_speed_change_24h`, `temp_change_24h` (6) | (T−24h, T] | ≈ T + 5 days | 24 h | **No** | same as above |
| Atmosphere: `tcwv`, `tcwv_change_24h`, `ivt`, `ivt_dir_sin/cos`, `vimd_sum_6h`, `cape`, `cape_max_24h`, `k_index` (9) | (T−24h, T] | ≈ T + 5 days | 24 h | **No** | NWP analysis fields: GFS has PWAT, CAPE, K-index; IVT/VIMD must be computed from model levels |
| Calendar: `month_sin/cos`, `hour_sin/cos` (4) | T | known at T | 24 h | **Yes** | — |
| Label: rain (T, T+24h] | future | ≈ T + 24 h + 5 days | — | target only (evaluation) | gauge totals for verification |

**Verdict (inferred):** 29 of 33 features are ERA5-only and would not exist at issue time. The streaming demo is a
**replay/hindcast** of 01–10/09/2024, not a live early-warning service. For production the model must be retrained
and re-validated on an operational source (NWP analysis + observations); the NWP analysis delay (~4–6 h) makes the
effective lead time ~18–20 h instead of 24 h. Nothing from 2024–2025 is used for features, thresholds or model choice
(train 2015–2022, threshold and model chosen on 2023; checked in `src/experiments/*` and `tests/test_no_leakage.py`).

## E2 — Warning trade-off (measured, test 2024–2025, 8,462,129 cell-hours, 102,878 heavy)

Both thresholds were chosen on validation 2023 only, then frozen. Source: `reports/official/model/m4_threshold_choice.csv`.

| Threshold rule (chosen on 2023) | Threshold | TP | FP | FN | Precision | Recall | Missed heavy cell-hours |
|---|---|---|---|---|---|---|---|
| Best F1 (current) | 0.932 | 47,714 | 60,688 | 55,164 | 44.0% | 46.4% | 55,164 (53.6%) |
| Recall ≥ 60% on 2023 (recall-first) | 0.739 | 75,687 | 331,316 | 27,191 | 18.6% | 73.6% | 27,191 (26.4%) |

Reference only (thresholds read on test, NOT used for selection): 0.85 → recall 63%, precision 28%; 0.90 → 55% / 36%.

**Decision:** keep 0.932 as the reported operating point (balanced) and offer 0.739 as a recall-first mode.
Moving to recall-first halves the missed cell-hours (−27,973) at the cost of +270,628 false alarms (≈ 5.5× more).
The operating point is a disaster-management decision (cost of a miss vs cost of an alarm), not only a modelling one.

**One-minute defense — safe operational claim:** at the F1 threshold the model misses about half of the heavy-rain
cell-hours, so it must **not** be presented as a stand-alone warning system. The safe claim is: *as a hindcast on ERA5
it ranks risk well (PR-AUC 0.420 vs 0.147 persistence and 0.012 random) and can serve as a decision-support layer that
flags cells for forecasters; switching to the recall-first threshold catches ~74% of heavy-rain cell-hours with many
more false alarms.*

## E3 — Stream vs batch (measured)

See `F_serving_stream.md` and `reports/streaming/stream_report.md` (section "E3 feature-vector parity"): feature
vectors (33 features) of 4 sample cells are compared between the stream (as finally computed in foreachBatch after any
late data) and batch recomputation, together with a log of injected duplicate / late / out-of-order events with their
event times (`junk_log` in MongoDB).

Watermark semantics (inferred from Spark docs and our run): the 6 h watermark is relative to the **maximum event time
seen in previous micro-batches**, not wall-clock. During bursts (warm-up, 20-hour burst) some messages more than 6 h late
arrived before the watermark had advanced and were accepted (clean run: 89 injected, 20 dropped; an earlier run dropped 44 — it depends on how batches fall). Replay speed therefore changes
which late events survive; correctness of the outputs is protected by recomputation + upsert, not by the watermark.

## Added checks (measured, `team_audit/`)

- **Reanalysis revisions:** the `expver` variable in all 68 raw files is `0001` (ERA5 final) for every hour 2015–2025,
  no `0005` (ERA5T). Train, validation and test use the same product version; no later revision leaks into training.
  Residual risk: operation would use ERA5T/NWP with a different distribution (E1).
- **Labels from raw:** recomputed with numpy from the NetCDF files: train 313,169 / val 28,683 / test 104,439 positives,
  identical to Spark (diff 0, 493/493 cells).
- **False-alarm cost (E2):** moving from 0.932 to 0.739 buys +27,973 TP for +270,628 FP = **9.7 false alarms per extra
  heavy-rain cell-hour caught**. Decision rule: use the recall-first threshold only if one missed event costs more than
  ~10 false alarms. Two-level use: "watch" at p ≥ 0.739, "warning" at p ≥ 0.932.
- **Province label definitions (test 2024–2025, 34 provinces × 17,520 h = 595,680 province-hours):**

| Definition | Positive province-hours | Lâm Đồng (35 cells) | Gia Lai (31) | Hưng Yên (3) | Hải Phòng (4) |
|---|---|---|---|---|---|
| any-area (MAX, ≥ 1 cell > 50 mm) — **used** | 20,138 | 355 | 878 | 366 | 330 |
| p90 of cells > 50 mm | 13,783 | 91 | 453 | 352 | 312 |
| coverage ≥ 25 % of cells | 9,918 | 31 | 243 | 366 | 330 |

  The definition changes alerted province-hours ~2× nationally and 11× for Lâm Đồng, almost nothing for 3–4-cell
  provinces ⇒ we keep any-area (do not miss local extremes) and publish the share of alerted cells next to it.
- **MapReduce on the full official train set:** the repo's mapper/combiner/reducer run locally in Python on 34,561,272
  rows: combiner → 47,328 records, 5,916 keys (493 × 12), 0 mismatches vs numpy (not Hadoop/YARN; the YARN run
  remains pilot).
- **Feature-set selection — correction:** `selection.py` applies the "CI above zero" rule on the **test** bootstrap,
  so the feature set was chosen on test. Re-checked on validation 2023 only: M1 0.172, M2 0.176, M3 0.171, **M4 0.210**
  ⇒ the same choice (M4). Algorithm and thresholds were chosen on validation.

## Other Card E prompts

- **Province label:** province risk = **MAX** probability over its cells; a province is alerted if **any** cell is above the
  threshold (`province_risk` collection: `p_max`, `alert_cells / cells`). This answers "is any part of the province at risk",
  not "is the whole province wet"; large provinces (e.g. Gia Lai 26 cells, Lâm Đồng 35) will alert more often than small ones.
- **MapReduce equivalence:** 0 / 144 differences vs Spark SQL is measured on the **pilot sample** (12 cities, 2019–2022),
  not on the full official data.

Card result: **[x] Verified** (E2, E3) · **[x] Design-only** (operational inputs, E1) · Evidence: this file, `F_serving_stream.md`.
