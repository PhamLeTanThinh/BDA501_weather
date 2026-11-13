# E — ML / analytics validation (measured, official data)

Sources: `reports/official/model/ablation_results.csv`, `ablation_deltas.csv`, `model_compare_results.csv`,
`model_compare_deltas.csv`, `m4_threshold_choice.csv`, manifests. Commands in README "Model".

| Question | Answer / evidence |
|---|---|
| Task | Binary classification per cell-hour: will rain in the next 24 h exceed 50 mm (QĐ 18/2021)? 493 land cells, forecasts rolled up to 34 provinces by MAX. |
| Features and availability before the outcome | 33 features (M4): 24 local weather (rain 3/24/72 h, humidity, pressure and trends, wind, temperature, cloud, month, hour) + 9 atmosphere (water vapour, moisture transport and convergence, CAPE, K-index). All use hours ≤ t only (unit-tested). Operationally, ERA5 is published ~5 days late ⇒ hindcast only (`H_project_card.md` E1). |
| Split / validation | Temporal by year: train 2015–2022, validation 2023, test 2024–2025; rows whose label window crosses a boundary are purged. Feature set, algorithm and threshold chosen on validation; test touched once. Appropriate because weather is autocorrelated in time — a random split would leak neighbouring hours. |
| Metric | PR-AUC (heavy rain is 1.2 % of test cell-hours; accuracy of "always no" = 98.8 %, ROC-AUC of M4 = 0.955 looks perfect). Plus precision / recall / F1 at the chosen threshold; 95 % CI by block bootstrap over days (500 resamples, paired). |
| Baseline / comparison | Random = 0.012; persistence (rain last 24 h) = 0.147; feature ablation M1 0.361, M2 0.363, M3 0.363, **M4 0.420**; algorithms on M4: logistic 0.368, **RF 0.420**, GBT 0.412 (validation 0.180 / **0.210** / 0.200). |
| What would show the model is not useful | PR-AUC not clearly above persistence, or CI of the difference crossing 0. Observed: M4 − M1 +0.059 [0.029, 0.086]; RF − logistic +0.052 [0.022, 0.077]; GBT − RF −0.006 [−0.027, 0.013] (tie). |
| Largest leakage / bias / evaluation risk | (1) ERA5 inputs not available in real time ⇒ results are hindcast skill. (2) Label is ERA5 cell rain (25 % agreement with gauge heavy days). (3) Year-to-year shift: 2023 (0.66 %) was dry, 2024–2025 wet (1.11 %, 1.31 %) ⇒ threshold tuned on a dry year. (4) Probabilities not calibrated (sampling 3 % negatives + weight ×3.26). |

## Warning trade-off (test 2024–2025)

| Threshold (chosen on 2023) | TP | FP | FN | Precision | Recall |
|---|---|---|---|---|---|
| 0.932 best F1 | 47,714 | 60,688 | 55,164 | 44.0 % | 46.4 % |
| 0.739 recall-first | 75,687 | 331,316 | 27,191 | 18.6 % | 73.6 % |

## Evidence vs claim

| Claim for Session 10 | Evidence | What it does NOT prove |
|---|---|---|
| Atmosphere features improve heavy-rain skill | M4 − M1 +0.059, 95 % CI above 0 on 8.46 M test cell-hours | That the gain holds with operational NWP inputs |
| Random Forest is the best of the three algorithms we tried | highest validation PR-AUC; beats logistic (CI > 0) | That RF beats tuned GBT or deep models — GBT is statistically tied, no tuning was done |
| The model ranks risk far better than simple rules | PR-AUC 0.420 vs 0.147 persistence, 0.012 random | That it is a reliable stand-alone warning system (recall 46 % at F1 threshold) |
| Stream gives the same forecasts as batch | 79,993 / 79,993 cell-hours identical, 0 alert differences; feature-vector check in `F_serving_stream.md` | Real-time performance with live data |
