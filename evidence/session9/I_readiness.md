# I — Final completeness check and Session 10 readiness

## 8. Required items

| Required item | Ready? | Evidence |
|---|---|---|
| Problem & Big Data justification | [x] | 47.5 M cell-hours × 33 features, 11 years, 493 cells; hourly velocity in the stream; NetCDF + Parquet + JSON + documents (deck "4V", README) |
| End-to-end architecture, implemented vs proposed | [x] | deck slide "Architecture overview" with [I] / [I*] / [P] tags; `A_architecture.md` |
| Project-related MapReduce | [~] | climatology on YARN, combiner, 0/144 diff vs Spark SQL — **pilot data** (labelled) |
| PySpark DataFrames / Spark SQL | [x] | silver/gold pipeline on Spark; 3 Spark SQL queries (pilot) + province × year query on official data (`C_query_plan.md`) |
| Curated Parquet / columnar output | [x] | gold partitioned by year, float32 features; column pruning shown in plan |
| ≥ 2 analytical queries + formatted plan | [x] | Q1–Q3 (pilot) + official province × year query; `C_query_plan_*.txt` |
| Performance / scalability experiment | [x] | `B_benchmark.md` (shuffle partitions, broadcast vs sort-merge), deck slide E6 |
| Spark ML / large-scale analytics | [x] | ablation M1–M4 + 3 algorithms, temporal split, bootstrap CI (`E_model_validation.md`) |
| NoSQL serving | [x] | MongoDB upsert by cell\|hour, read-back by web (`F_serving_stream.md`) |
| Streaming consideration | [x] | Kafka + Structured Streaming, checkpoint, watermark, dedup, junk + crash test (`F_serving_stream.md`) |
| Cloud/cluster deployment design | [x] | `G_deployment.md` 6.1 (design review) |
| Validation | [x] | `D_data_validation.md`, `E_model_validation.md` |
| Reproducibility | [x] | README from raw CDS, versions, seed 42 (`G_deployment.md` 6.2) |
| Final report + README + code package | [~] | README + code on GitHub `main`; written report to be confirmed by the team |
| Team contribution statement | [x] | deck "Conclusions and team contributions" |
| Demo evidence | [x] | `scripts/stream_demo.sh up` → web map; Spark UI |

## Red-flag checklist

| Red flag | Status |
|---|---|
| Technologies the team cannot explain / does not use | No — each component tagged and explained (`A_architecture.md`, deletion test) |
| "Real-time" claim based only on replay | Addressed — the stream is labelled a hindcast replay; ERA5 ~5 days late (`H_project_card.md` E1) |
| Target computed from model features | No — label = rain (t, t+24h], features use hours ≤ t (unit tests) |
| Split / features expose the future | No — split by year, boundary purge, model + threshold chosen on 2023 |
| Metric does not match the task | No — PR-AUC + precision/recall for a rare-event classifier |
| NoSQL justified only as "faster" | No — access patterns: latest hour map, per-cell series, top provinces, quarantine |
| No controlled performance result | Fixed — `B_benchmark.md` |
| Cannot reproduce from README | Fixed — README rewritten for the official pipeline |
| Implemented and proposed mixed on diagram | Fixed — [I]/[P] tags, dashed = design |

## 9. Self-assessment (diagnostic, not the lecturer's grade)

| Criterion | Weight | Self-score | Gap / action |
|---|---|---|---|
| Accuracy & correctness | 25 | 21 | recall 46 % stated honestly; MapReduce/SQL still pilot |
| Creativity & problem-solving | 15 | 12 | ablation with CIs, junk-injection stream test, cell→province mapping |
| Completeness of product | 25 | 21 | end-to-end implemented; cluster deployment design-only |
| Presentation & expression | 10 | 8 | bilingual deck, live web demo |
| Application of theory | 20 | 16 | shuffle/partition/broadcast measured; CAP and watermark semantics explained |
| Timeliness & requirements | 5 | 4 | written report status to confirm |
| **Total** | 100 | **82** | |

## Top three corrections before Session 10

| Priority | Weakness | Fix | Done? |
|---|---|---|---|
| 1 | no measured performance result | ran `perf_official` on official data, deck slide E6 rewritten | [x] |
| 2 | README described the pilot only | README rewritten (raw CDS → model → stream), code merged to `main` | [x] |
| 3 | stream = batch shown only on probabilities | 33-feature vector parity on 4 sample cells + junk log (`F_serving_stream.md`) | [x] |

## Likely Q&A

**Q:** *Your model misses more than half of heavy-rain cases. What can you safely claim?*
**A:** As a hindcast on ERA5 it ranks risk far better than simple rules (PR-AUC 0.420 vs 0.147 persistence, 0.012 random),
so it is a decision-support layer that flags cells for forecasters, not a stand-alone warning. The operating point is a
policy choice: the recall-first threshold chosen on 2023 catches 74 % of heavy-rain cell-hours at 19 % precision. For live
use the model must be retrained on operational inputs, because ERA5 is published about 5 days late.
