# C — Spark query-plan / shuffle audit (measured, official gold)

Query (from `src/experiments/perf_official.py`): heavy-rain **cell-days** per new province × year on gold (47.5 M rows):
`gold → groupBy(location_id, date) sum(precipitation) → join cells (493 rows) on location_id → groupBy(new_province, year)
count(rain_day > 50 mm), max(rain_day)`. Formatted plans: `C_query_plan_broadcast.txt`, `C_query_plan_sortmerge.txt`
(`explain(mode="formatted")`).
Command: `scripts/run_in_spark.sh src.experiments.perf_official --profile official` (Spark 4.2 local[3], AQE off; also writes `reports/official/performance/plan_{broadcast,sortmerge}.txt`). Top result: Quảng Trị 2020 = 305 heavy cell-days (max 455 mm in one day).

| Check | Evidence / answer |
|---|---|
| Operation inspected | the query above, both join strategies |
| Where is Exchange / shuffle | broadcast plan: **Exchange (6)** after the partial HashAggregate (5) — hash-partitioned by (location_id, day); **Exchange (16)** before the final aggregate by (province, year); **BroadcastExchange (12)** ships the cell table. Sort-merge plan: same two Exchanges **plus** Exchange (8) re-partitioning the daily table by location_id and Exchange (14) for the cell table, each followed by a **Sort** |
| Join strategy | BroadcastHashJoin Inner BuildRight (cell table is the build side) when threshold = 10 MB; SortMergeJoin when broadcast is disabled |
| What determines partitioning | the grouping/join keys of each Exchange (hashpartitioning(location_id, day) / (location_id) / (new_province, year)) and `spark.sql.shuffle.partitions` = 16 (AQE off) |
| Would cache/persist help | not for a single pass: each run reads gold once. It would help if several queries reused the daily table (e.g. the 3 Spark SQL queries), at the cost of ~2 M rows in memory |
| Could broadcast help | yes in principle (removes Exchange 8 and 14 and both Sorts; shuffle 45.5 → 23.2 MB), condition: the small side fits in executor memory (cell table is a few KB). Measured: no time gain here because the join input is already small after aggregation (`B_benchmark.md`) |
| Is Parquet layout helping | yes: column pruning — `ReadSchema: struct<location_id, ts, precipitation>` reads 3 of 51 columns; `PushedFilters: IsNotNull(location_id)`. Year folders (`year=`) allow partition pruning when a query filters by year (training reads only 2015–2022); this query reads all years, so no pruning |

Other plans: the pilot Spark SQL queries Q1–Q3 (`reports/spark_sql/q*_plan.txt`) show the same patterns (group-by →
one Exchange; join with small tables → broadcast; window → Exchange by location then Sort), on pilot data.
