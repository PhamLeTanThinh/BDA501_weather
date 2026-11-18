# Final demo script (Session 10)

Order follows the Session 10 requirements: architecture → data path → MapReduce → Spark DataFrame/SQL → ML →
NoSQL serving → streaming and deployment. Slides: `deck/BDA501_Final_Presentation.pptx` (slide "Live demo").

## 15 minutes before

```bash
bash scripts/demo_check.sh        # Docker, data, HDFS/YARN, evidence files; resets and warms up the stream
```

Wait for `ALL CHECKS PASSED` and `stream warmed up` (takes ~6 min). The script stops the YARN ResourceManager and
NodeManager after checking them, to leave memory for the stream; the finished job stays visible in the job history. Open these tabs in the browser:

| Tab | URL |
|---|---|
| Web map (stream) | http://localhost:8000 |
| Spark UI (stream job) | http://localhost:4040 |
| MapReduce job history | http://localhost:19888 |
| HDFS NameNode | http://localhost:9870 → Utilities → Browse → /rain-forecast |

If a check fails and cannot be fixed in 5 minutes, present from `report/demo/` (one screenshot per step).

## During the demo (~8 minutes)

| # | Show | Where | Say (one line) |
|---|---|---|---|
| 1 | Data path | HDFS browser `/rain-forecast/raw`, `/rain-forecast/bronze`; `reports/official/quality/gold_summary.json` | 68 CDS zips → 47.55 M bronze rows → 47.54 M silver/gold rows, 0 duplicate keys |
| 2 | MapReduce on YARN | job history (19888): job `rain_climatology_location_month`, counters; `reports/official/mapreduce/validation.json` | map → combine → shuffle → reduce on 34.6 M official training rows; combiner cuts the shuffle; Spark SQL gives the same 5,916 keys |
| 3 | Spark SQL + plan | `reports/official/spark_sql/q2_heavy_days_province_year_plan.txt` | Exchange = shuffle; BroadcastHashJoin with the 493-row cell table |
| 4 | Spark ML | slide "E1" and "Where recall is lost" | PR-AUC 0.420 vs 0.147 persistence; recall 46% overall, 63% on the central coast, 11% in the South |
| 5 | Streaming → MongoDB → web | http://localhost:8000 → press **▶ Start**; Spark UI micro-batches | junk is quarantined, duplicates dropped, map fills hour by hour; read one document back: |

```bash
docker compose -f docker/stream-compose.yml exec -T mongo mongosh --quiet rain_stream \
  --eval 'db.predictions.findOne({cell:"g20.75_106.75", status:"ok"}, {_id:1,p:1,alert:1,province:1,threshold:1})'
```

Optional (if asked about fault tolerance): `docker compose -f docker/stream-compose.yml kill spark`, wait 20 s,
`docker compose -f docker/stream-compose.yml up -d spark` — the stream resumes from the checkpoint, no duplicates.

## After

```bash
bash scripts/stream_demo.sh down
docker stop bda501-client bda501-historyserver bda501-nodemanager bda501-resourcemanager bda501-datanode bda501-namenode
```

## Who presents what

| Part | Presenter |
|---|---|
| Problem, data, architecture, MapReduce, Spark SQL | Trần Quốc Việt |
| ML: features, model comparison, evaluation, recall by province, performance | Nguyễn Phạm Thành Luân |
| Streaming, MongoDB, web demo, deployment | Phạm Lê Tấn Thịnh |
