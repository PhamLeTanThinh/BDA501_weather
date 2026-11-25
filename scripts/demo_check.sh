#!/usr/bin/env bash
# Pre-demo check for the final assessment: run ~15 minutes before presenting.
#   bash scripts/demo_check.sh          check everything, reset the stream and warm it up until the Start button is ready
#   bash scripts/demo_check.sh --quick  only check (no stream reset)
# Prints one line per check: [OK] / [FAIL] + what to do.
set -uo pipefail
export MSYS_NO_PATHCONV=1
ROOT="$(cd "$(dirname "$0")/.." && (pwd -W 2>/dev/null || pwd))"
DATA="${BDA_DATA_HOST:-D:/BDA501_data}"
FAIL=0
ok()   { echo "[OK]   $*"; }
bad()  { echo "[FAIL] $*"; FAIL=1; }

# 1. Docker
if docker info >/dev/null 2>&1; then ok "Docker is running"; else
  bad "Docker is not running → start Docker Desktop, wait for it, rerun"; exit 1; fi

# 2. Images and data
for img in bda501-rain-spark:1 bda501-rain-stream:1; do
  docker image inspect "$img" >/dev/null 2>&1 && ok "image $img" || bad "image $img missing → see README Setup"; done
for d in lake/gold/features_hourly lake/gold/m4_scores lake/silver/grid_weather lake/models/official; do
  [ -d "$DATA/$d" ] && ok "data $d" || bad "data $DATA/$d missing"; done

# 3. Hadoop cluster (MapReduce demo)
docker start bda501-namenode bda501-datanode bda501-resourcemanager bda501-nodemanager bda501-historyserver bda501-client >/dev/null 2>&1
for i in $(seq 1 24); do docker exec bda501-client hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF && break; sleep 5; done
docker exec bda501-client hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF && ok "HDFS up (safe mode off)" || bad "HDFS not ready → docker logs bda501-namenode"
for i in $(seq 1 24); do docker exec bda501-client yarn node -list 2>/dev/null | grep -q RUNNING && break; sleep 5; done
docker exec bda501-client yarn node -list 2>/dev/null | grep -q RUNNING && ok "YARN NodeManager running (UI http://localhost:8088)" || bad "YARN not ready → docker restart bda501-resourcemanager bda501-nodemanager"
docker exec bda501-client hdfs dfs -test -e /rain-forecast/mr/output/_SUCCESS 2>/dev/null && ok "MapReduce output on HDFS (/rain-forecast/mr/output)" \
  || bad "no MapReduce output on HDFS → SPLIT_BYTES=134217728 LOG_DIR=reports/official/mapreduce bash scripts/run_mapreduce.sh"

# free ~0.7 GB for the stream: the finished job stays visible in the job history (http://localhost:19888) and HDFS (9870)
docker stop bda501-nodemanager bda501-resourcemanager >/dev/null 2>&1 && ok "YARN RM/NM stopped to free memory (job history + HDFS stay up)"

# 4. Evidence files shown in the demo
for f in reports/official/mapreduce/validation.json reports/official/spark_sql/q2_heavy_days_province_year_plan.txt \
         reports/official/model/m4_threshold_choice.csv reports/streaming/stream_report.md reports/tests/pytest_full.txt; do
  [ -f "$ROOT/$f" ] && ok "evidence $f" || bad "evidence $f missing"; done

# 5. Streaming: reset, start, wait until warm-up is done (Start button ready)
if [ "${1:-}" != "--quick" ]; then
  bash "$ROOT/scripts/stream_demo.sh" reset >/dev/null 2>&1
  bash "$ROOT/scripts/stream_demo.sh" up >/dev/null 2>&1
  st=""
  for i in $(seq 1 90); do
    st=$(curl -s -m 5 http://localhost:8000/api/state | grep -o '"state":"[a-z]*"' | head -1 | cut -d'"' -f4)
    [ "$st" = "ready" ] && break; sleep 5; done
  [ "$st" = "ready" ] && ok "stream warmed up: open http://localhost:8000 and press ▶ Start (Spark UI http://localhost:4040)" \
    || bad "stream not ready (state='$st') → docker compose -f docker/stream-compose.yml logs spark producer"
fi

echo; [ $FAIL = 0 ] && echo "ALL CHECKS PASSED" || echo "SOME CHECKS FAILED — see [FAIL] lines; fallback: report/demo/"
exit $FAIL
