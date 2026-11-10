#!/usr/bin/env bash
# Chạy lại toàn bộ pipeline từ raw đã tải → báo cáo và bộ slide. Cần: Docker, cụm Hadoop lab, container mongo.
#   scripts/run_all.sh pilot
set -euo pipefail
P="${1:-pilot}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RUN=scripts/run_in_spark.sh
step() { echo; echo "=== $* ($(date +%H:%M:%S))"; }

step "bronze → silver";       $RUN src.pipeline.bronze_silver --profile "$P"
step "silver → gold";         $RUN src.pipeline.gold_features --profile "$P"
step "tests";                 $RUN pytest -q tests
step "ablation + LOLO";       $RUN src.experiments.ablation --profile "$P"

step "streaming: reset state, replay Yagi, forecast"
rm -rf data/lake/stream_in data/lake/stream_staging data/lake/checkpoints data/lake/stream_state data/lake/stream_out
docker exec mongo mongosh -quiet rain_forecast --eval 'db.predictions.drop(); db.province_risk.drop()' >/dev/null
$RUN src.streaming.replay_producer --profile "$P" --no-sleep
$RUN src.streaming.stream_forecast --profile "$P" --once
$RUN src.streaming.stream_forecast --profile "$P" --once      # chạy lại: phải không xử lý thêm file nào
$RUN src.streaming.stream_forecast --profile "$P" --evaluate
$RUN src.serving.query_mongo --profile "$P"

step "Spark SQL queries + plans"; $RUN src.analytics.spark_sql_queries --profile "$P"
step "MapReduce";             $RUN src.mapreduce.mr_io export --profile "$P"
bash scripts/run_mapreduce.sh
$RUN src.mapreduce.mr_io validate --profile "$P"
step "performance experiment"; $RUN src.experiments.perf_experiment --profile "$P"
step "deck";                  $RUN src.experiments.deck_data --profile "$P"
echo "build slides: (cd deck && NODE_PATH=\$PWD/node_modules PPTX_SKILL=<pptx skill dir> node build_bilingual.js)"
