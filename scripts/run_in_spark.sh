#!/usr/bin/env bash
# Chạy một module Python của repo trong container Spark (Spark 4.2, Java 17, Python 3.11).
#   scripts/run_in_spark.sh src.pipeline.bronze_silver --profile pilot
#   scripts/run_in_spark.sh pytest -q tests
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && (pwd -W 2>/dev/null || pwd))"
IMAGE="${IMAGE:-bda501-rain-spark:1}"
NET="${NET:-bda501-rain}"
docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
if [ "$1" = "pytest" ]; then shift; CMD=(python3 -m pytest -p no:cacheprovider "$@"); else CMD=(python3 -m "$@"); fi
# dữ liệu chính thức (raw CDS, lake) nằm ngoài repo: mount vào /data, config dùng ${BDA_DATA}
DATA="${BDA_DATA_HOST:-D:/BDA501_data}"
MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" -e TZ=Asia/Ho_Chi_Minh \
  -e MONGO_URI="${MONGO_URI:-mongodb://mongo:27017}" -e BDA_DATA=/data -e SPARK_DRIVER_MEMORY \
  -v "$ROOT:/app" -v "$DATA:/data" -w /app --entrypoint "${CMD[0]}" "$IMAGE" "${CMD[@]:1}"
