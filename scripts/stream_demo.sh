#!/usr/bin/env bash
# Demo luồng streaming Kafka → Spark → MongoDB → web (http://localhost:8000, Spark UI http://localhost:4040).
#   scripts/stream_demo.sh build    build image bda501-rain-stream:1
#   scripts/stream_demo.sh up       chạy Kafka, MongoDB, Spark, web; producer bắt đầu phát lại bão Yagi
#   scripts/stream_demo.sh reset    xóa topic, checkpoint, bộ đệm, MongoDB để chạy lại từ đầu
#   scripts/stream_demo.sh down     dừng mọi container
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash: không đổi đường dẫn /opt/... trong lệnh docker exec
ROOT="$(cd "$(dirname "$0")/.." && (pwd -W 2>/dev/null || pwd))"
DC=(docker compose -f "$ROOT/docker/stream-compose.yml")
DATA="${BDA_DATA_HOST:-D:/BDA501_data}"
case "${1:-up}" in
  build) docker build -t bda501-rain-stream:1 -f "$ROOT/docker/Dockerfile.stream" "$ROOT/docker" ;;
  up)    "${DC[@]}" up -d kafka mongo
         "${DC[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
           --create --if-not-exists --topic weather.silver.hourly --partitions 3 --replication-factor 1
         "${DC[@]}" up -d spark web
         sleep 20
         "${DC[@]}" up -d producer
         echo "web: http://localhost:8000 (đợi làm nóng xong rồi bấm ▶ Bắt đầu stream) · Spark UI: http://localhost:4040" ;;
  reset) "${DC[@]}" up -d kafka mongo
         until "${DC[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 3; done
         "${DC[@]}" stop producer spark || true
         "${DC[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
           --delete --topic weather.silver.hourly || true
         "${DC[@]}" exec -T mongo mongosh --quiet rain_stream --eval "db.dropDatabase()" || true
         rm -rf "$DATA/lake/stream_kafka" ;;
  down)  "${DC[@]}" down ;;
  *)     echo "dùng: $0 build|up|reset|down"; exit 1 ;;
esac
