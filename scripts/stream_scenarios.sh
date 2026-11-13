#!/usr/bin/env bash
# Chạy đủ kịch bản streaming rồi đo: trùng, trễ trong/quá watermark, sai thứ tự, bản ghi hỏng, thiếu giờ ở vài ô,
# dữ liệu dồn cục (producer tự chèn) + TẮT JOB SPARK GIỮA CHỪNG RỒI CHẠY LẠI (script này làm), cuối cùng chạy
# src.streaming.stream_report → reports/streaming/stream_report.{json,md}.
#   scripts/stream_scenarios.sh            (cần: scripts/stream_demo.sh build đã chạy)
set -euo pipefail
export MSYS_NO_PATHCONV=1
export WAIT_START=0          # kịch bản đo lường: không chờ nút Bắt đầu trên web
ROOT="$(cd "$(dirname "$0")/.." && (pwd -W 2>/dev/null || pwd))"
DC=(docker compose -f "$ROOT/docker/stream-compose.yml")
KILL_AT="${KILL_AT:-110}"   # tắt Spark khi producer đã phát tới giờ thứ bao nhiêu
DOWN_S="${DOWN_S:-45}"      # Spark tắt bao lâu (producer vẫn phát → dồn backlog trong Kafka)
q() { "${DC[@]}" exec -T mongo mongosh --quiet rain_stream --eval "$1"; }

# bật Kafka + MongoDB trước để reset xóa được topic và database cũ (nếu chúng đang tắt, reset sẽ bỏ sót dữ liệu cũ)
"${DC[@]}" up -d kafka mongo >/dev/null 2>&1
until "${DC[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 3; done
bash "$ROOT/scripts/stream_demo.sh" reset >/dev/null 2>&1 || true
bash "$ROOT/scripts/stream_demo.sh" up
echo "chờ producer tới giờ thứ $KILL_AT ..."
until [ "$(q 'print((db.stream_stats.findOne({_id:"producer"})||{}).hours_done||0)')" -ge "$KILL_AT" ]; do sleep 5; done
echo "$(date +%T) TẮT Spark (SIGKILL) giữa chừng"
"${DC[@]}" kill spark
q 'db.stream_stats.updateOne({_id:"scenario"},{$set:{killed_at_hour:'"$KILL_AT"', killed_at:new Date()}},{upsert:true})' >/dev/null
sleep "$DOWN_S"
echo "$(date +%T) BẬT lại Spark (đọc tiếp từ checkpoint)"
"${DC[@]}" start spark
until [ "$(q 'print((db.stream_stats.findOne({_id:"producer"})||{}).finished===true)')" = "true" ]; do sleep 10; done
echo "producer xong; chờ Spark đọc hết Kafka ..."
# xong khi 3 lần kiểm tra liên tiếp (45 s) không có batch mới hoặc batch mới không còn dòng nào
idle=0; last=""
while [ $idle -lt 3 ]; do
  sleep 15
  cur=$(q 'var d=db.progress.find({query:"forecast"}).sort({batch:-1}).limit(1).toArray(); print(d.length?d[0].batch+"/"+d[0].kafka_rows:"none")')
  if [ "$cur" = "$last" ] || [ "${cur#*/}" = "0" ]; then idle=$((idle+1)); else idle=0; fi
  last=$cur
done
"${DC[@]}" stop spark producer web >/dev/null 2>&1   # báo cáo cần RAM; Kafka + MongoDB vẫn chạy
"${DC[@]}" run --rm -T --no-deps spark python3 -m src.streaming.stream_report
"${DC[@]}" run --rm -T --no-deps web python3 -m src.streaming.stream_export
