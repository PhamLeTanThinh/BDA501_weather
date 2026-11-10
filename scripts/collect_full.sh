#!/usr/bin/env bash
# Tải ERA5 qua Open-Meteo cho bản chính thức (63 ô trung tâm, 2015–2025), tự chờ khi hết lượt trong ngày.
#   exit 0 = xong; exit 2 = hết lượt ngày → nghỉ 1 giờ rồi chạy tiếp; lỗi khác → nghỉ 5 phút rồi thử lại.
cd "$(dirname "$0")/.."
LOG=data/raw/era5_full_fetch.out
while true; do
  .venv/Scripts/python.exe -m src.ingest.collect_era5 --profile full >> "$LOG" 2>&1
  rc=$?
  echo "[$(date '+%F %T')] exit $rc" >> "$LOG"
  [ $rc -eq 0 ] && break
  if [ $rc -eq 2 ]; then sleep 3600; else sleep 300; fi
done
echo "[$(date '+%F %T')] done" >> "$LOG"
