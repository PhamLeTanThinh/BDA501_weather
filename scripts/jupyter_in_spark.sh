#!/usr/bin/env bash
# Mở Jupyter Lab trong container Spark (Linux, cùng môi trường với pipeline) để chạy notebook từng khối.
# Spark chạy thẳng trên Windows cần winutils.exe/hadoop.dll nên notebook dùng kernel trong container.
#   scripts/jupyter_in_spark.sh      → mở http://localhost:8888/lab/tree/notebooks/eda_official.ipynb
#   (VS Code: chọn kernel "Existing Jupyter Server" và dán URL có token in ra bên dưới)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && (pwd -W 2>/dev/null || pwd))"
DATA="${BDA_DATA_HOST:-D:/BDA501_data}"
NET="${NET:-bda501-rain}"
docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
MSYS_NO_PATHCONV=1 docker run --rm -it --name bda501-jupyter --network "$NET" -p 8888:8888 -p 4040:4040 \
  -e TZ=Asia/Ho_Chi_Minh -e BDA_DATA=/data -e SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-5g}" \
  -v "$ROOT:/app" -v "$DATA:/data" -w /app --entrypoint python3 bda501-rain-spark:1 \
  -m jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --ServerApp.root_dir=/app
