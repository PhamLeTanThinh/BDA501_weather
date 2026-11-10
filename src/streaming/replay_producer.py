"""Phát lại ERA5 lịch sử (Yagi, 09/2024) thành file mới trong thư mục nguồn của Structured Streaming.

Mỗi file = 1 giờ lịch sử × mọi ô lưới mà các điểm demo cần (ô trung tâm + ô lân cận). File được ghi
vào staging rồi đổi tên sang stream_in (os.replace là nguyên tử trên cùng ổ đĩa / HDFS rename), nên Spark
không bao giờ đọc file chưa ghi xong. Không sửa hay ghi đè file đã phát.

    python -m src.streaming.replay_producer --profile pilot [--no-sleep]
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.common.config import ROOT, load_config
from src.common.spark import get_spark
from src.pipeline.bronze_silver import VARS


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="pilot")
    ap.add_argument("--no-sleep", action="store_true")
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    sc = cfg["streaming"]
    spark = get_spark("replay_producer", driver_memory="1g")
    lake = ROOT / cfg["paths"]["lake"]
    grids = (spark.read.parquet(str(lake / "silver" / "location_grid"))
             .where(F.col("location_id").isin(sc["locations"])).select("grid_id").distinct())
    wx = (spark.read.parquet(str(lake / "silver" / "grid_weather")).join(grids, "grid_id")
          .where((F.col("ts") >= F.to_timestamp(F.lit(sc["replay_start"])))
                 & (F.col("ts") <= F.to_timestamp(F.lit(sc["replay_end"]))))
          .select("grid_id", F.date_format("ts", "yyyy-MM-dd HH:mm").alias("event_time"), *VARS)
          .orderBy("event_time", "grid_id").toPandas())
    spark.stop()

    staging, stream_in = ROOT / sc["staging"], ROOT / sc["stream_in"]
    staging.mkdir(parents=True, exist_ok=True)
    stream_in.mkdir(parents=True, exist_ok=True)
    hours = sorted(wx.event_time.unique())
    print(f"Phát {len(hours)} giờ × {wx.grid_id.nunique()} ô lưới vào {stream_in}", flush=True)
    for h in hours:
        name = "weather_" + h.replace("-", "").replace(" ", "T").replace(":", "") + ".json"
        if (stream_in / name).exists():
            continue  # đã phát ở lần chạy trước
        emitted = datetime.now(timezone.utc).isoformat()
        tmp = staging / name
        with open(tmp, "w", encoding="utf-8") as f:
            for r in wx[wx.event_time == h].to_dict("records"):
                r = {k: (None if isinstance(v, float) and v != v else v) for k, v in r.items()}
                f.write(json.dumps({**r, "emitted_at_utc": emitted}) + "\n")
        os.replace(tmp, stream_in / name)
        print(f"  {h} → {name}", flush=True)
        if not args.no_sleep:
            time.sleep(sc["seconds_per_replay_hour"])


if __name__ == "__main__":
    main()
