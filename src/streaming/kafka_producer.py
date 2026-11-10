"""Producer Kafka: phát lại silver (ERA5 CDS, 493 ô) theo từng giờ vào topic, kèm dữ liệu rác có kiểm soát.

Mỗi "giờ phát lại" gửi 1 bản tin / ô (key = grid_id, value = JSON đúng cột silver + event_time giờ VN).
Rác được chèn với tỷ lệ cấu hình được, để luồng Spark phải tự phát hiện (không có cờ nào trong bản tin
nói đâu là rác):

  duplicate        gửi lại y hệt bản tin ngay sau đó                → Spark khử trùng (watermark)
  duplicate_later  gửi lại y hệt bản tin sau vài giờ phát lại        → vẫn bị khử trùng (state còn giữ)
  late             bản tin thật đến trễ 1–4 giờ (vẫn trong watermark) → được nhận, dự báo được tính lại
  too_late         bản tin thật đến trễ 12 giờ (quá watermark)      → Spark bỏ, giờ đó coi như thiếu
  malformed        chuỗi JSON hỏng                                  → cách ly: malformed_json
  missing_field    thiếu event_time hoặc grid_id                    → cách ly: missing_field
  out_of_range     giá trị vô lý (mưa âm, nhiệt 85 °C, …)            → cách ly: out_of_range
  unknown_cell     grid_id không thuộc 493 ô                        → cách ly: unknown_cell

Ngoài ra là các tình huống theo giờ phát lại:

  reordered        cứ 25 giờ một lần, gửi giờ h+1 trước giờ h               → bản tin giờ h đến sai thứ tự
  missing_hours    5 ô bị mất 6 giờ liên tiếp (không bao giờ gửi)           → feature của ô đó null tới khi đủ lại
  burst            20 giờ phát liền một lúc, không nghỉ                     → Spark đọc theo maxOffsetsPerTrigger

Số đã chèn theo từng loại được ghi vào MongoDB (stream_stats/_id=producer) để web so với số Spark bắt được.

    python -m src.streaming.kafka_producer --bootstrap kafka:9092 [--seconds-per-hour 5] [--no-sleep]
"""
import argparse
import json
import os
import random
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import pyarrow.dataset as ds
from kafka import KafkaProducer
from pymongo import MongoClient

from src.pipeline.cds_silver import VARS

VN = timezone(timedelta(hours=7))
RATES = {"duplicate": 0.03, "duplicate_later": 0.02, "late": 0.03, "too_late": 0.001,
         "malformed": 0.005, "missing_field": 0.003, "out_of_range": 0.005, "unknown_cell": 0.003}
BAD_VALUES = [("precipitation", -12.0), ("temperature_2m", 85.0), ("relative_humidity_2m", 140.0),
              ("pressure_msl", 420.0), ("cape", -500.0)]


def load_silver(lake, start, end):
    t0 = int(start.timestamp())
    t1 = int(end.timestamp())
    tbl = ds.dataset(f"{lake}/silver/grid_weather", format="parquet", partitioning="hive").to_table(
        columns=["grid_id", "ts_epoch", *VARS],
        filter=(ds.field("year") == start.year) & (ds.field("ts_epoch") >= t0) & (ds.field("ts_epoch") <= t1))
    df = tbl.to_pandas()
    return {h: g for h, g in df.groupby("ts_epoch")}


def message(row, epoch):
    r = {"grid_id": row["grid_id"], "event_time": datetime.fromtimestamp(epoch, VN).isoformat()}
    for v in VARS:
        x = row[v]
        r[v] = None if x != x else float(x)  # NaN → null; giữ đủ chữ số để kiểm tra parity với batch
    r["msg_id"] = uuid.uuid4().hex
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--topic", default="weather.silver.hourly")
    ap.add_argument("--lake", default="/data/lake")
    ap.add_argument("--start", default="2024-09-01 00:00")
    ap.add_argument("--end", default="2024-09-10 23:00")
    ap.add_argument("--seconds-per-hour", type=float, default=5.0, help="mỗi giờ dữ liệu phát trong bao nhiêu giây")
    ap.add_argument("--no-sleep", action="store_true")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--warmup-until", default="2024-09-04 00:00",
                    help="phát dồn tới giờ này (làm nóng 72 giờ), rồi chờ nút Bắt đầu trên web")
    ap.add_argument("--wait-start", type=int, default=int(os.environ.get("PRODUCER_WAIT_START", "1")),
                    help="1: dừng sau làm nóng, chờ lệnh start trong MongoDB (stream_stats/_id=control)")
    ap.add_argument("--mongo", default=os.environ.get("MONGO_URI", "mongodb://mongo:27017"))
    args = ap.parse_args()
    rnd = random.Random(args.seed)
    start = datetime.strptime(args.start, "%Y-%m-%d %H:%M").replace(tzinfo=VN)
    end = datetime.strptime(args.end, "%Y-%m-%d %H:%M").replace(tzinfo=VN)
    hours = load_silver(args.lake, start, end)
    print(f"silver: {len(hours)} giờ × {len(next(iter(hours.values())))} ô", flush=True)

    prod = KafkaProducer(bootstrap_servers=args.bootstrap, key_serializer=lambda k: k.encode(),
                         value_serializer=lambda v: v.encode(), linger_ms=50, acks="all")
    stats = MongoClient(args.mongo)["rain_stream"]["stream_stats"]
    stats.update_one({"_id": "control"}, {"$set": {"command": "pause" if args.wait_start else "run"}}, upsert=True)
    warm = int(datetime.strptime(args.warmup_until, "%Y-%m-%d %H:%M").replace(tzinfo=VN).timestamp())

    def wait_for_run(state):
        """Chờ lệnh run từ web (nút Bắt đầu / Tiếp tục); ghi trạng thái để web hiển thị."""
        if stats.find_one({"_id": "control"}, {"command": 1}).get("command") == "run":
            return
        stats.update_one({"_id": "producer"}, {"$set": {"state": state}}, upsert=True)
        print(f"chờ lệnh bắt đầu ({state})", flush=True)
        while stats.find_one({"_id": "control"}, {"command": 1}).get("command") != "run":
            time.sleep(1)
        stats.update_one({"_id": "producer"}, {"$set": {"state": "running"}})
    counts = Counter()
    pending = defaultdict(list)  # giờ phát lại → bản tin bị giữ lại (trễ / trùng muộn)

    def send(key, text):
        prod.send(args.topic, key=key or "junk", value=text)
        counts["sent"] += 1

    epochs = sorted(hours)
    # sai thứ tự: đổi chỗ giờ h và h+1 (sau 72 giờ làm nóng)
    order = list(range(len(epochs)))
    for i in range(80, len(epochs) - 1, 25):
        order[i], order[i + 1] = order[i + 1], order[i]
        counts["reordered_hours"] += 1
    # thiếu giờ: 5 ô, mỗi ô mất 6 giờ liên tiếp
    cells = sorted(next(iter(hours.values())).grid_id)
    gaps = {(c, epochs[k]) for c, start in zip(rnd.sample(cells, 5), rnd.sample(range(90, 200), 5))
            for k in range(start, start + 6)}
    burst = set(range(150, 170))
    for i, idx in enumerate(order):
        ep = epochs[idx]
        warming = ep < warm
        if not warming:
            wait_for_run("ready" if i == sum(e < warm for e in epochs) else "paused")
        reordered = idx != i
        for _, row in hours[ep].iterrows():
            if (row["grid_id"], ep) in gaps:
                counts["missing_hours"] += 1
                continue
            m = message(row, ep)
            if reordered and idx < i:
                counts["reordered"] += 1
            text = json.dumps(m)
            u = rnd.random()
            if u < RATES["late"]:
                pending[i + rnd.randint(1, 4)].append((m["grid_id"], text))
                counts["late"] += 1
                continue
            if u < RATES["late"] + RATES["too_late"]:
                pending[i + 12].append((m["grid_id"], text))
                counts["too_late"] += 1
                continue
            send(m["grid_id"], text)
            if rnd.random() < RATES["duplicate"]:
                send(m["grid_id"], text)
                counts["duplicate"] += 1
            if rnd.random() < RATES["duplicate_later"]:
                pending[i + rnd.randint(1, 3)].append((m["grid_id"], text))
                counts["duplicate_later"] += 1
            if rnd.random() < RATES["out_of_range"]:
                col, val = rnd.choice(BAD_VALUES)
                send(m["grid_id"], json.dumps({**m, col: val, "msg_id": uuid.uuid4().hex}))
                counts["out_of_range"] += 1
            if rnd.random() < RATES["unknown_cell"]:
                send("g99.00_999.00", json.dumps({**m, "grid_id": "g99.00_999.00", "msg_id": uuid.uuid4().hex}))
                counts["unknown_cell"] += 1
            if rnd.random() < RATES["missing_field"]:
                drop = rnd.choice(["event_time", "grid_id"])
                bad = {k: v for k, v in m.items() if k != drop}
                send(m["grid_id"], json.dumps(bad))
                counts["missing_field"] += 1
            if rnd.random() < RATES["malformed"]:
                send(m["grid_id"], text[: rnd.randint(10, len(text) - 5)])  # JSON bị cắt cụt
                counts["malformed"] += 1
        for key, text in pending.pop(i, []):
            send(key, text)
        prod.flush()
        clock = datetime.fromtimestamp(ep, VN)
        stats.update_one({"_id": "producer"}, {"$set": {
            "replay_hour": clock.isoformat(), "hours_done": i + 1, "hours_total": len(epochs),
            "injected": {k: counts[k] for k in [*RATES, "reordered", "missing_hours"]}, "sent": counts["sent"],
            "reordered_hours": counts["reordered_hours"], "burst": i in burst,
            "state": "warmup" if warming else "running", "warmup_hours": sum(e < warm for e in epochs),
            "seconds_per_hour": 0 if args.no_sleep else args.seconds_per_hour,
            "updated_at": datetime.now(timezone.utc)}}, upsert=True)
        print(f"{clock:%d/%m %H:%M}  gửi {counts['sent']:,}  rác {sum(counts[k] for k in RATES):,}", flush=True)
        if not args.no_sleep and i not in burst and not warming:
            time.sleep(args.seconds_per_hour)
    for i in sorted(pending):  # bản tin còn giữ lại sau giờ cuối
        for key, text in pending[i]:
            send(key, text)
    prod.flush()
    stats.update_one({"_id": "producer"}, {"$set": {"finished": True, "sent": counts["sent"], "state": "finished"}})
    print("xong", dict(counts), flush=True)


if __name__ == "__main__":
    main()
