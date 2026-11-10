"""Spark Structured Streaming: Kafka (silver phát lại) → kiểm tra → watermark + khử trùng → feature → M4 → MongoDB.

Hai truy vấn cùng đọc topic weather.silver.hourly:

  rejects   bản tin hỏng → cách ly (MongoDB stream_rejects) và đếm theo lý do:
            malformed_json · missing_field · unknown_cell · out_of_range
  forecast  bản tin hợp lệ → withWatermark(event_time, 6 giờ) → dropDuplicatesWithinWatermark(ô, giờ)
            · trùng (kể cả trùng đến muộn vài giờ) bị bỏ, đếm qua state operator
            · trễ trong 6 giờ: được nhận, các giờ bị ảnh hưởng được tính lại feature và dự báo (revision + 1)
            · trễ quá 6 giờ: bị watermark bỏ (numRowsDroppedByWatermark), giờ đó coi như thiếu → feature null
            rồi foreachBatch: nối vào bộ đệm lịch sử 120 giờ trên lake (khử trùng lần nữa theo ô, giờ),
            tính feature M4 bằng đúng local_features + atmos_features của batch, dự báo bằng M4 chính thức
            và ngưỡng chọn trên 2023, upsert MongoDB (predictions, province_risk). Khi đã qua t + 24 giờ thì
            ghi thêm mưa thật 24 giờ và nhãn để web đếm đúng / sai.

    python -m src.streaming.kafka_stream --bootstrap kafka:9092
"""
import argparse
import json
import os
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pymongo import ASCENDING, MongoClient, UpdateOne
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.common.config import ROOT
from src.common.spark import get_spark
from src.features.atmos import atmos_features
from src.features.local_weather import add_label, local_features
from src.pipeline.cds_silver import VALID, VARS

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
SCHEMA = StructType([StructField("grid_id", StringType()), StructField("event_time", StringType())]
                    + [StructField(v, DoubleType()) for v in VARS] + [StructField("msg_id", StringType())])
KEEP_HOURS = 120          # bộ đệm: 72 giờ cửa sổ + 24 giờ lag + chỗ cho dữ liệu trễ
WATERMARK = "6 hours"


def parse(kdf, cells):
    """Bản tin Kafka → cột silver + lý do loại (null nếu hợp lệ)."""
    raw = kdf.select(F.col("value").cast("string").alias("raw"), F.col("timestamp").alias("kafka_ts"),
                     "partition", "offset")
    j = raw.withColumn("j", F.from_json("raw", SCHEMA)).select("raw", "kafka_ts", "partition", "offset", "j.*")
    j = (j.withColumn("event_ts", F.to_timestamp("event_time"))
          .join(F.broadcast(cells.select("grid_id", F.lit(True).alias("known"))), "grid_id", "left"))
    bad_range = F.lit(False)
    for c, (lo, hi) in VALID.items():
        bad_range = bad_range | (F.col(c).isNotNull() & ((F.col(c) < lo) | (F.col(c) > hi)))
    reason = (F.when(F.expr("try_parse_json(raw)").isNull(), "malformed_json")
               .when(F.col("grid_id").isNull() | F.col("event_ts").isNull(), "missing_field")
               .when(F.col("known").isNull(), "unknown_cell")
               .when(bad_range, "out_of_range"))
    return j.withColumn("reason", reason)


class Forecaster:
    def __init__(self, spark, db, lake, predict_from):
        self.spark, self.db, self.lake = spark, db, Path(lake)
        man = json.loads((ROOT / "reports" / "official" / "model" / "ablation_manifest.json").read_text(encoding="utf-8"))
        self.features = man["feature_sets"]["M4"]
        self.threshold = float(man["thresholds_from_validation"]["M4"])
        self.model_version = f"rf_{man['run_id']}_M4"
        self.model = RandomForestClassificationModel.load(str(self.lake / "models" / "official" / man["run_id"] / "M4"))
        self.asm = VectorAssembler(inputCols=self.features, outputCol="features")
        cells = spark.read.parquet(str(self.lake / "silver" / "cells"))
        self.cells = cells.select(F.col("grid_id").alias("location_id"), F.col("grid_lat").alias("lat"),
                                  F.col("grid_lon").alias("lon"), "new_province", "region").cache()
        self.root = self.lake / "stream_kafka" / "history"
        self.root.mkdir(parents=True, exist_ok=True)
        done = sorted(self.root.glob("b*/_SUCCESS"), key=lambda p: p.parent.name)
        self.cur = str(done[-1].parent) if done else None
        self.accepted = self.lake / "stream_kafka" / "accepted"
        self.max_seen = None
        if self.cur:
            self.max_seen = spark.read.parquet(self.cur).agg(F.max("ts")).first()[0]
        self.predict_from = predict_from
        print(f"M4 · {len(self.features)} feature · ngưỡng {self.threshold:.3f} · bộ đệm {self.cur}", flush=True)

    def process(self, df, bid):
        t0 = time.time()
        # mốc "giờ lớn nhất đã thấy" lấy từ batch đã commit trước đó (bid − 1), không lấy từ bộ đệm: khi Spark chạy
        # lại một batch sau sự cố, bộ đệm có thể đã chứa chính batch đó → đếm trễ sai
        prev = self.db.batch_stats.find_one({"_id": bid - 1}, {"max_ts": 1})
        if prev and prev.get("max_ts"):
            self.max_seen = datetime.strptime(prev["max_ts"], "%Y-%m-%d %H:%M")
        # trễ = giờ dữ liệu nhỏ hơn giờ lớn nhất đã đến trước nó (theo thứ tự đến trong Kafka)
        order = Window.orderBy("kafka_ts", "partition", "offset").rowsBetween(Window.unboundedPreceding, -1)
        seen = F.max("event_ts").over(order)
        if self.max_seen:
            seen = F.greatest(F.coalesce(seen, F.lit(self.max_seen)), F.lit(self.max_seen))
        new = (df.withColumn("is_late", F.coalesce(F.col("event_ts") < seen, F.lit(False)))
                 .select("grid_id", F.col("event_ts").alias("ts"), "is_late", *VARS).cache())
        n = new.count()
        if n == 0:
            new.unpersist()
            return
        mn, mx, late = new.agg(F.min("ts"), F.max("ts"), F.sum(F.col("is_late").cast("int"))).first()
        late = int(late or 0)
        touched = new.groupBy(F.col("grid_id").alias("location_id")).agg(F.min("ts").alias("since_cell"))
        new = new.drop("is_late")
        hist = self.spark.read.parquet(self.cur) if self.cur else None
        allr = (hist.unionByName(new) if hist is not None else new).dropDuplicates(["grid_id", "ts"])
        gmax = max(mx, self.max_seen) if self.max_seen else mx
        nxt = str(self.root / f"b{bid:06d}_{int(time.time() * 1000)}")
        # lưu mọi dòng hợp lệ đã nhận theo batch (ghi đè theo batch id → chạy lại không nhân đôi) để kiểm tra parity
        new.write.mode("overwrite").parquet(str(self.accepted / f"b{bid:06d}"))
        allr.where(F.col("ts") > F.lit(gmax - timedelta(hours=KEEP_HOURS))).write.mode("overwrite").parquet(nxt)
        old, self.cur, self.max_seen = self.cur, nxt, gmax
        new.unpersist()
        if old and old != nxt:
            shutil.rmtree(old, ignore_errors=True)

        buf = (self.spark.read.parquet(nxt).withColumnRenamed("grid_id", "location_id")
               .withColumn("ts_epoch", F.unix_timestamp("ts")).join(F.broadcast(self.cells), "location_id"))
        f = add_label(atmos_features(local_features(buf, key="location_id"), key="location_id"),
                      key="location_id", horizon_hours=24, threshold_mm=50.0).cache()
        # chỉ tính lại ô nhận dữ liệu mới, từ giờ sớm nhất vừa đến của ô đó (dữ liệu trễ → tính lại các giờ sau nó)
        target = (f.join(F.broadcast(touched), "location_id")
                   .where((F.col("ts") >= F.col("since_cell")) & (F.col("ts") >= F.lit(self.predict_from))))
        ok = F.lit(True)
        for c in self.features:
            ok = ok & F.col(c).isNotNull()
        scored = (self.model.transform(self.asm.transform(target.where(ok)))
                  .select("location_id", "ts", "lat", "lon", "new_province", "region",
                          vector_to_array("probability")[1].alias("p"))).collect()
        waiting = target.where(~ok).select("location_id", "ts", "lat", "lon", "new_province", "region").collect()
        # nhãn thật: chỉ có khi đã đủ 24 giờ sau t trong bộ đệm
        truth = (f.where((F.col("ts") >= F.lit(self.predict_from)) & (F.col("ts") >= F.lit(mn - timedelta(hours=25)))
                         & F.col("label").isNotNull())
                  .select("location_id", "ts", "rain_next_24h", "label").collect())
        f.unpersist()

        now = datetime.now(timezone.utc)
        key = lambda r: f"{r.location_id}|{r.ts:%Y-%m-%d %H:%M}"
        ops = [UpdateOne({"_id": key(r)}, {"$set": {
            "cell": r.location_id, "ts": f"{r.ts:%Y-%m-%d %H:%M}", "lat": r.lat, "lon": r.lon,
            "province": r.new_province, "region": r.region, "p": round(float(r.p), 4), "threshold": self.threshold,
            "alert": bool(r.p >= self.threshold), "status": "ok", "model": self.model_version,
            "batch_id": bid, "updated_at": now}, "$inc": {"revisions": 1}}, upsert=True) for r in scored]
        ops += [UpdateOne({"_id": key(r)}, {"$set": {
            "cell": r.location_id, "ts": f"{r.ts:%Y-%m-%d %H:%M}", "lat": r.lat, "lon": r.lon,
            "province": r.new_province, "region": r.region, "p": None, "alert": False,
            "status": "missing_history", "was_missing": True, "model": self.model_version, "batch_id": bid,
            "updated_at": now}},
            upsert=True) for r in waiting]
        if ops:
            self.db.predictions.bulk_write(ops, ordered=False)
        if truth:
            self.db.predictions.bulk_write([UpdateOne({"_id": key(r)}, {"$set": {
                "observed_24h_mm": round(float(r.rain_next_24h), 1), "label": int(r.label)}}) for r in truth], ordered=False)
        # rủi ro theo tỉnh mới: xác suất lớn nhất trong các ô của tỉnh, số ô cảnh báo
        prov = {}
        for r in scored:
            k = (r.new_province, f"{r.ts:%Y-%m-%d %H:%M}")
            p = prov.setdefault(k, {"p_max": 0.0, "alert_cells": 0, "cells": 0})
            p["p_max"] = max(p["p_max"], float(r.p))
            p["alert_cells"] += int(r.p >= self.threshold)
            p["cells"] += 1
        if prov:
            self.db.province_risk.bulk_write([UpdateOne({"_id": f"{k[0]}|{k[1]}"}, {"$set": {
                "province": k[0], "ts": k[1], **v, "updated_at": now}}, upsert=True) for k, v in prov.items()],
                ordered=False)
        self.db.stream_stats.update_one({"_id": "pipeline"}, {
            "$set": {"clock": f"{gmax:%Y-%m-%d %H:%M}", "last_batch": bid, "last_batch_seconds": round(time.time() - t0, 1),
                     "last_batch_rows": n, "updated_at": now}}, upsert=True)
        self.db.batch_stats.update_one({"_id": bid}, {"$set": {
            "valid_rows": n, "late_accepted": late, "predictions": len(scored), "missing_history": len(waiting),
            "max_ts": f"{gmax:%Y-%m-%d %H:%M}",
            "clock": f"{gmax:%Y-%m-%d %H:%M}", "seconds": round(time.time() - t0, 1), "updated_at": now}}, upsert=True)
        print(f"batch {bid}: {n} dòng hợp lệ (trễ {late}), {len(scored)} dự báo, {len(waiting)} thiếu lịch sử, "
              f"đồng hồ {gmax:%d/%m %H:%M}, {time.time() - t0:.1f}s", flush=True)


def write_rejects(db):
    def run(df, bid):
        rows = df.select("reason", "raw", "kafka_ts", "partition", "offset").collect()
        if not rows:
            return
        by = {}
        for r in rows:
            by[r.reason] = by.get(r.reason, 0) + 1
        db.reject_stats.update_one({"_id": bid}, {"$set": {"by_reason": by, "rows": len(rows)}}, upsert=True)
        db.stream_rejects.insert_many([{"reason": r.reason, "raw": (r.raw or "")[:240], "kafka_ts": r.kafka_ts,
                                        "partition": r.partition, "offset": r.offset} for r in rows[-50:]])
    return run


class Progress(StreamingQueryListener):
    """Đếm số dòng bị watermark bỏ (trễ quá hạn) và số bản trùng bị bỏ từ state operator của truy vấn forecast."""

    def __init__(self, db):
        self.db = db

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        if p.name != "forecast":
            self.db.progress.update_one({"_id": f"{p.name}-{p.batchId}"}, {"$set": {
                "query": p.name, "batch": p.batchId, "kafka_rows": p.numInputRows}}, upsert=True)
            return
        dropped_late = sum(op.numRowsDroppedByWatermark for op in p.stateOperators)
        dup = sum(int((op.customMetrics or {}).get("numDroppedDuplicateRows", 0)) for op in p.stateOperators)
        self.db.progress.update_one({"_id": f"{p.name}-{p.batchId}"}, {"$set": {
            "query": p.name, "batch": p.batchId, "dropped_late": dropped_late, "dropped_duplicates": dup,
            "kafka_rows": p.numInputRows}}, upsert=True)
        self.db.stream_stats.update_one({"_id": "pipeline"}, {
            "$set": {"input_rows_per_s": round(p.inputRowsPerSecond or 0, 1),
                     "processed_rows_per_s": round(p.processedRowsPerSecond or 0, 1),
                     "watermark": (p.eventTime or {}).get("watermark")}}, upsert=True)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--topic", default="weather.silver.hourly")
    ap.add_argument("--lake", default="/data/lake")
    ap.add_argument("--predict-from", default="2024-09-04 00:00")
    ap.add_argument("--trigger", default="10 seconds")
    ap.add_argument("--max-offsets", type=int, default=4000)
    ap.add_argument("--mongo", default=os.environ.get("MONGO_URI", "mongodb://mongo:27017"))
    args = ap.parse_args()
    spark = get_spark("kafka_stream", master="local[3]", shuffle_partitions=6,
                      conf={"spark.jars.packages": KAFKA_PKG, "spark.sql.adaptive.enabled": "false"})
    spark.sparkContext.setLogLevel("WARN")
    db = MongoClient(args.mongo)["rain_stream"]
    db.predictions.create_index([("ts", ASCENDING)])
    db.predictions.create_index([("cell", ASCENDING), ("ts", ASCENDING)])
    db.province_risk.create_index([("ts", ASCENDING)])
    if "stream_rejects" not in db.list_collection_names():
        db.create_collection("stream_rejects", capped=True, size=2_000_000)
    spark.streams.addListener(Progress(db))

    cells = spark.read.parquet(f"{args.lake}/silver/cells").select("grid_id")
    kdf = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", args.bootstrap)
           .option("subscribe", args.topic).option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", args.max_offsets).option("failOnDataLoss", "false").load())
    parsed = parse(kdf, cells)
    ck = Path(args.lake) / "stream_kafka" / "checkpoints"

    (parsed.where(F.col("reason").isNotNull()).writeStream.queryName("rejects").foreachBatch(write_rejects(db))
     .option("checkpointLocation", str(ck / "rejects")).trigger(processingTime=args.trigger).start())

    fc = Forecaster(spark, db, args.lake, datetime.strptime(args.predict_from, "%Y-%m-%d %H:%M"))
    valid = (parsed.where(F.col("reason").isNull()).select("grid_id", "event_ts", "kafka_ts", "partition", "offset", *VARS)
             .withColumn("kafka_ts", F.col("kafka_ts")).withWatermark("event_ts", WATERMARK)
             .dropDuplicatesWithinWatermark(["grid_id", "event_ts"]))
    (valid.writeStream.queryName("forecast").foreachBatch(fc.process)
     .option("checkpointLocation", str(ck / "forecast")).trigger(processingTime=args.trigger).start())
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
