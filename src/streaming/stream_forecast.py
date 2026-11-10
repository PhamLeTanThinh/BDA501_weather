"""Spark Structured Streaming: file mới trong stream_in → feature tại t → RF đã train → MongoDB.

Không Kafka. Mỗi micro-batch (foreachBatch):
  1. Khử trùng (grid_id, event_time), nối vào bộ đệm lịch sử trên lake (stream_state/history).
  2. Đọc lại bộ đệm 96 giờ gần nhất, tính feature bằng đúng point_features() của batch.
  3. Chỉ dự đoán các giờ vừa đến (và từ predict_from), feature đủ → RF; thiếu → insufficient_history.
  4. Upsert MongoDB theo _id = location|giờ|model|run_type (chạy lại batch không sinh bản ghi trùng),
     đồng thời ghi Parquet trên lake để hậu kiểm.
Model và ngưỡng đọc từ reports/ablation_manifest.json (đã khóa trên validation 2023, không fit lại).

    python -m src.streaming.stream_forecast --profile pilot [--model M1|M2|M3]  (mặc định: model được chọn từ ablation) [--no-mongo] [--once]
    python -m src.streaming.stream_forecast --profile pilot --evaluate
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark
from src.experiments.selection import select_model
from src.features.geospatial import load_geography
from src.features.local_weather import add_label
from src.pipeline.bronze_silver import VARS
from src.pipeline.gold_features import point_features

SCHEMA = StructType([StructField("grid_id", StringType()), StructField("event_time", StringType())]
                    + [StructField(v, DoubleType()) for v in VARS]
                    + [StructField("emitted_at_utc", StringType())])
BUFFER_HOURS = 96  # 72h rolling + 24h lag áp suất/gió/nhiệt


class Forecaster:
    def __init__(self, spark, cfg, model_name, use_mongo):
        self.spark, self.cfg, self.sc = spark, cfg, cfg["streaming"]
        manifest = json.loads((ROOT / cfg["paths"]["reports"] / "ablation_manifest.json").read_text(encoding="utf-8"))
        self.model_version = f"rf_{manifest['run_id']}_{model_name}"
        self.features = manifest["feature_sets"][model_name]
        self.threshold = manifest["thresholds_from_validation"][model_name]
        self.model = RandomForestClassificationModel.load(f"{manifest['model_dir']}/{model_name}")
        self.asm = VectorAssembler(inputCols=self.features, outputCol="features")
        lake = path(cfg, "lake")
        self.history = lake / "stream_state" / "history"
        self.out = lake / "stream_out" / "predictions"
        self.plan = (spark.read.parquet(str(lake / "silver" / "location_grid"))
                     .where(F.col("location_id").isin(self.sc["locations"])).cache())
        self.locs = spark.read.parquet(str(lake / "silver" / "locations")).cache()
        self.geo = load_geography(spark, path(cfg, "geography")).cache()
        self.coll = None
        if use_mongo:
            from pymongo import MongoClient
            self.coll = MongoClient(os.environ.get("MONGO_URI", self.sc["mongo_uri"]))[self.sc["mongo_db"]][self.sc["mongo_collection"]]

    def __call__(self, batch, batch_id):
        new = (batch.withColumn("ts", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm"))
               .dropDuplicates(["grid_id", "ts"]).withColumn("ts_epoch", F.unix_timestamp("ts")))
        new_hours = [r[0] for r in new.select("ts").distinct().collect()]
        if not new_hours:
            return
        new.select("grid_id", "ts", "ts_epoch", *VARS).write.mode("append").parquet(str(self.history))
        latest = max(new_hours)
        hist = (self.spark.read.parquet(str(self.history)).dropDuplicates(["grid_id", "ts"])
                .where(F.col("ts") > F.lit(latest) - F.expr(f"INTERVAL {BUFFER_HOURS} HOURS")))
        feats = point_features(self.cfg, hist, self.plan, self.locs, self.geo, required_geo=True)
        feats = feats.where(F.col("ts").isin(new_hours)
                            & (F.col("ts") >= F.to_timestamp(F.lit(self.sc["predict_from"]))))
        complete = F.lit(True)
        for c in self.features:
            complete = complete & F.col(c).isNotNull()
        ready = feats.where(complete)
        pred = (self.model.transform(self.asm.transform(ready))
                .select("location_id", "new_province_code", "ts", "rain_24h",
                        vector_to_array("probability")[1].alias("p")))
        rows = pred.toPandas()
        skipped = feats.where(~complete).select("location_id", "ts").collect()
        now = datetime.now(timezone.utc).isoformat()
        docs = [{"_id": f"{r.location_id}|{r.ts:%Y-%m-%dT%H:%M}|{self.model_version}|replay",
                 "location_id": r.location_id, "new_province_code": r.new_province_code,
                 "prediction_time": r.ts.strftime("%Y-%m-%dT%H:%M:00+07:00"),
                 "forecast_horizon_hours": self.cfg["label"]["horizon_hours"],
                 "heavy_rain_threshold_mm": self.cfg["label"]["threshold_mm"],
                 "heavy_rain_probability": float(r.p), "decision_threshold": self.threshold,
                 "is_heavy_rain_predicted": bool(r.p >= self.threshold),
                 "rain_24h_observed": float(r.rain_24h), "status": "ok",
                 "model_version": self.model_version, "run_type": "replay",
                 "batch_id": int(batch_id), "processed_at_utc": now} for r in rows.itertuples()]
        docs += [{"_id": f"{r.location_id}|{r.ts:%Y-%m-%dT%H:%M}|{self.model_version}|replay",
                  "location_id": r.location_id, "prediction_time": r.ts.strftime("%Y-%m-%dT%H:%M:00+07:00"),
                  "status": "insufficient_history", "model_version": self.model_version, "run_type": "replay",
                  "batch_id": int(batch_id), "processed_at_utc": now} for r in skipped]
        if not docs:
            return
        # rủi ro cấp tỉnh = max xác suất các điểm; là risk score, không phải xác suất đã hiệu chỉnh
        prov = {}
        for d in docs:
            if d["status"] == "ok":
                k = (d["new_province_code"], d["prediction_time"])
                if k not in prov or d["heavy_rain_probability"] > prov[k]["province_risk_score"]:
                    prov[k] = {"_id": f"{k[0]}|{k[1][:16]}|{self.model_version}|replay", "province_code": k[0],
                               "prediction_time": k[1], "province_risk_score": d["heavy_rain_probability"],
                               "driving_location_id": d["location_id"], "decision_threshold": self.threshold,
                               "alert": d["heavy_rain_probability"] >= self.threshold,
                               "aggregation_method": "max_point_probability",
                               "model_version": self.model_version, "run_type": "replay"}
        ok_docs = [d for d in docs if d["status"] == "ok"]
        if ok_docs:
            self.spark.createDataFrame(ok_docs).write.mode("append").json(str(self.out))
        if self.coll is not None:
            from pymongo import ReplaceOne
            self.coll.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs], ordered=False)
            if prov:
                self.coll.database["province_risk"].bulk_write(
                    [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in prov.values()], ordered=False)
        alerts = sum(d.get("is_heavy_rain_predicted", False) for d in docs)
        print(f"batch {batch_id}: {len(new_hours)} giờ mới ({min(new_hours):%d/%m %H:%M}–{latest:%d/%m %H:%M}), "
              f"{len(rows)} dự đoán, {alerts} cảnh báo, {len(skipped)} thiếu lịch sử", flush=True)


def evaluate(spark, cfg):
    """Hậu kiểm sau khi replay đã qua t+24h: nhãn tính từ chính dữ liệu đã phát, không đọc silver."""
    lake = path(cfg, "lake")
    hist = spark.read.parquet(str(lake / "stream_state" / "history")).dropDuplicates(["grid_id", "ts"])
    plan = spark.read.parquet(str(lake / "silver" / "location_grid")).where("role = 'center'")
    wx = plan.select("location_id", "grid_id").join(hist, "grid_id")
    lab = add_label(wx, horizon_hours=cfg["label"]["horizon_hours"], threshold_mm=cfg["label"]["threshold_mm"])
    pred = (spark.read.json(str(lake / "stream_out" / "predictions")).where("status = 'ok'")
            .withColumn("ts", F.to_timestamp(F.substring("prediction_time", 1, 16), "yyyy-MM-dd'T'HH:mm"))
            .dropDuplicates(["_id"]))
    j = (pred.join(lab.select("location_id", "ts", "rain_next_24h", "label"), ["location_id", "ts"])
         .where(F.col("label").isNotNull()).orderBy("location_id", "ts"))
    pdf = j.select("location_id", "ts", "heavy_rain_probability", "is_heavy_rain_predicted",
                   "rain_next_24h", "label").toPandas()
    out = ROOT / cfg["paths"]["reports"] / "yagi_replay_backtest.csv"
    pdf.to_csv(out, index=False)
    tp = int(((pdf.label == 1) & pdf.is_heavy_rain_predicted).sum())
    fp = int(((pdf.label == 0) & pdf.is_heavy_rain_predicted).sum())
    fn = int(((pdf.label == 1) & ~pdf.is_heavy_rain_predicted).sum())
    print(f"Hậu kiểm {len(pdf)} dự đoán đã có nhãn: TP={tp} FP={fp} FN={fn} → {out}")


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="pilot")
    ap.add_argument("--model", default=None, help="mặc định: theo src.experiments.selection")
    ap.add_argument("--no-mongo", action="store_true")
    ap.add_argument("--once", action="store_true", help="xử lý hết file đang có rồi dừng (availableNow)")
    ap.add_argument("--evaluate", action="store_true")
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    spark = get_spark("stream_forecast", driver_memory="3g", shuffle_partitions=4)
    if args.evaluate:
        evaluate(spark, cfg)
        return
    sc = cfg["streaming"]
    src = (spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 6)
           .json(str(ROOT / sc["stream_in"])))
    model = args.model or select_model(ROOT / cfg["paths"]["reports"] / "ablation_deltas.csv")
    print(f"Streaming dùng model {model}", flush=True)
    writer = (src.writeStream.foreachBatch(Forecaster(spark, cfg, model, not args.no_mongo))
              .option("checkpointLocation", str(ROOT / sc["checkpoint"])))
    writer = writer.trigger(availableNow=True) if args.once else writer.trigger(
        processingTime=f"{sc['trigger_seconds']} seconds")
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
