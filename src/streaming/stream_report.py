"""Đo lường và kiểm tra parity của luồng Kafka → Spark → MongoDB sau một lần chạy (kể cả có tắt job giữa chừng).

  1. Không mất, không đọc trùng: tổng bản tin trong topic = tổng dòng mỗi truy vấn Spark đã đọc (theo batch id).
  2. Rác chèn vào ↔ Spark phát hiện: theo từng loại (producer ghi số đã chèn, Spark ghi số đã loại).
  3. MongoDB không có dự báo trùng (cell, giờ).
  4. Parity: dự báo của stream = dự báo batch tính lại trên đúng các dòng stream đã nhận (cùng feature, cùng model).
  5. So với silver sạch: dự báo trùng khớp ở mọi ô-giờ có đủ dữ liệu; chỉ khác ở ô-giờ bị mất dữ liệu có chủ đích.

    python -m src.streaming.stream_report      → reports/streaming/stream_report.json, stream_report.md
"""
import argparse
import json
import os
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.common.config import ROOT
from src.common.spark import get_spark
from src.features.atmos import atmos_features
from src.features.local_weather import local_features
from src.pipeline.cds_silver import VARS


def kafka_total(bootstrap, topic):
    c = KafkaConsumer(bootstrap_servers=bootstrap)
    parts = [TopicPartition(topic, p) for p in c.partitions_for_topic(topic)]
    end, beg = c.end_offsets(parts), c.beginning_offsets(parts)
    c.close()
    return sum(end[p] - beg[p] for p in parts)


def predict(spark, wx, model, features, predict_from):
    """Feature M4 + dự báo trên một bảng (grid_id, ts, VARS) — đúng các hàm stream dùng."""
    cells = spark.read.parquet("/data/lake/silver/cells").select(F.col("grid_id").alias("location_id"))
    df = (wx.withColumnRenamed("grid_id", "location_id").withColumn("ts_epoch", F.unix_timestamp("ts"))
          .join(F.broadcast(cells), "location_id"))
    f = atmos_features(local_features(df, key="location_id"), key="location_id").where(F.col("ts") >= F.lit(predict_from))
    ok = F.lit(True)
    for c in features:
        ok = ok & F.col(c).isNotNull()
    p = (model.transform(VectorAssembler(inputCols=features, outputCol="features").transform(f.where(ok)))
         .select("location_id", "ts", vector_to_array("probability")[1].alias("p")))
    miss = f.where(~ok).select("location_id", "ts", F.lit(None).cast("double").alias("p"))
    out = p.unionByName(miss).toPandas()
    out["ts"] = pd.to_datetime(out["ts"]).dt.strftime("%Y-%m-%d %H:%M")
    return out.rename(columns={"location_id": "cell"})


def batch_features(spark, wx, features, cells, predict_from):
    """Vector feature M4 tính bằng batch (cùng hàm local_features + atmos_features) cho các ô mẫu."""
    df = (wx.where(F.col("grid_id").isin(cells)).withColumnRenamed("grid_id", "location_id")
          .withColumn("ts_epoch", F.unix_timestamp("ts")))
    f = atmos_features(local_features(df, key="location_id"), key="location_id").where(F.col("ts") >= F.lit(predict_from))
    out = f.select("location_id", "ts", *features).toPandas()
    out["ts"] = pd.to_datetime(out["ts"]).dt.strftime("%Y-%m-%d %H:%M")
    return out


def feature_parity(spark, db, accepted, silver, features, predict_from):
    """E3: so từng feature của stream (vector lưu trong foreachBatch, lần tính cuối của mỗi ô-giờ) với batch."""
    raw = spark.read.parquet("/data/lake/stream_kafka/features/b*").toPandas()
    raw["ts"] = pd.to_datetime(raw["ts"]).dt.strftime("%Y-%m-%d %H:%M")
    cells = sorted(raw.location_id.unique())
    st = raw.sort_values("batch_id").drop_duplicates(["location_id", "ts"], keep="last")   # bản tính cuối (sau dữ liệu trễ)
    recomputed = int(raw.groupby(["location_id", "ts"]).size().gt(1).sum())
    out = {"cells": cells, "stream_cell_hours": len(st), "recomputed_cell_hours": recomputed}
    for name, src in (("same_accepted_rows", accepted), ("clean_silver", silver)):
        b = batch_features(spark, src, features, cells, predict_from).dropna(subset=features)
        m = st.merge(b, on=["location_id", "ts"], suffixes=("_s", "_b"))
        diff = pd.DataFrame({c: (m[f"{c}_s"] - m[f"{c}_b"]).abs() for c in features})
        bad = diff.gt(1e-6 + 1e-6 * pd.DataFrame({c: m[f"{c}_b"].abs() for c in features})).any(axis=1)
        out[name] = {"cell_hours_compared": int(len(m)), "features_per_vector": len(features),
                     "values_compared": int(len(m) * len(features)), "cell_hours_with_any_mismatch": int(bad.sum()),
                     "max_abs_diff": float(diff.max().max()) if len(m) else None,
                     "features_with_mismatch": sorted(c for c in features if (diff[c] > 1e-6 + 1e-6 * m[f"{c}_b"].abs()).any()),
                     "examples_mismatch": m.loc[bad, ["location_id", "ts"]].head(5).to_dict("records")}
    log = pd.DataFrame(list(db.junk_log.find({}, {"_id": 0})))
    out["junk_log_events"] = int(len(log))
    out["junk_log_by_kind"] = log.groupby("kind").size().to_dict() if len(log) else {}
    out["junk_log_sample"] = log.sort_values(["cell", "event_time"]).head(12).to_dict("records") if len(log) else []
    return out


def compare(a, b, la, lb, threshold):
    m = a.merge(b, on=["cell", "ts"], how="outer", suffixes=(f"_{la}", f"_{lb}"), indicator=True)
    both = m[(m["_merge"] == "both") & m[f"p_{la}"].notna() & m[f"p_{lb}"].notna()]
    diff = (both[f"p_{la}"] - both[f"p_{lb}"]).abs()
    alert = ((both[f"p_{la}"] >= threshold) != (both[f"p_{lb}"] >= threshold)).sum()
    return {"cell_hours_both_scored": int(len(both)), "max_abs_diff_p": float(diff.max() if len(diff) else 0),
            "identical_p": int((diff <= 5e-5 + 1e-12).sum()), "alert_mismatch": int(alert),
            f"scored_only_{la}": int((m[f"p_{la}"].notna() & m[f"p_{lb}"].isna()).sum()),
            f"scored_only_{lb}": int((m[f"p_{lb}"].notna() & m[f"p_{la}"].isna()).sum())}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--topic", default="weather.silver.hourly")
    ap.add_argument("--mongo", default=os.environ.get("MONGO_URI", "mongodb://mongo:27017"))
    ap.add_argument("--start", default="2024-09-01 00:00")
    ap.add_argument("--end", default="2024-09-10 23:00")
    ap.add_argument("--predict-from", default="2024-09-04 00:00")
    args = ap.parse_args()
    db = MongoClient(args.mongo)["rain_stream"]
    rep = {"created_utc": datetime.now(timezone.utc).isoformat()}

    # 1. Kafka ↔ Spark
    total = kafka_total(args.bootstrap, args.topic)
    read = {r["_id"]: r for r in db.progress.aggregate([{"$group": {
        "_id": "$query", "rows": {"$sum": "$kafka_rows"}, "batches": {"$sum": 1}, "dropped_late": {"$sum": "$dropped_late"},
        "dropped_duplicates": {"$sum": "$dropped_duplicates"}}}])}
    rep["kafka"] = {"messages_in_topic": total,
                    "read_by_forecast_query": read.get("forecast", {}).get("rows"),
                    "read_by_rejects_query": read.get("rejects", {}).get("rows"),
                    "forecast_batches": read.get("forecast", {}).get("batches")}
    rep["kafka"]["no_loss_no_double_read"] = (rep["kafka"]["read_by_forecast_query"] == total
                                              and rep["kafka"]["read_by_rejects_query"] == total)

    # 2. rác chèn vào ↔ Spark xử lý
    prod = db.stream_stats.find_one({"_id": "producer"}) or {}
    inj = prod.get("injected", {})
    rej = {}
    for r in db.reject_stats.find({}, {"by_reason": 1}):
        for k, v in r["by_reason"].items():
            rej[k] = rej.get(k, 0) + v
    bs = next(db.batch_stats.aggregate([{"$group": {"_id": None, "valid": {"$sum": "$valid_rows"},
                                                    "late": {"$sum": "$late_accepted"}}}]), {})
    fq = read.get("forecast", {})
    rep["junk"] = [
        {"case": "duplicate (immediate + later)", "injected": inj.get("duplicate", 0) + inj.get("duplicate_later", 0),
         "handled": fq.get("dropped_duplicates"), "how": "watermark + dropDuplicatesWithinWatermark"},
        {"case": "late within 6 h + out of order", "injected": inj.get("late", 0) + inj.get("reordered", 0),
         "handled": bs.get("late"), "how": "accepted, affected hours recomputed"},
        {"case": "late beyond 6 h", "injected": inj.get("too_late", 0), "handled": fq.get("dropped_late"),
         "how": "dropped by watermark"},
        {"case": "malformed JSON", "injected": inj.get("malformed", 0), "handled": rej.get("malformed_json", 0), "how": "quarantined"},
        {"case": "missing field", "injected": inj.get("missing_field", 0), "handled": rej.get("missing_field", 0), "how": "quarantined"},
        {"case": "out-of-range value", "injected": inj.get("out_of_range", 0), "handled": rej.get("out_of_range", 0), "how": "quarantined"},
        {"case": "unknown cell", "injected": inj.get("unknown_cell", 0), "handled": rej.get("unknown_cell", 0), "how": "quarantined"},
        {"case": "missing hours (never sent)", "injected": inj.get("missing_hours", 0), "handled": None,
         "how": "features null → missing_history"},
    ]
    rep["rows"] = {"sent": prod.get("sent"), "valid_into_forecast": bs.get("valid"),
                   "rejected_records": sum(rej.values()), "dropped_duplicates": fq.get("dropped_duplicates"),
                   "dropped_late": fq.get("dropped_late"),
                   "dropped_total": sum(rej.values()) + (fq.get("dropped_duplicates") or 0) + (fq.get("dropped_late") or 0)}

    # 3. MongoDB không trùng
    dup = list(db.predictions.aggregate([{"$group": {"_id": {"c": "$cell", "t": "$ts"}, "n": {"$sum": 1}}},
                                         {"$match": {"n": {"$gt": 1}}}, {"$count": "n"}]))
    rep["mongo"] = {"prediction_docs": db.predictions.count_documents({}),
                    "status_ok": db.predictions.count_documents({"status": "ok"}),
                    "status_missing_history": db.predictions.count_documents({"status": "missing_history"}),
                    "duplicate_cell_hours": dup[0]["n"] if dup else 0}

    # 4–5. parity
    spark = get_spark("stream_report", master="local[3]", shuffle_partitions=6)
    man = json.loads((ROOT / "reports/official/model/ablation_manifest.json").read_text(encoding="utf-8"))
    feats, thr = man["feature_sets"]["M4"], float(man["thresholds_from_validation"]["M4"])
    model = RandomForestClassificationModel.load(f"/data/lake/models/official/{man['run_id']}/M4")
    pf = datetime.strptime(args.predict_from, "%Y-%m-%d %H:%M")
    acc = spark.read.parquet("/data/lake/stream_kafka/accepted/b*")
    n_acc, n_acc_d = acc.count(), acc.dropDuplicates(["grid_id", "ts"]).count()
    rep["accepted"] = {"rows": n_acc, "distinct_cell_hours": n_acc_d, "duplicates_reaching_foreachBatch": n_acc - n_acc_d}
    stream = pd.DataFrame(list(db.predictions.find({}, {"_id": 0, "cell": 1, "ts": 1, "p": 1})))
    batch_acc = predict(spark, acc.dropDuplicates(["grid_id", "ts"]).select("grid_id", "ts", *VARS), model, feats, pf)
    t0, t1 = datetime.strptime(args.start, "%Y-%m-%d %H:%M"), datetime.strptime(args.end, "%Y-%m-%d %H:%M")
    silver = (spark.read.parquet("/data/lake/silver/grid_weather").where(F.col("year") == t0.year)
              .where((F.col("ts") >= F.lit(t0)) & (F.col("ts") <= F.lit(t1))).select("grid_id", "ts", *VARS))
    batch_clean = predict(spark, silver, model, feats, pf)
    rep["parity_stream_vs_batch_same_rows"] = compare(stream, batch_acc, "stream", "batch", thr)
    rep["stream_vs_clean_silver"] = compare(stream, batch_clean, "stream", "silver", thr)
    rep["silver_rows_in_period"] = silver.count()
    rep["accepted_vs_silver_missing_cell_hours"] = rep["silver_rows_in_period"] - n_acc_d
    rep["feature_parity"] = feature_parity(spark, db, acc.dropDuplicates(["grid_id", "ts"]).select("grid_id", "ts", *VARS),
                                           silver, feats, pf)
    spark.stop()

    out = ROOT / "reports" / "streaming"
    out.mkdir(parents=True, exist_ok=True)
    (out / "stream_report.json").write_text(json.dumps(rep, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    lines = ["# Streaming run: measurements and parity", "",
             f"Kafka messages in topic: {total:,} · read by forecast query: {rep['kafka']['read_by_forecast_query']:,} "
             f"· read by rejects query: {rep['kafka']['read_by_rejects_query']:,} → no loss, no double read: "
             f"**{rep['kafka']['no_loss_no_double_read']}**", "",
             "| Case | Injected | Handled by Spark | How |", "|---|---|---|---|"]
    lines += [f"| {r['case']} | {r['injected']:,} | {'–' if r['handled'] is None else format(r['handled'], ',')} | {r['how']} |"
              for r in rep["junk"]]
    lines += ["", f"Rows: {json.dumps(rep['rows'])}", f"MongoDB: {json.dumps(rep['mongo'])}",
              f"Accepted rows: {json.dumps(rep['accepted'])}",
              f"Parity (stream vs batch on the same accepted rows; p is stored rounded to 4 decimals, so |diff| ≤ 5e-5 means equal): "
              f"{json.dumps(rep['parity_stream_vs_batch_same_rows'])}",
              f"Stream vs clean silver: {json.dumps(rep['stream_vs_clean_silver'])}",
              f"Cell-hours missing from accepted vs silver: {rep['accepted_vs_silver_missing_cell_hours']:,}",
              "", "## E3 feature-vector parity (sample cells)", "",
              "```", json.dumps(rep["feature_parity"], indent=1, ensure_ascii=False, default=str), "```"]
    (out / "stream_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
