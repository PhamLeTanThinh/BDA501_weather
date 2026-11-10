"""Ghi kết quả của làn batch vào MongoDB để phục vụ người dùng (cạnh dự báo của làn streaming).

climatology  ← output MapReduce (data/mr/output): tỷ lệ giờ có mưa lớn theo điểm × tháng, năm train
daily_stats  ← Spark SQL trên gold: mưa ngày và cờ ngày mưa lớn theo điểm × ngày, kèm tỉnh mới

Upsert theo _id cố định nên chạy lại không sinh bản trùng.

    python -m src.serving.load_batch_to_mongo --profile pilot
"""
import argparse
import json
import os
import sys

from pymongo import ASCENDING, MongoClient, ReplaceOne

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark


def climatology_docs(cfg, locations):
    docs = []
    for f in sorted((ROOT / "data" / "mr" / "output").glob("part-*")):
        for line in f.read_text(encoding="utf-8").splitlines():
            key, val = line.split("\t")
            loc, month = key.split("|")
            n, heavy, rate, mean_p, max_p = val.split(",")
            meta = locations.get(loc, {})
            docs.append({"_id": key, "location_id": loc, "month": int(month),
                         "new_province_code": meta.get("new_province_code"), "new_province": meta.get("new_province"),
                         "hours": int(n), "heavy_hours": int(heavy), "heavy_rate": float(rate),
                         "mean_rain_mm_h": float(mean_p), "max_rain_mm_h": float(max_p),
                         "period": f"train {cfg['years'][0]}–2022", "source": "hadoop_mapreduce"})
    return docs


def daily_docs(spark, cfg):
    spark.read.parquet(str(path(cfg, "lake") / "gold" / "features_hourly")).createOrReplaceTempView("gold")
    rows = spark.sql("""
        SELECT location_id, new_province_code, new_province, to_date(ts) AS day,
               round(sum(precipitation), 1) AS rain_mm, count(precipitation) AS hours
        FROM gold GROUP BY location_id, new_province_code, new_province, to_date(ts)""").collect()
    return [{"_id": f"{r.location_id}|{r.day}", "location_id": r.location_id, "new_province_code": r.new_province_code,
             "new_province": r.new_province, "date": str(r.day), "rain_mm": float(r.rain_mm or 0), "hours": int(r.hours),
             "heavy_day": bool((r.rain_mm or 0) > cfg["label"]["threshold_mm"]), "source": "spark_sql"} for r in rows]


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="pilot")
    cfg = load_config(profile=ap.parse_args().profile)
    sc = cfg["streaming"]
    db = MongoClient(os.environ.get("MONGO_URI", sc["mongo_uri"]))[sc["mongo_db"]]
    spark = get_spark("load_batch_to_mongo", driver_memory="2g")
    locs = {r.location_id: r.asDict() for r in spark.read.parquet(str(path(cfg, "lake") / "silver" / "locations")).collect()}
    clim, daily = climatology_docs(cfg, locs), daily_docs(spark, cfg)
    spark.stop()
    for name, docs, idx in (("climatology", clim, [("new_province_code", ASCENDING), ("month", ASCENDING)]),
                            ("daily_stats", daily, [("new_province_code", ASCENDING), ("date", ASCENDING)])):
        db[name].bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs], ordered=False)
        db[name].create_index(idx, name="province_time")
    # read-back
    q1 = db.climatology.find_one({"location_id": "HUE", "month": 10}, {"_id": 0})
    q2 = list(db.daily_stats.aggregate([{"$match": {"heavy_day": True}},
                                        {"$group": {"_id": "$new_province", "heavy_days": {"$sum": 1}}},
                                        {"$sort": {"heavy_days": -1}}, {"$limit": 5}]))
    out = {"counts": {"climatology": db.climatology.count_documents({}), "daily_stats": db.daily_stats.count_documents({})},
           "sample_climatology": q1, "top_provinces_heavy_days": q2}
    rep = ROOT / cfg["paths"]["reports"] / "mongo"
    rep.mkdir(parents=True, exist_ok=True)
    (rep / "batch_read_back.json").write_text(json.dumps(out, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
