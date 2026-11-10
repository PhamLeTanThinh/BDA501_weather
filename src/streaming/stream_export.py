"""Xuất kết quả lượt streaming (MongoDB rain_stream) ra JSON nhỏ cho deck: chuỗi Yagi 2 ô, mẫu document, số liệu batch.

    python -m src.streaming.stream_export      → reports/streaming/stream_deck.json
"""
import json
import os

from pymongo import MongoClient

from src.common.config import ROOT

CELLS = {"g20.75_106.75": "Hải Phòng", "g21.00_107.00": "Quảng Ninh"}


def main():
    db = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongo:27017"))["rain_stream"]
    out = {"cells": CELLS}
    out["series"] = {c: list(db.predictions.find({"cell": c, "status": "ok"},
                                                 {"_id": 0, "ts": 1, "p": 1, "alert": 1, "observed_24h_mm": 1, "label": 1,
                                                  "revisions": 1}).sort("ts", 1)) for c in CELLS}
    doc = db.predictions.find_one({"cell": "g20.75_106.75", "ts": "2024-09-06 19:00"}) or {}
    doc.pop("updated_at", None)
    out["sample_doc"] = doc
    # đúng / sai trên toàn bộ ô-giờ đã biết mưa thật
    conf = {}
    for r in db.predictions.aggregate([{"$match": {"status": "ok", "label": {"$in": [0, 1]}}},
                                       {"$group": {"_id": {"a": "$alert", "l": "$label"}, "n": {"$sum": 1}}}]):
        conf[{(True, 1): "tp", (True, 0): "fp", (False, 1): "fn", (False, 0): "tn"}[(r["_id"]["a"], r["_id"]["l"])]] = r["n"]
    out["confusion_all_cells"] = conf
    secs = [b["seconds"] for b in db.batch_stats.find({}, {"seconds": 1})]
    out["batches"] = {"n": len(secs), "mean_seconds": round(sum(secs) / len(secs), 1) if secs else None,
                      "max_seconds": max(secs) if secs else None}
    out["alerts"] = db.predictions.count_documents({"status": "ok", "alert": True})
    out["revised"] = db.predictions.count_documents({"$or": [{"revisions": {"$gt": 1}}, {"was_missing": True, "status": "ok"}]})
    out["scenario"] = db.stream_stats.find_one({"_id": "scenario"}, {"_id": 0})
    p = ROOT / "reports" / "streaming" / "stream_deck.json"
    p.write_text(json.dumps(out, indent=1, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"→ {p} · {sum(len(v) for v in out['series'].values())} điểm chuỗi · {conf}")


if __name__ == "__main__":
    main()
