"""Web demo luồng streaming: đọc MongoDB (rain_stream) và trả JSON cho trang bản đồ cảnh báo.

    uvicorn src.web.app:app --host 0.0.0.0 --port 8000
"""
import os
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from fastapi import FastAPI
from fastapi.responses import FileResponse
from pymongo import MongoClient

db = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongo:27017"))["rain_stream"]
app = FastAPI(title="Heavy-rain stream demo")
STATIC = Path(__file__).parent / "static"
CELLS = (pq.read_table(os.environ.get("CELLS", "/data/lake/silver/cells"), columns=["grid_id", "grid_lat", "grid_lon", "new_province"])
         .to_pandas().rename(columns={"grid_id": "cell", "grid_lat": "lat", "grid_lon": "lon", "new_province": "province"})
         .to_dict("records"))
PREDICT_FROM = "2024-09-04 00:00"


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


def totals():
    """Cộng số liệu theo batch (mỗi batch một bản ghi, khóa batch id → chạy lại batch không đếm hai lần)."""
    out = {}
    for r in db.batch_stats.aggregate([{"$group": {"_id": None, "valid_rows": {"$sum": "$valid_rows"},
                                                   "late_accepted": {"$sum": "$late_accepted"},
                                                   "predictions": {"$sum": "$predictions"},
                                                   "missing_history": {"$sum": "$missing_history"}}}]):
        r.pop("_id")
        out.update(r)
    for r in db.progress.aggregate([{"$group": {"_id": "$query", "kafka_rows": {"$sum": "$kafka_rows"},
                                                "dropped_late": {"$sum": "$dropped_late"},
                                                "dropped_duplicates": {"$sum": "$dropped_duplicates"}}}]):
        if r["_id"] == "forecast":
            out.update(dropped_late=r["dropped_late"], dropped_duplicates=r["dropped_duplicates"], kafka_rows=r["kafka_rows"])
    rej = {}
    for r in db.reject_stats.find({}, {"by_reason": 1}):
        for k, v in r["by_reason"].items():
            rej[k] = rej.get(k, 0) + v
    out["rejected"] = rej
    return out


@app.post("/api/control/{command}")
def control(command: str):
    """Nút trên web: run (bắt đầu / tiếp tục) hoặc pause (tạm dừng) producer."""
    if command not in ("run", "pause"):
        return {"ok": False}
    db.stream_stats.update_one({"_id": "control"}, {"$set": {"command": command}}, upsert=True)
    return {"ok": True, "command": command}


@app.get("/api/cells")
def cells_list():
    return {"cells": CELLS, "predict_from": PREDICT_FROM}


@app.get("/api/state")
def state():
    prod = db.stream_stats.find_one({"_id": "producer"}) or {}
    pipe = db.stream_stats.find_one({"_id": "pipeline"}) or {}
    prod.pop("_id", None)
    pipe.pop("_id", None)
    pipe.update(totals())
    latest = db.predictions.find_one({"status": "ok"}, sort=[("ts", -1)], projection={"ts": 1})
    cells, provinces, score = [], [], {}
    if latest:
        ts = latest["ts"]
        cells = list(db.predictions.find({"ts": ts}, {"_id": 0, "cell": 1, "lat": 1, "lon": 1, "p": 1, "alert": 1,
                                                       "province": 1, "status": 1, "revisions": 1, "threshold": 1}))
        provinces = list(db.province_risk.find({"ts": ts}, {"_id": 0}).sort("p_max", -1).limit(8))
        # đúng / sai trên các giờ đã biết mưa thật (qua t + 24 giờ)
        agg = db.predictions.aggregate([{"$match": {"status": "ok", "label": {"$in": [0, 1]}}},
                                        {"$group": {"_id": {"a": "$alert", "l": "$label"}, "n": {"$sum": 1}}}])
        for r in agg:
            k = {(True, 1): "tp", (True, 0): "fp", (False, 1): "fn", (False, 0): "tn"}[(r["_id"]["a"], r["_id"]["l"])]
            score[k] = r["n"]
        # dự báo được tính lại (hoặc bổ sung sau khi thiếu) vì dữ liệu trễ của ô đó mới tới
        score["revised"] = db.predictions.count_documents({"$or": [{"revisions": {"$gt": 1}},
                                                                   {"was_missing": True, "status": "ok"}]})
    else:
        ts = None
    rejects = list(db.stream_rejects.find({}, {"_id": 0, "reason": 1, "raw": 1, "offset": 1}).sort("$natural", -1).limit(8))
    # số giây kể từ lần producer gửi / Spark xong batch gần nhất (đồng hồ thật, không phải giờ dữ liệu)
    now = datetime.now(timezone.utc)
    age = lambda d: round((now - d.replace(tzinfo=timezone.utc)).total_seconds(), 1) if isinstance(d, datetime) else None
    live = {"producer_age_s": age(prod.get("updated_at")), "batch_age_s": age(pipe.get("updated_at"))}
    return {"live": live, "producer": prod, "pipeline": pipe, "ts": ts, "cells": cells, "provinces": provinces,
            "score": score, "rejects": rejects}


@app.get("/api/series/{cell}")
def series(cell: str):
    rows = db.predictions.find({"cell": cell}, {"_id": 0, "ts": 1, "p": 1, "alert": 1, "observed_24h_mm": 1,
                                                "revisions": 1, "province": 1}).sort("ts", 1)
    return list(rows)
