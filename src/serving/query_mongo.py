"""Đọc lại kết quả từ MongoDB theo đúng các truy vấn người dùng cần, kèm index và explain.

Q1  Ban PCTT: các tỉnh có cảnh báo tại một giờ, xếp theo risk score   → province_risk, index (prediction_time, province_risk_score)
Q2  Giao thông: chuỗi xác suất 24h tới của một điểm trong khoảng ngày  → predictions, index (location_id, prediction_time)
Q3  Tổng hợp: số giờ cảnh báo theo tỉnh                                → aggregation pipeline

    python -m src.serving.query_mongo --profile pilot
Kết quả: reports/mongo/*.json
"""
import argparse
import json
import os
import sys

from pymongo import ASCENDING, DESCENDING, MongoClient

from src.common.config import ROOT, load_config


def winning(explain):
    """Tên stage đầu vào thực tế (IXSCAN hay COLLSCAN) trong winning plan."""
    stages, node = [], explain["queryPlanner"]["winningPlan"]
    node = node.get("queryPlan", node)
    while node:
        stages.append(node.get("stage"))
        node = node.get("inputStage")
    return stages


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="pilot")
    ap.add_argument("--time", default="2024-09-07T00:00:00+07:00")
    ap.add_argument("--location", default="HPHO")
    args = ap.parse_args()
    cfg = load_config(profile=args.profile)
    sc = cfg["streaming"]
    db = MongoClient(os.environ.get("MONGO_URI", sc["mongo_uri"]))[sc["mongo_db"]]
    pred, prov = db[sc["mongo_collection"]], db["province_risk"]
    pred.create_index([("location_id", ASCENDING), ("prediction_time", ASCENDING)], name="loc_time")
    prov.create_index([("prediction_time", ASCENDING), ("province_risk_score", DESCENDING)], name="time_score")

    q1_filter = {"prediction_time": args.time, "alert": True}
    q1 = list(prov.find(q1_filter, {"_id": 0}).sort("province_risk_score", DESCENDING))
    q1_plan = winning(prov.find(q1_filter).sort("province_risk_score", DESCENDING).explain())

    q2_filter = {"location_id": args.location, "status": "ok",
                 "prediction_time": {"$gte": "2024-09-05T00:00:00+07:00", "$lt": "2024-09-09T00:00:00+07:00"}}
    q2_proj = {"_id": 0, "prediction_time": 1, "heavy_rain_probability": 1, "is_heavy_rain_predicted": 1,
               "rain_24h_observed": 1}
    q2 = list(pred.find(q2_filter, q2_proj).sort("prediction_time", ASCENDING))
    q2_plan = winning(pred.find(q2_filter).sort("prediction_time", ASCENDING).explain())

    q3 = list(prov.aggregate([
        {"$group": {"_id": "$province_code", "hours": {"$sum": 1},
                    "alert_hours": {"$sum": {"$cond": ["$alert", 1, 0]}},
                    "max_risk": {"$max": "$province_risk_score"}}},
        {"$sort": {"alert_hours": -1}}]))

    out = ROOT / cfg["paths"]["reports"] / "mongo"
    out.mkdir(parents=True, exist_ok=True)
    result = {
        "counts": {"predictions": pred.count_documents({}), "province_risk": prov.count_documents({}),
                   "distinct_ids_predictions": len(pred.distinct("_id"))},
        "indexes": {"predictions": list(pred.index_information()), "province_risk": list(prov.index_information())},
        "q1_alerts_at_time": {"filter": q1_filter, "plan_stages": q1_plan, "rows": q1},
        "q2_location_series": {"filter": q2_filter, "plan_stages": q2_plan, "n": len(q2), "rows": q2},
        "q3_alert_hours_by_province": q3,
        "sample_prediction": pred.find_one({"location_id": args.location, "prediction_time": args.time}),
    }
    (out / "read_back.json").write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("counts", "indexes")}, indent=2))
    print("Q1", q1_plan, q1)
    print("Q2", q2_plan, len(q2), "dòng")
    print("Q3", q3)


if __name__ == "__main__":
    main()
