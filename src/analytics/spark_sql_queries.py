"""Truy vấn phân tích Spark SQL trên gold + query plan dạng formatted.

Q1  Tần suất mưa lớn theo vùng × tháng (aggregation → Exchange hashpartitioning).
Q2  Số ngày mưa lớn mỗi năm theo tỉnh mới, kèm độ cao (join với bảng địa lý → BroadcastHashJoin).
Q3  5 đợt mưa 24h lớn nhất của mỗi điểm (window function → Exchange + Sort theo location_id).

    python -m src.analytics.spark_sql_queries --profile pilot
Kết quả: reports/spark_sql/q*_result.csv, q*_plan.txt
"""
import argparse
import sys

from src.common.config import ROOT, load_config, path
from src.common.spark import get_spark

QUERIES = {
    "q1_heavy_rate_region_month": """
        SELECT region, month,
               count(*)                                  AS hours,
               sum(label)                                AS heavy_hours,
               round(100 * avg(label), 2)                AS heavy_pct,
               round(avg(rain_next_24h), 2)              AS mean_next24_mm
        FROM gold
        WHERE label IS NOT NULL
        GROUP BY region, month
        ORDER BY region, month""",
    "q2_heavy_days_province_year": """
        WITH daily AS (
            SELECT location_id, to_date(ts) AS day, sum(precipitation) AS rain_day_mm
            FROM gold GROUP BY location_id, to_date(ts))
        SELECT l.new_province, g.elevation_m, year(d.day) AS year,
               count(*)                                   AS days,
               sum(CASE WHEN d.rain_day_mm > 50 THEN 1 ELSE 0 END) AS heavy_days,
               round(max(d.rain_day_mm), 1)               AS max_day_mm
        FROM daily d
        JOIN locations l ON d.location_id = l.location_id
        JOIN geo g       ON d.location_id = g.location_id
        GROUP BY l.new_province, g.elevation_m, year(d.day)
        ORDER BY l.new_province, year""",
    "q3_top_events_per_location": """
        SELECT location_id, ts, rain_next_24h, rk FROM (
            SELECT location_id, ts, rain_next_24h,
                   row_number() OVER (PARTITION BY location_id ORDER BY rain_next_24h DESC) AS rk
            FROM gold WHERE rain_next_24h IS NOT NULL)
        WHERE rk <= 5 ORDER BY location_id, rk""",
}


def register(spark, cfg):
    lake = path(cfg, "lake")
    spark.read.parquet(str(lake / "gold" / "features_hourly")).createOrReplaceTempView("gold")
    spark.read.parquet(str(lake / "silver" / "locations")).createOrReplaceTempView("locations")
    spark.read.csv(str(path(cfg, "geography")), header=True, inferSchema=True).createOrReplaceTempView("geo")


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="pilot")
    cfg = load_config(profile=ap.parse_args().profile)
    spark = get_spark("spark_sql_queries")
    register(spark, cfg)
    out = ROOT / cfg["paths"]["reports"] / "spark_sql"
    out.mkdir(parents=True, exist_ok=True)
    for name, sql in QUERIES.items():
        df = spark.sql(sql)
        (out / f"{name}_plan.txt").write_text(df._sc._jvm.PythonSQLUtils.explainString(
            df._jdf.queryExecution(), "formatted"), encoding="utf-8")
        res = df.toPandas()
        res.to_csv(out / f"{name}_result.csv", index=False)
        print(f"{name}: {len(res)} dòng")
    spark.stop()


if __name__ == "__main__":
    main()
