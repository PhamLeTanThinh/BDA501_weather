"""EDA cho bản chính thức (ERA5 CDS, ô đất liền) → reports/official/eda/.

Đọc silver và gold, tính bằng Spark, chỉ đưa bảng tổng hợp nhỏ về pandas để ghi CSV và vẽ hình:

  volume.json                   số dòng, số ô, khoảng thời gian, dung lượng từng lớp
  precip_quantiles.csv          phân vị mưa giờ và mưa 24 giờ tới; tỷ lệ giờ không mưa
  label_by_split.csv            số dòng, số dương, tỷ lệ dương theo split
  label_by_region_month.csv     tỷ lệ mưa lớn theo vùng × tháng (mùa mưa)    → fig_label_region_month.png
  label_by_cell.csv             tỷ lệ mưa lớn theo ô                          → fig_label_map.png
  label_by_new_province.csv     theo tỉnh mới
  label_by_year.csv             theo năm
  feature_label_corr.csv        tương quan từng feature M1/M4 với nhãn trên train
  crosscheck_openmeteo.csv      so CDS với Open-Meteo tại 63 điểm (mưa ngày, nhiệt độ, độ lệch giờ)

    scripts/run_in_spark.sh src.analytics.eda_official --profile official
"""
import argparse
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src.common.config import ROOT, load_config, path  # noqa: E402
from src.common.spark import get_spark  # noqa: E402
from src.pipeline.cds_gold import FEATURES  # noqa: E402

# bảng màu tham chiếu (skill dataviz): xanh tuần tự cho độ lớn, thứ tự categorical cố định cho vùng
SEQ = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
CAT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
REGIONS = ["Bac", "BacTrungBo", "NamTrungBo", "TayNguyen", "Nam"]
REGION_VI = {"Bac": "Bắc Bộ", "BacTrungBo": "Bắc Trung Bộ", "NamTrungBo": "Nam Trung Bộ",
             "TayNguyen": "Tây Nguyên", "Nam": "Nam Bộ"}


def dir_size_gb(p):
    return round(sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(p) for f in fs) / 1e9, 2)


def style(ax):
    ax.set_facecolor(SURFACE)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=9)
    ax.grid(color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)


def volume(cfg, silver, gold):
    lake = path(cfg, "lake")
    b = silver.agg(F.count("*").alias("rows"), F.countDistinct("grid_id").alias("cells"),
                   F.min("ts").alias("first"), F.max("ts").alias("last")).first()
    return {"silver_rows": b.rows, "cells": b.cells, "first_ts_vn": str(b.first), "last_ts_vn": str(b.last),
            "gold_rows": gold.count(), "gold_columns": len(gold.columns),
            "size_gb": {layer: dir_size_gb(lake / layer) for layer in ("bronze", "silver", "gold")},
            "raw_gb": dir_size_gb(path(cfg, "raw_cds"))}


def precip_quantiles(silver, gold):
    qs = [0.5, 0.75, 0.9, 0.95, 0.99, 0.999]
    hourly = silver.approxQuantile("precipitation", qs, 0.0005)
    next24 = gold.approxQuantile("rain_next_24h", qs, 0.0005)
    dry = silver.agg(F.avg((F.col("precipitation") < 0.1).cast("int"))).first()[0]
    mx = silver.agg(F.max("precipitation")).first()[0]
    mx24 = gold.agg(F.max("rain_next_24h")).first()[0]
    rows = [{"quantile": q, "hourly_mm": h, "next_24h_mm": n} for q, h, n in zip(qs, hourly, next24)]
    rows.append({"quantile": "max", "hourly_mm": mx, "next_24h_mm": mx24})
    rows.append({"quantile": "share_hours_below_0.1mm", "hourly_mm": dry, "next_24h_mm": None})
    return pd.DataFrame(rows)


def label_rates(gold, by):
    return (gold.where(F.col("label").isNotNull()).groupBy(*by)
            .agg(F.count("*").alias("rows"), F.sum("label").alias("positives"),
                 F.avg("label").alias("positive_rate")).orderBy(*by).toPandas())


def fig_region_month(df, out):
    fig, ax = plt.subplots(figsize=(8, 4.2), facecolor=SURFACE)
    style(ax)
    for color, reg in zip(CAT, REGIONS):
        d = df[df.region == reg].sort_values("month")
        ax.plot(d.month, d.positive_rate * 100, color=color, linewidth=2, marker="o", markersize=4,
                label=REGION_VI[reg])
    ax.set_xticks(range(1, 13))
    ax.set_xlabel("Tháng", color=INK2)
    ax.set_ylabel("Tỷ lệ giờ có nhãn mưa lớn (%)", color=INK2)
    # chú thích đặt dưới trục để không đè lên đỉnh mùa mưa của các vùng
    ax.legend(frameon=False, fontsize=9, labelcolor=INK2, ncol=5, loc="upper center",
              bbox_to_anchor=(0.5, -0.16))
    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def fig_map(df, out):
    cmap = LinearSegmentedColormap.from_list("seq", SEQ)
    fig, ax = plt.subplots(figsize=(5.2, 8), facecolor=SURFACE)
    ax.set_facecolor(SURFACE)
    sc = ax.scatter(df.lon, df.lat, c=df.positive_rate * 100, cmap=cmap, marker="s", s=26, linewidths=0)
    ax.set_aspect("equal")
    for s in ax.spines.values():
        s.set_visible(False)
    ax.tick_params(colors=INK2, labelsize=8)
    cb = fig.colorbar(sc, ax=ax, shrink=0.5, pad=0.02)
    cb.set_label("% giờ có nhãn mưa lớn", color=INK2, fontsize=9)
    cb.ax.tick_params(colors=INK2, labelsize=8)
    cb.outline.set_visible(False)
    ax.set_title(f"Tỷ lệ mưa lớn theo ô ERA5 ({len(df)} ô đất liền)", color=INK, fontsize=11, loc="left")
    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def feature_corr(gold):
    train = gold.where((F.col("split") == "train") & F.col("label").isNotNull())
    rows = []
    for c in FEATURES:
        r = train.select(F.corr(c, "label").alias("r"),
                         F.avg(F.col(c).isNull().cast("int")).alias("null_rate")).first()
        rows.append({"feature": c, "group": "M4" if c in FEATURES[24:] else "M1",
                     "corr_with_label": r.r, "null_rate": r.null_rate})
    return pd.DataFrame(rows).sort_values("corr_with_label", key=abs, ascending=False)


def crosscheck(spark, cfg, silver, cells):
    """CDS (ô chứa điểm) so với Open-Meteo (ô trung tâm của điểm) tại 63 điểm, 2015–2025."""
    full = load_config(profile="full")
    raw_om = path(full, "raw")
    if not raw_om.exists():
        return None
    from src.pipeline.bronze_silver import RAW_SCHEMA, VARS as OM_VARS
    om = (spark.read.schema(RAW_SCHEMA).option("multiLine", True).json(str(raw_om / "grid=*" / "year=*.json"))
          .select(F.col("_provenance.grid_id").alias("om_grid"),
                  F.explode(F.arrays_zip(F.col("hourly.time").alias("time"),
                                         F.col("hourly.precipitation").alias("p"),
                                         F.col("hourly.temperature_2m").alias("t"))).alias("r"))
          .select("om_grid", F.to_timestamp("r.time", "yyyy-MM-dd'T'HH:mm").alias("ts"),
                  F.col("r.p").alias("om_precip"), F.col("r.t").alias("om_temp")))
    plan = (spark.read.csv(str(path(full, "grid_plan")), header=True)
            .where(F.col("role") == "center").select("location_id", F.col("grid_id").alias("om_grid")))
    pts = (cells.where(F.col("location_ids").isNotNull())
           .select("grid_id", F.explode(F.split("location_ids", r"\|")).alias("location_id")))
    cds = silver.join(F.broadcast(pts), "grid_id").select("location_id", "ts", F.col("precipitation").alias("cds_precip"),
                                                       F.col("temperature_2m").alias("cds_temp"))
    j = cds.join(om.join(F.broadcast(plan), "om_grid"), ["location_id", "ts"])
    hourly = j.groupBy("location_id").agg(F.count("*").alias("hours"),
                                          F.corr("cds_precip", "om_precip").alias("hourly_precip_corr"),
                                          F.avg(F.col("cds_temp") - F.col("om_temp")).alias("temp_bias_c"),
                                          F.avg(F.abs(F.col("cds_temp") - F.col("om_temp"))).alias("temp_mae_c"))
    daily = (j.groupBy("location_id", F.to_date("ts").alias("d"))
             .agg(F.sum("cds_precip").alias("c"), F.sum("om_precip").alias("o"))
             .groupBy("location_id").agg(F.corr("c", "o").alias("daily_precip_corr"),
                                         (F.sum("c") / F.sum("o")).alias("precip_ratio_cds_over_om")))
    # độ lệch giờ: tương quan mưa giờ khi dịch Open-Meteo ±1 giờ; đúng múi giờ thì lag 0 cao nhất
    lags = {}
    for lag in (-1, 0, 1):
        sh = om.withColumn("ts", F.col("ts") + F.expr(f"INTERVAL {lag} HOURS")).join(F.broadcast(plan), "om_grid")
        lags[f"corr_lag_{lag:+d}h"] = cds.join(sh, ["location_id", "ts"]).select(
            F.corr("cds_precip", "om_precip")).first()[0]
    res = hourly.join(daily, "location_id").orderBy("location_id").toPandas()
    return res, lags


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="official")
    cfg = load_config(profile=ap.parse_args().profile)
    spark = get_spark("eda_official", master="local[8]", shuffle_partitions=64)
    lake = path(cfg, "lake")
    out = ROOT / cfg["paths"]["reports"] / "eda"
    out.mkdir(parents=True, exist_ok=True)
    silver = spark.read.parquet(str(lake / "silver" / "grid_weather"))
    cells = spark.read.parquet(str(lake / "silver" / "cells"))
    gold = spark.read.parquet(str(lake / "gold" / "features_hourly"))

    vol = volume(cfg, silver, gold)
    (out / "volume.json").write_text(json.dumps(vol, indent=2, ensure_ascii=False), encoding="utf-8")
    precip_quantiles(silver, gold).to_csv(out / "precip_quantiles.csv", index=False)
    label_rates(gold, ["split"]).to_csv(out / "label_by_split.csv", index=False)
    rm = label_rates(gold, ["region", "month"])
    rm.to_csv(out / "label_by_region_month.csv", index=False)
    fig_region_month(rm, out / "fig_label_region_month.png")
    by_cell = label_rates(gold, ["location_id", "lat", "lon", "new_province", "region"])
    by_cell.to_csv(out / "label_by_cell.csv", index=False)
    fig_map(by_cell, out / "fig_label_map.png")
    label_rates(gold, ["new_province"]).sort_values("positive_rate", ascending=False).to_csv(
        out / "label_by_new_province.csv", index=False)
    label_rates(gold, ["year"]).to_csv(out / "label_by_year.csv", index=False)
    feature_corr(gold).to_csv(out / "feature_label_corr.csv", index=False)
    cc = crosscheck(spark, cfg, silver, cells)
    if cc is not None:
        res, lags = cc
        res.to_csv(out / "crosscheck_openmeteo.csv", index=False)
        vol["crosscheck_openmeteo"] = {
            "points": len(res), "median_daily_precip_corr": float(res.daily_precip_corr.median()),
            "median_hourly_precip_corr": float(res.hourly_precip_corr.median()),
            "median_temp_bias_c": float(res.temp_bias_c.median()), **lags}
        (out / "volume.json").write_text(json.dumps(vol, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(vol, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
