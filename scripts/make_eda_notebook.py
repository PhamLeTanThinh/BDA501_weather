"""Sinh notebooks/eda_official.ipynb: EDA chạy từng khối, dùng lại hàm của src.analytics.eda_official.

    .venv/Scripts/python.exe scripts/make_eda_notebook.py
"""
from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parents[1]
md, code = nbf.v4.new_markdown_cell, nbf.v4.new_code_cell

cells = [
    md("# EDA – ERA5 CDS, ô đất liền Việt Nam (bản chính thức)\n\n"
       "Đọc **silver** (`grid_weather`, 47,5 triệu dòng ô × giờ) và **gold** (`features_hourly`) từ lake "
       "`D:/BDA501_data/lake` bằng PySpark chạy trên máy (Java 17). Mỗi khối tính bằng Spark rồi chỉ đưa bảng "
       "tổng hợp nhỏ về pandas để xem.\n\n"
       "Hàm tính toán nằm ở `src/analytics/eda_official.py` (cùng code với bước EDA trong pipeline); notebook này "
       "gọi lại từng hàm để chạy và xem kết quả theo từng khối."),
    code("import os, sys, json\n"
         "from pathlib import Path\n"
         "ROOT = Path.cwd().parent if Path.cwd().name == 'notebooks' else Path.cwd()\n"
         "sys.path.insert(0, str(ROOT)); os.chdir(ROOT)\n"
         "import pandas as pd\n"
         "from IPython.display import Image, display\n"
         "from pyspark.sql import functions as F\n"
         "from src.common.config import load_config, path\n"
         "from src.common.spark import get_spark\n"
         "from src.analytics import eda_official as eda\n"
         "pd.set_option('display.float_format', lambda x: f'{x:,.4f}')\n"
         "cfg = load_config(profile='official')\n"
         "spark = get_spark('eda_notebook', master='local[8]', shuffle_partitions=64)\n"
         "lake = path(cfg, 'lake')\n"
         "silver = spark.read.parquet(str(lake / 'silver' / 'grid_weather'))\n"
         "cells = spark.read.parquet(str(lake / 'silver' / 'cells'))\n"
         "gold = spark.read.parquet(str(lake / 'gold' / 'features_hourly'))\n"
         "OUT = ROOT / 'reports' / 'official' / 'eda'; OUT.mkdir(parents=True, exist_ok=True)\n"
         "print(lake)"),
    md("## 1. Quy mô dữ liệu\nSố dòng, số ô, khoảng thời gian (giờ Việt Nam) và dung lượng từng lớp."),
    code("vol = eda.volume(cfg, silver, gold)\nvol"),
    md("## 2. Schema silver và gold"),
    code("silver.printSchema()\nprint(len(gold.columns), 'cột gold:', gold.columns)"),
    md("## 3. Chất lượng silver\nĐộ đủ giờ theo ô × năm, số giá trị bị đặt null vì ngoài khoảng vật lý."),
    code("silver_summary = json.loads((ROOT / 'reports/official/quality/silver_summary.json').read_text(encoding='utf-8'))\n"
         "silver_summary"),
    code("comp = pd.read_csv(ROOT / 'reports/official/quality/silver_cell_year_completeness.csv')\n"
         "comp.assign(missing=comp.expected - comp.hours).describe()"),
    md("## 4. Thống kê mô tả các biến silver"),
    code("silver.select(*eda_vars).summary('mean', 'stddev', 'min', '25%', '50%', '75%', 'max').toPandas().set_index('summary').T"
         .replace("eda_vars", "[c for c in silver.columns if c not in ('grid_id', 'ts', 'ts_epoch', 'year')]")),
    md("## 5. Phân bố lượng mưa\nMưa giờ tại ô và tổng mưa 24 giờ tới (biến tạo nhãn)."),
    code("q = eda.precip_quantiles(silver, gold)\nq"),
    md("## 6. Nhãn mưa lớn theo split\nNhãn = 1 khi tổng mưa (t, t+24h] > 50 mm."),
    code("by_split = eda.label_rates(gold, ['split'])\nby_split"),
    md("## 7. Mùa mưa lớn theo vùng × tháng"),
    code("rm = eda.label_rates(gold, ['region', 'month'])\n"
         "eda.fig_region_month(rm, OUT / 'fig_label_region_month.png')\n"
         "display(Image(OUT / 'fig_label_region_month.png', width=820))\n"
         "rm.pivot(index='month', columns='region', values='positive_rate').mul(100).round(2)"),
    md("## 8. Phân bố không gian: theo ô và theo tỉnh mới"),
    code("by_cell = eda.label_rates(gold, ['location_id', 'lat', 'lon', 'new_province', 'region'])\n"
         "eda.fig_map(by_cell, OUT / 'fig_label_map.png')\n"
         "display(Image(OUT / 'fig_label_map.png', width=420))"),
    code("by_prov = eda.label_rates(gold, ['new_province']).sort_values('positive_rate', ascending=False)\nby_prov.head(15)"),
    md("## 9. Theo năm"),
    code("by_year = eda.label_rates(gold, ['year'])\nby_year"),
    md("## 10. Tương quan feature với nhãn (train)\nM1 = khí tượng bề mặt (24 feature), M4 = khí quyển (9 feature từ 6 biến mới)."),
    code("corr = eda.feature_corr(gold)\ncorr"),
    md("## 11. Đối chiếu với Open-Meteo tại 63 điểm\nCùng nguồn ERA5; tương quan cao nhất ở độ lệch 0 giờ xác nhận đổi UTC → giờ VN đúng."),
    code("cc = eda.crosscheck(spark, cfg, silver, cells)\n"
         "if cc is not None:\n"
         "    res, lags = cc\n"
         "    display(pd.Series(lags, name='hourly precip corr'))\n"
         "    display(res.describe())"),
    md("## 12. Ghi kết quả và trang báo cáo\nGhi CSV/JSON như bước EDA của pipeline, rồi dựng trang HTML và chụp ảnh từng phần bằng Playwright."),
    code("(OUT / 'volume.json').write_text(json.dumps({**vol, **({'crosscheck_openmeteo': {\n"
         "    'points': len(res), 'median_daily_precip_corr': float(res.daily_precip_corr.median()),\n"
         "    'median_hourly_precip_corr': float(res.hourly_precip_corr.median()),\n"
         "    'median_temp_bias_c': float(res.temp_bias_c.median()), **lags}} if cc is not None else {})},\n"
         "    indent=2, ensure_ascii=False), encoding='utf-8')\n"
         "q.to_csv(OUT / 'precip_quantiles.csv', index=False)\n"
         "by_split.to_csv(OUT / 'label_by_split.csv', index=False)\n"
         "rm.to_csv(OUT / 'label_by_region_month.csv', index=False)\n"
         "by_cell.to_csv(OUT / 'label_by_cell.csv', index=False)\n"
         "by_prov.to_csv(OUT / 'label_by_new_province.csv', index=False)\n"
         "by_year.to_csv(OUT / 'label_by_year.csv', index=False)\n"
         "corr.to_csv(OUT / 'feature_label_corr.csv', index=False)\n"
         "if cc is not None: res.to_csv(OUT / 'crosscheck_openmeteo.csv', index=False)\n"
         "from src.analytics import eda_report\n"
         "page = OUT / 'eda_report.html'\n"
         "page.write_text(eda_report.build_html(), encoding='utf-8')\n"
         "try:  # Playwright có trên máy; trong container chỉ ghi HTML\n"
         "    print(eda_report.screenshot(page, OUT / 'screens'))\n"
         "except ImportError:\n"
         "    print('Đã ghi', page, '- chụp ảnh trên máy: .venv/Scripts/python.exe -m src.analytics.eda_report')"),
    code("spark.stop()"),
]

nb = nbf.v4.new_notebook(cells=cells, metadata={
    "kernelspec": {"name": "python3", "display_name": "Python 3 (.venv)", "language": "python"}})
dest = ROOT / "notebooks" / "eda_official.ipynb"
dest.parent.mkdir(exist_ok=True)
nbf.write(nb, dest)
print(dest)
