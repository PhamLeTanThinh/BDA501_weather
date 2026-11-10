"""Trang báo cáo EDA (HTML) từ kết quả của eda_official, rồi chụp ảnh từng phần bằng Playwright.

    python -m src.analytics.eda_report        → reports/official/eda/eda_report.html
                                                 reports/official/eda/screens/NN_<phần>.png
"""
import base64
import html
import json

import pandas as pd

from src.common.config import ROOT

EDA = ROOT / "reports" / "official" / "eda"
QUALITY = ROOT / "reports" / "official" / "quality"
REGION_VI = {"Bac": "Bắc Bộ", "BacTrungBo": "Bắc Trung Bộ", "NamTrungBo": "Nam Trung Bộ",
             "TayNguyen": "Tây Nguyên", "Nam": "Nam Bộ"}
SPLIT_VI = {"train": "Train 2015–2022", "validation": "Validation 2023", "test": "Test 2024–2025"}

CSS = """
:root { --surface:#fcfcfb; --card:#ffffff; --ink:#0b0b0b; --ink2:#52514e; --muted:#8a8984; --line:#e4e3df;
        --accent:#2a78d6; --m4:#eb6834; }
* { box-sizing:border-box; }
body { margin:0; background:var(--surface); color:var(--ink); font:15px/1.5 "Segoe UI", system-ui, sans-serif; }
main { max-width:1080px; margin:0 auto; padding:32px 24px 64px; }
h1 { font-size:26px; margin:0 0 4px; }
.sub { color:var(--ink2); margin:0 0 28px; }
section { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:22px 24px;
          margin:0 0 20px; }
h2 { font-size:18px; margin:0 0 4px; }
.note { color:var(--ink2); font-size:13px; margin:0 0 16px; }
.tiles { display:grid; grid-template-columns:repeat(4, 1fr); gap:12px; }
.tile { border:1px solid var(--line); border-radius:10px; padding:12px 14px; }
.tile .v { font-size:24px; font-weight:600; font-variant-numeric:tabular-nums; }
.tile .k { color:var(--ink2); font-size:12.5px; }
table { width:100%; border-collapse:collapse; font-size:13.5px; font-variant-numeric:tabular-nums; }
th, td { padding:6px 10px; border-bottom:1px solid var(--line); text-align:right; }
th { color:var(--ink2); font-weight:600; background:#f6f5f2; }
th:first-child, td:first-child { text-align:left; }
.two { display:grid; grid-template-columns:1.1fr 1fr; gap:20px; align-items:start; }
img { max-width:100%; display:block; }
.tag { display:inline-block; padding:0 6px; border-radius:4px; font-size:11.5px; font-weight:600; color:#fff; }
.tag.M1 { background:var(--accent); } .tag.M4 { background:var(--m4); }
.bar { height:8px; border-radius:4px; background:var(--accent); display:inline-block; vertical-align:middle; }
"""


def n(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ") if pd.notna(x) else "–"


def pct(x, d=2):
    return f"{x * 100:.{d}f}%" if pd.notna(x) else "–"


def table(df, cols):
    """cols: [(tiêu đề, hàm định dạng dòng → chuỗi html)]."""
    head = "".join(f"<th>{html.escape(h)}</th>" for h, _ in cols)
    body = "".join("<tr>" + "".join(f"<td>{f(r)}</td>" for _, f in cols) + "</tr>" for _, r in df.iterrows())
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def img(name):
    data = base64.b64encode((EDA / name).read_bytes()).decode()
    return f'<img src="data:image/png;base64,{data}" alt="{name}">'


def section(sid, title, note, body):
    return f'<section id="{sid}"><h2>{title}</h2><p class="note">{note}</p>{body}</section>'


def build_html():
    vol = json.loads((EDA / "volume.json").read_text(encoding="utf-8"))
    silver = json.loads((QUALITY / "silver_summary.json").read_text(encoding="utf-8"))
    q = pd.read_csv(EDA / "precip_quantiles.csv")
    split = pd.read_csv(EDA / "label_by_split.csv")
    prov = pd.read_csv(EDA / "label_by_new_province.csv")
    year = pd.read_csv(EDA / "label_by_year.csv")
    corr = pd.read_csv(EDA / "feature_label_corr.csv")
    s = []

    sz = vol["size_gb"]
    tiles = [(n(vol["cells"]), "ô ERA5 đất liền (0,25°)"), (n(vol["silver_rows"]), "dòng silver (ô × giờ)"),
             ("13", "biến ERA5 gốc từ CDS"), (n(vol["gold_columns"]), "cột gold (33 feature)"),
             ("2015–2025", f"{vol['first_ts_vn'][:10]} → {vol['last_ts_vn'][:10]}"),
             (f"{vol['raw_gb']} GB", "raw (68 zip NetCDF)"),
             (f"{sz['bronze']} / {sz['silver']} GB", "bronze / silver (Parquet)"),
             (f"{sz['gold']} GB", "gold (Parquet)")]
    s.append(section("volume", "1. Quy mô dữ liệu",
                     "ERA5 single levels tải từ Copernicus CDS, khung 23,5°N–8,5°N × 102°E–110°E, lọc còn các ô đất liền.",
                     '<div class="tiles">' + "".join(f'<div class="tile"><div class="v">{v}</div><div class="k">{k}</div></div>'
                                                     for v, k in tiles) + "</div>"))

    nulls = silver["nulls_after_rules"]
    qrows = pd.DataFrame([
        ("Số ô × năm", n(silver["cell_years"])), ("Ô × năm thiếu giờ", n(silver["cell_years_incomplete"])),
        ("Khóa (ô, giờ) trùng", n(silver["duplicate_cell_hours"])),
        ("Giá trị ngoài khoảng vật lý → null", ", ".join(f"{k}: {v}" for k, v in nulls.items() if v) or "0"),
        ("Dòng bronze → silver", f"{n(silver['bronze_rows'])} → {n(silver['silver_rows'])} "
                                 "(bỏ 7 giờ UTC của 01/01/2026 giờ VN)")], columns=["k", "v"])
    s.append(section("quality", "2. Chất lượng silver",
                     "Mỗi ô có đủ 8 760 giờ (năm thường) hoặc 8 784 giờ (năm nhuận); giá trị sai không xóa dòng mà đặt null.",
                     table(qrows, [("Kiểm tra", lambda r: r.k), ("Kết quả", lambda r: r.v)])))

    dry = q[q["quantile"] == "share_hours_below_0.1mm"].hourly_mm.iloc[0]
    q = q[q["quantile"] != "share_hours_below_0.1mm"]
    label = lambda v: "Lớn nhất" if v == "max" else f"p{float(v) * 100:g}"  # noqa: E731
    s.append(section("precip", "3. Phân bố lượng mưa",
                     f"Mưa giờ tại ô và tổng mưa 24 giờ tới (biến tạo nhãn). {pct(dry, 1)} số giờ mưa dưới 0,1 mm; "
                     "chỉ khoảng 1% số giờ có tổng 24 giờ tới vượt 50 mm: dữ liệu lệch rất mạnh.",
                     table(q, [("Phân vị", lambda r: label(r["quantile"])),
                               ("Mưa giờ (mm)", lambda r: n(r.hourly_mm, 2)),
                               ("Mưa 24 giờ tới (mm)", lambda r: n(r.next_24h_mm, 1))])))

    split = split.set_index("split").loc[["train", "validation", "test"]].reset_index()
    s.append(section("labels", "4. Nhãn mưa lớn theo split",
                     "Nhãn = 1 khi tổng mưa (t, t+24h] > 50 mm (QĐ 18/2021/QĐ-TTg). Lớp dương chỉ ~1%: dùng trọng số lớp và PR-AUC.",
                     table(split, [("Split", lambda r: SPLIT_VI[r.split]), ("Số dòng", lambda r: n(r.rows)),
                                   ("Số giờ nhãn 1", lambda r: n(r.positives)), ("Tỷ lệ", lambda r: pct(r.positive_rate)),
                                   ("Tỷ lệ 0 : 1", lambda r: f"{(1 - r.positive_rate) / r.positive_rate:,.0f} : 1")])))

    s.append(section("season", "5. Mùa mưa lớn theo vùng",
                     "Đỉnh mưa lớn dịch dần theo vĩ độ: Bắc Bộ tháng 8, Bắc Trung Bộ tháng 9–10, Nam Trung Bộ tháng 10–11; "
                     "Nam Bộ và Tây Nguyên thấp hơn hẳn. Tháng trong năm (month_sin/cos) là feature cần thiết.",
                     img("fig_label_region_month.png")))

    top = prov.head(12)
    mx = top.positive_rate.max()
    s.append(section("map", "6. Phân bố không gian",
                     "Tỷ lệ giờ có nhãn mưa lớn theo ô và theo tỉnh mới (34 tỉnh). Dải ven biển miền Trung từ Hà Tĩnh "
                     "đến Quảng Ngãi cao nhất; Nam Bộ thấp nhất. Mô hình dự báo theo ô nên chỉ ra được vùng nào trong tỉnh.",
                     '<div class="two"><div>' + img("fig_label_map.png") + "</div><div>" +
                     table(top, [("Tỉnh mới", lambda r: html.escape(r.new_province)),
                                 ("Tỷ lệ", lambda r: pct(r.positive_rate)),
                                 ("", lambda r: f'<span class="bar" style="width:{80 * r.positive_rate / mx:.0f}px"></span>')])
                     + "</div></div>"))

    s.append(section("years", "7. Theo năm",
                     "Tỷ lệ mưa lớn dao động mạnh giữa các năm (La Niña 2020–2022, bão Yagi 2024): split theo thời gian là cần thiết.",
                     table(year, [("Năm", lambda r: str(int(r.year))), ("Số dòng có nhãn", lambda r: n(r.rows)),
                                  ("Số giờ nhãn 1", lambda r: n(r.positives)), ("Tỷ lệ", lambda r: pct(r.positive_rate))])))

    s.append(section("corr", "8. Tương quan feature với nhãn (train)",
                     "Hệ số tương quan Pearson giữa từng feature và nhãn 0/1 trên train, sắp theo trị tuyệt đối. "
                     '<span class="tag M1">M1</span> khí tượng bề mặt, <span class="tag M4">M4</span> khí quyển (6 biến mới).',
                     table(corr.head(18), [("Feature", lambda r: f'{r.feature} <span class="tag {r.group}">{r.group}</span>'),
                                           ("Tương quan", lambda r: f"{r.corr_with_label:+.3f}"),
                                           ("Tỷ lệ null", lambda r: pct(r.null_rate))])))

    cc = vol.get("crosscheck_openmeteo")
    if cc:
        cct = pd.DataFrame([
            ("Số điểm so sánh", n(cc["points"])),
            ("Tương quan mưa ngày (trung vị 63 điểm)", f"{cc['median_daily_precip_corr']:.3f}"),
            ("Tương quan mưa giờ (trung vị)", f"{cc['median_hourly_precip_corr']:.3f}"),
            ("Chênh nhiệt độ CDS − Open-Meteo (trung vị)", f"{cc['median_temp_bias_c']:+.2f} °C"),
            ("Tương quan mưa giờ khi lệch −1 / 0 / +1 giờ",
             f"{cc['corr_lag_-1h']:.3f} / <b>{cc['corr_lag_+0h']:.3f}</b> / {cc['corr_lag_+1h']:.3f}")], columns=["k", "v"])
        s.append(section("crosscheck", "9. Đối chiếu với Open-Meteo (pilot)",
                         "Cùng là ERA5 nên hai nguồn phải gần như trùng; tương quan cao nhất ở độ lệch 0 giờ xác nhận đổi UTC → giờ VN đúng.",
                         table(cct, [("Chỉ số", lambda r: r.k), ("Giá trị", lambda r: r.v)])))

    return ('<!doctype html><html lang="vi"><head><meta charset="utf-8"><title>EDA ERA5 CDS</title>'
            f"<style>{CSS}</style></head><body><main><h1>EDA – ERA5 CDS, ô đất liền Việt Nam</h1>"
            '<p class="sub">BDA501 · dự báo mưa lớn 24 giờ tới · dữ liệu 2015–2025 · silver và gold</p>'
            + "".join(s) + "</main></body></html>")


def screenshot(page_path, out_dir):
    from playwright.sync_api import sync_playwright  # chỉ có trên máy (venv), không có trong container Spark
    out_dir.mkdir(exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1180, "height": 900}, device_scale_factor=2)
        page.goto(page_path.as_uri())
        page.screenshot(path=str(out_dir / "00_full_page.png"), full_page=True)
        for i, el in enumerate(page.query_selector_all("section"), start=1):
            el.screenshot(path=str(out_dir / f"{i:02d}_{el.get_attribute('id')}.png"))
        browser.close()
    return sorted(x.name for x in out_dir.glob("*.png"))


def main():
    page = EDA / "eda_report.html"
    page.write_text(build_html(), encoding="utf-8")
    print(page)
    print("\n".join(screenshot(page, EDA / "screens")))


if __name__ == "__main__":
    main()
