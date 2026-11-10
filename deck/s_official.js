// Official run (ERA5 from Copernicus CDS, 493 land cells): one slide per pipeline step, numbers read from
// reports/official/deck_official.json (python -m src.experiments.deck_official).
const fs = require("fs");
const path = require("path");
const K = require("./common");
const { C, HEX, W, MX, t, n, pct, f, slide, card, bullets, table, source, badge, arrow } = K;
const SEC = () => t("Architecture & pipeline", "Kiến trúc & pipeline");
const R = (p) => path.join(K.ROOT, "reports", "official", p);
const O = JSON.parse(fs.readFileSync(R("deck_official.json"), "utf8"));
const gb = (x) => `${f(x, 1)} GB`;
const hd = (i) => (O.hdfs[i] ? `${O.hdfs[i].split(/\s+/)[0]} GiB` : "–");  // hdfs dfs -du -h dùng đơn vị nhị phân
const mil = (x) => t(`${f(x / 1e6, 1)} M`, `${f(x / 1e6, 1).replace(".", ",")} triệu`);

// Ô số lớn ở đầu slide: [giá trị, nhãn]
function stats(S, items, y = 1.25) {
  const w = (W - 2 * MX - 0.2 * (items.length - 1)) / items.length;
  items.forEach(([v, k], i) => {
    const x = MX + i * (w + 0.2);
    S.addShape("roundRect", { x, y, w, h: 1.0, rectRadius: 0.08, fill: { color: C.background2 }, line: { color: C.background2 } });
    S.addText(v, { x: x + 0.15, y: y + 0.08, w: w - 0.3, h: 0.5, fontSize: 22, bold: true, color: C.text2, margin: 0, isTextBox: true, fit: "shrink" });
    S.addText(k, { x: x + 0.15, y: y + 0.56, w: w - 0.3, h: 0.38, fontSize: 11, color: C.text1, margin: 0, isTextBox: true, fit: "shrink" });
  });
}

function steps(S, items, x, y, w, rowH = 0.92, size = 12.5) {
  items.forEach(([a, b], i) => {
    const yy = y + i * rowH;
    badge(S, C, x, yy + 0.03, i + 1, HEX.accent1);
    S.addText([{ text: a, options: { bold: true, fontSize: size + 1, breakLine: true } }, { text: b, options: { fontSize: size } }],
      { x: x + 0.42, y: yy, w: w - 0.42, h: rowH - 0.05, color: C.text1, margin: 0, valign: "top", isTextBox: true });
  });
}

// Khối chữ lớn: một con số/cụm từ lớn và một dòng giải thích ngắn, nhìn là hiểu.
function bigLine(S, x, y, w, big, caption, color, size = 40) {
  S.addText(big, { x, y, w, h: size / 60, fontSize: size, bold: true, color, margin: 0, valign: "bottom", isTextBox: true, fit: "shrink" });
  S.addText(caption, { x, y: y + size / 60 + 0.05, w, h: 0.4, fontSize: 14, color: C.text1, margin: 0, valign: "top", isTextBox: true });
}

// Sơ đồ luồng một hàng: kho đầu vào (trụ) → các bước xử lý (khung, đánh số) → kho đầu ra (trụ).
function flowRow(S, y, input, steps, output, hb = 1.45) {
  const sw = 1.55, pw = (W - 2 * MX - 2 * sw - 0.25 * (steps.length + 1)) / steps.length, gap = 0.25;
  const cyl = (x, [name, sub]) => S.addText([{ text: name, options: { bold: true, fontSize: 13, color: C.background1, breakLine: true } },
    { text: sub, options: { fontSize: 10, color: C.background1 } }],
    { shape: "can", x, y: y - 0.1, w: sw, h: hb + 0.2, fill: { color: C.accent1 }, line: { color: C.accent1 }, align: "center", valign: "bottom",
      margin: [0, 0.05, 0.12, 0.05], isTextBox: true });
  const xs = [MX];
  cyl(MX, input);
  steps.forEach(([head, sub], i) => {
    const x = MX + sw + gap + i * (pw + gap);
    xs.push(x);
    S.addShape("roundRect", { x, y, w: pw, h: hb, rectRadius: 0.1, fill: { color: C.background1 }, line: { color: C.text2, width: 1.25 } });
    S.addText([{ text: `${i + 1}. ${head}`, options: { bold: true, fontSize: 13, color: C.text2, breakLine: true } }, { text: sub, options: { fontSize: 10.5, color: C.text1 } }],
      { x: x + 0.1, y: y + 0.08, w: pw - 0.2, h: hb - 0.16, align: "center", valign: "middle", margin: 0, isTextBox: true });
  });
  const xo = W - MX - sw;
  xs.push(xo);
  cyl(xo, output);
  const widths = [sw, ...steps.map(() => pw), sw];
  for (let i = 0; i < xs.length - 1; i++) arrow(S, C, xs[i] + widths[i] + 0.03, y + hb / 2, xs[i + 1] - 0.03, y + hb / 2, { color: C.accent2, width: 1.75 });
  return { xo, sw };
}

const dl = O.download, cells = O.cells, sv = O.silver, gd = O.gold, vol = O.volume;

module.exports = {
// 1. kéo dữ liệu về
ingest: (IC) => {
  const S = slide("CONTENT", SEC(), t("Step 1 · Data pull: ERA5 from Copernicus CDS", "Bước 1 · Kéo dữ liệu: ERA5 từ Copernicus CDS"));
  stats(S, [[`${dl.zip_files}`, t("NetCDF files (zip), 2015–2025", "file NetCDF (zip), 2015–2025")],
    ["13", t("ERA5 variables (7 surface + 6 atmosphere)", "biến ERA5 (7 bề mặt + 6 khí quyển)")],
    ["61 × 33", t("grid points, 0.25° (≈ 26–28 km)", "điểm lưới 0,25° (≈ 26–28 km)")],
    [gb(dl.raw_gb), t("raw size on disk and on HDFS", "dung lượng raw trên đĩa và HDFS")],
    [`${dl.requests_total}`, t(`CDS requests on ${dl.accounts} team accounts`, `request CDS trên ${dl.accounts} tài khoản nhóm`)]]);
  // một ý duy nhất nên không đánh số
  S.addText([{ text: t("One request = whole Vietnam box × 2 months × 13 variables", "Mỗi request = cả khung Việt Nam × 2 tháng × 13 biến"),
    options: { bold: true, fontSize: 16, color: C.text2, breakLine: true } },
    { text: t("Box 23.5°N–8.5°N, 102°E–110°E, hourly, 2015–2025. A whole year is rejected by CDS as too large; 2 months is accepted.",
      "Khung 23,5°N–8,5°N, 102°E–110°E, theo giờ, 2015–2025. Cả năm bị CDS từ chối vì quá lớn; 2 tháng thì được nhận."), options: { fontSize: 14 } }],
  { x: MX, y: 2.5, w: 7.3, h: 1.3, color: C.text1, margin: 0, valign: "top", isTextBox: true });
  table(S, C, [[t("Variable", "Biến"), t("Meaning", "Ý nghĩa")],
    ["tp", t("rain in the past hour (m)", "mưa trong giờ qua (m)")],
    ["t2m, d2m", t("temperature, dew point (K)", "nhiệt độ, điểm sương (K)")],
    ["msl, tcc", t("sea-level pressure (Pa), cloud (0–1)", "áp suất mực biển (Pa), mây (0–1)")],
    ["u10, v10", t("wind components (m/s)", "thành phần gió (m/s)")],
    ["tcwv", t("water vapour in the column", "hơi nước cả cột khí quyển")],
    ["viwve, viwvn", t("moisture transport E / N", "dòng vận chuyển ẩm Đ / B")],
    ["vimd", t("moisture divergence (− = converging)", "phân kỳ ẩm (âm = hội tụ)")],
    ["cape, kx", t("instability: CAPE, K-index", "bất ổn định: CAPE, chỉ số K")],
  ], { x: 8.25, y: 2.5, w: W - MX - 8.25, colW: [1.35, W - MX - 8.25 - 1.35], fontSize: 11 });
  source(S, C, t("Code: src/ingest/cds_jobs.py (submit / download) · raw: D:/BDA501_data/raw/era5_cds → HDFS /rain-forecast/raw/era5_cds",
    "Code: src/ingest/cds_jobs.py (submit / download) · raw: D:/BDA501_data/raw/era5_cds → HDFS /rain-forecast/raw/era5_cds"));
},

// 2. xử lý tọa độ: ô đất liền → tỉnh
cells: (IC) => {
  const S = slide("CONTENT", SEC(), t(`Step 2 · From grid to provinces: ${cells.n} land cells, ${cells.new_provinces} provinces`,
    `Bước 2 · Từ lưới đến tỉnh: ${cells.n} ô đất liền, ${cells.new_provinces} tỉnh`));
  S.addImage({ path: R("fig_cells_region.png"), x: MX + 0.2, y: 1.2, w: 3.0, h: 5.5, altText: "map of land cells by region" });
  steps(S, [
    [t("Sample points inside each cell", "Lấy điểm mẫu trong mỗi ô"),
      t("From the cell centre (lat, lon), place a 5 × 5 grid of points every 0.05° inside the 0.25° cell: 25 coordinates per cell.",
        "Từ tọa độ tâm ô (vĩ độ, kinh độ), đặt lưới 5 × 5 điểm cách nhau 0,05° bên trong ô 0,25°: 25 tọa độ cho mỗi ô.")],
    [t("Point-in-polygon against province borders", "So từng điểm với đa giác ranh giới tỉnh"),
      t(`Each point is tested against the 63 old-province polygons (geoBoundaries GeoJSON). A cell is kept if ≥ 25% of its points fall inside Vietnam → ${cells.n} of 2,013 cells.`,
        `Mỗi điểm được kiểm tra nằm trong đa giác tỉnh cũ nào (63 tỉnh, GeoJSON geoBoundaries). Ô được giữ nếu ≥ 25% điểm nằm trong Việt Nam → ${cells.n}/2.013 ô.`)],
    [t("Majority vote picks the old province", "Tỉnh cũ = tỉnh có nhiều điểm nhất"),
      t("Cell 21.25°N 105.50°E: 10 points in Ha Noi, 9 in Vinh Phuc, 6 in Phu Tho → Ha Noi. 247 cells straddle a border and are settled this way.",
        "Ô 21,25°N 105,50°E: 10 điểm thuộc Hà Nội, 9 Vĩnh Phúc, 6 Phú Thọ → Hà Nội. 247 ô nằm vắt qua ranh giới được gán theo cách này.")],
    [t(`Lookup old → new (${cells.old_provinces} → ${cells.new_provinces})`, `Tra bảng tỉnh cũ → tỉnh mới (${cells.old_provinces} → ${cells.new_provinces})`),
      t("The 2025 merger joined whole provinces, so a table is exact: Binh Dinh → Gia Lai, Binh Thuan → Lam Dong. Each cell keeps both names.",
        "Sáp nhập 2025 gộp nguyên tỉnh nên tra bảng là chính xác: Bình Định → Gia Lai, Bình Thuận → Lâm Đồng. Mỗi ô giữ cả hai tên.")],
  ], 4.35, 1.3, W - MX - 4.35, 1.15, 12.5);
  S.addText(t(`Forecasts stay per cell: a province has ${cells.cells_per_new_province.min}–${cells.cells_per_new_province.max} cells, so an alert reads e.g. “Gia Lai → cell 14.00°N 109.00°E (former Binh Dinh)”`,
    `Dự báo vẫn theo ô: một tỉnh có ${cells.cells_per_new_province.min}–${cells.cells_per_new_province.max} ô, nên cảnh báo có dạng “Gia Lai → ô 14,00°N 109,00°E (Bình Định cũ)”`),
    { x: 4.35, y: 6.15, w: W - MX - 4.35, h: 0.4, fontSize: 12, italic: true, color: C.accent1, fill: { color: C.background2 }, margin: 0.08, isTextBox: true });
  source(S, C, t("Code: src/ingest/build_cell_map.py · data/reference/cells_land.csv · borders: geoBoundaries ADM1 (public domain)",
    "Code: src/ingest/build_cell_map.py · data/reference/cells_land.csv · ranh giới: geoBoundaries ADM1 (public domain)"));
},

// 3. bronze
bronze: (IC) => {
  const S = slide("CONTENT", SEC(), t("Step 3 · Bronze: NetCDF → Parquet, one row per cell-hour", "Bước 3 · Bronze: NetCDF → Parquet, mỗi dòng = ô × giờ"));
  stats(S, [[mil(sv.bronze_rows), t("rows (493 cells × hours incl. 31/12/2014)", "dòng (493 ô × giờ, gồm 31/12/2014)")],
    [gb(vol.size_gb.bronze), t("Parquet (zstd), 68 files", "Parquet (zstd), 68 file")],
    ["~2 min", t("to convert all 68 raw files", "để chuyển cả 68 file raw")],
    [hd(1), t("on HDFS /rain-forecast/bronze", "trên HDFS /rain-forecast/bronze")]]);
  // luồng xử lý của bước bronze (không đưa code lên slide)
  const y = 2.75, hb = 1.45;
  const { xo, sw } = flowRow(S, y, ["raw/", t("68 NetCDF zips · 2,013 points × 13 vars", "68 zip NetCDF · 2.013 điểm × 13 biến")], [
    [t("Open", "Mở file"), t("2 NetCDF per zip: instant vars + hourly totals (rain, moisture divergence)", "2 NetCDF mỗi zip: biến tức thời + tổng theo giờ (mưa, phân kỳ ẩm)")],
    [t("Keep land", "Giữ đất liền"), t(`pick the ${cells.n} land cells from cells_land.csv`, `lấy ${cells.n} ô đất liền theo cells_land.csv`)],
    [t("Flatten", "Trải phẳng"), t("grid → one row per cell × hour; units and UTC time unchanged", "lưới → mỗi dòng = ô × giờ; giữ nguyên đơn vị và giờ UTC")],
    [t("Write", "Ghi"), t("one Parquet per raw file, folder per year; done files are skipped", "mỗi file raw → một Parquet, thư mục theo năm; file đã có thì bỏ qua")],
  ], ["bronze/", `${mil(sv.bronze_rows)} · ${gb(vol.size_gb.bronze)}`], hb);
  // bronze → HDFS
  arrow(S, C, xo + sw / 2, y + hb + 0.15, xo + sw / 2, y + hb + 0.75, { color: C.accent2, width: 1.75 });
  S.addText(t("copy to HDFS /rain-forecast/bronze", "chép lên HDFS /rain-forecast/bronze"),
    { x: xo - 0.85, y: y + hb + 0.8, w: sw + 0.85, h: 0.35, fontSize: 11, bold: true, color: C.accent2, align: "right", margin: 0, isTextBox: true });
  card(S, C, MX, 4.95, 8.6, 1.0, { body: bullets([
    t("Nothing is cleaned here: bronze keeps CDS values as sent (K, Pa, m, UTC), so silver can always be rebuilt from it.",
      "Không làm sạch ở bước này: bronze giữ nguyên giá trị CDS gửi (K, Pa, m, giờ UTC), để silver luôn dựng lại được."),
    t("Runs in Python because Spark cannot read NetCDF; every later step runs on Spark.",
      "Chạy bằng Python vì Spark không đọc được NetCDF; mọi bước sau chạy trên Spark."),
  ], 12.5) });
},

// 4. silver
silver: (IC) => {
  const S = slide("CONTENT", SEC(), t("Step 4 · Silver: clean values, common units, Vietnam time", "Bước 4 · Silver: làm sạch, đổi đơn vị, đổi sang giờ Việt Nam"));
  const nulls = Object.entries(sv.nulls_after_rules).filter(([, v]) => v).map(([k, v]) => `${k} ${v}`).join(", ");
  stats(S, [[mil(sv.silver_rows), t(`rows = ${cells.n} cells × 96,432 h`, `dòng = ${cells.n} ô × 96.432 giờ`)],
    [`${sv.cell_years_incomplete}`, t(`of ${n(sv.cell_years)} cell-years missing an hour`, `trên ${n(sv.cell_years)} ô-năm thiếu giờ`)],
    [`${sv.duplicate_cell_hours}`, t("duplicate (cell, hour) keys", "khóa (ô, giờ) trùng")],
    [gb(vol.size_gb.silver), t("Parquet by year (Spark)", "Parquet theo năm (Spark)")]]);
  // luồng xử lý của bước silver (Spark), không đưa code lên slide
  flowRow(S, 2.75, ["bronze/", t("CDS units · UTC", "đơn vị CDS · giờ UTC")], [
    [t("Convert units", "Đổi đơn vị"), t("rain m → mm, K → °C, Pa → hPa, cloud 0–1 → %", "mưa m → mm, K → °C, Pa → hPa, mây 0–1 → %")],
    [t("Derive", "Tính thêm"), t("humidity from T and dew point; wind speed and direction from u, v", "độ ẩm từ nhiệt độ và điểm sương; tốc độ, hướng gió từ u, v")],
    [t("Vietnam time", "Giờ Việt Nam"), t("UTC + 7 h; keep 01/01/2015 00:00 → 31/12/2025 23:00", "UTC + 7 giờ; giữ 01/01/2015 00:00 → 31/12/2025 23:00")],
    [t("Range check", "Kiểm tra khoảng"), t(`impossible values → empty, row kept (${nulls})`, `giá trị vô lý → để trống, giữ dòng (${nulls})`)],
  ], ["silver/", `${mil(sv.silver_rows)} · ${gb(vol.size_gb.silver)}`]);
  const cc = vol.crosscheck_openmeteo;
  card(S, C, MX, 4.75, W - 2 * MX, 1.1, { title: t("Cross-check with Open-Meteo (same ERA5)", "Đối chiếu với Open-Meteo (cùng ERA5)"), titleSize: 14, body: bullets([
    t(`Daily rain correlation ${f(cc.median_daily_precip_corr, 4)} at ${cc.points} points; hourly correlation is highest with no time shift → Vietnam time is right`,
      `Tương quan mưa ngày ${f(cc.median_daily_precip_corr, 4)} tại ${cc.points} điểm; tương quan mưa giờ cao nhất khi không lệch giờ → giờ Việt Nam đúng`),
  ], 12.5) });
},

// 5. gold
gold: (IC) => {
  const S = slide("CONTENT", SEC(), t("Step 5 · Gold: label + 33 features per cell-hour", "Bước 5 · Gold: nhãn + 33 feature cho mỗi ô × giờ"));
  const sp = O.split;
  stats(S, [[mil(gd.rows), t(`rows (${gd.locations} cells), 0 duplicate keys`, `dòng (${gd.locations} ô), 0 khóa trùng`)],
    [`${mil(sp.train.rows)} · ${pct(sp.train.rate, 2)}`, t("train 2015–2022 · heavy-rain share", "train 2015–2022 · tỷ lệ mưa lớn")],
    [`${mil(sp.validation.rows)} · ${pct(sp.validation.rate, 2)}`, t("validation 2023", "validation 2023")],
    [`${mil(sp.test.rows)} · ${pct(sp.test.rate, 2)}`, t("test 2024–2025", "test 2024–2025")],
    [gb(vol.size_gb.gold), t("Parquet, float32 features", "Parquet, feature float32")]]);
  // luồng xử lý của bước gold (Spark), không đưa code lên slide
  flowRow(S, 2.75, ["silver/", t(`${mil(sv.silver_rows)} cell-hours`, `${mil(sv.silver_rows)} ô-giờ`)], [
    [t("Attach the cell", "Gắn thông tin ô"), t("province (old + new), region, coordinates", "tỉnh cũ + mới, vùng, tọa độ")],
    [t("M1 · surface (24)", "M1 · bề mặt (24)"), t("rain, humidity, pressure, wind, temperature, cloud over the last 3–72 h; season, hour", "mưa, độ ẩm, áp suất, gió, nhiệt, mây trong 3–72 h qua; mùa, giờ")],
    [t("M4 · atmosphere (9)", "M4 · khí quyển (9)"), t("water vapour, moisture flow and convergence, CAPE, K-index", "hơi nước, dòng ẩm và hội tụ ẩm, CAPE, chỉ số K")],
    [t("Label + split", "Nhãn + split"), t("rain in the next 24 h > 50 mm → 1; train / validation / test by year", "mưa 24 h tới > 50 mm → 1; chia train / validation / test theo năm")],
  ], ["gold/", t(`33 features · ${gb(vol.size_gb.gold)}`, `33 feature · ${gb(vol.size_gb.gold)}`)]);
  S.addText([{ text: t("Past only: ", "Chỉ nhìn quá khứ: "), options: { bold: true, color: C.accent1 } },
    { text: t("features use hours up to t, the label uses the 24 hours after t; a missing hour leaves the value empty, never 0 mm.",
      "feature chỉ dùng các giờ ≤ t, nhãn dùng 24 giờ sau t; giờ bị thiếu thì để trống, không coi là 0 mm.") }],
    { x: MX, y: 4.85, w: W - 2 * MX, h: 0.5, fontSize: 14, color: C.text1, margin: 0, isTextBox: true });
},

// 6. EDA: hiếm và theo mùa
edaSeason: (IC) => {
  const S = slide("CONTENT", SEC(), t("EDA · Heavy rain is rare and its season moves south with time", "EDA · Mưa lớn hiếm và mùa mưa dịch dần về phía nam"));
  S.addImage({ path: R("eda/fig_label_region_month.png"), x: MX, y: 1.25, w: 8.4, h: 8.4 * 840 / 1600, altText: "heavy rain share by region and month" });
  const p = Object.fromEntries(O.precip.map((r) => [String(r.quantile), r]));
  card(S, C, 9.25, 1.25, W - MX - 9.25, 2.35, { title: t("Rare", "Hiếm"), titleSize: 15, body: bullets([
    t(`${pct(p["share_hours_below_0.1mm"].hourly_mm, 0)} of hours have < 0.1 mm of rain`, `${pct(p["share_hours_below_0.1mm"].hourly_mm, 0)} số giờ mưa < 0,1 mm`),
    t(`99th percentile of next-24 h rain: ${f(p["0.99"].next_24h_mm, 0)} mm, just below the 50 mm threshold`,
      `Phân vị 99 của mưa 24 h tới: ${f(p["0.99"].next_24h_mm, 0)} mm, sát ngưỡng 50 mm`),
    t("“Always no” is ~99% right → judge with PR-AUC, weight the rare class", "Đoán “luôn không” đúng ~99% → đánh giá bằng PR-AUC, tăng trọng số lớp hiếm"),
  ], 11.5) });
  card(S, C, 9.25, 3.75, W - MX - 9.25, 2.6, { title: t("Seasonal", "Theo mùa"), titleSize: 15, body: bullets([
    t("North peaks in Aug; North Central in Sep–Oct; South Central in Oct–Nov", "Bắc Bộ đỉnh tháng 8; Bắc Trung Bộ tháng 9–10; Nam Trung Bộ tháng 10–11"),
    t("South and Highlands stay low", "Nam Bộ và Tây Nguyên thấp"),
    t("→ month sin/cos are needed; results are reported by region and season", "→ cần month sin/cos; báo cáo kết quả theo vùng và mùa"),
  ], 11.5) });
  source(S, C, t("All 493 cells, 2015–2025. Notebook: notebooks/eda_official.ipynb · report: reports/official/eda/eda_report.html",
    "Cả 493 ô, 2015–2025. Notebook: notebooks/eda_official.ipynb · báo cáo: reports/official/eda/eda_report.html"));
},

// 7. EDA: không gian và năm
edaSpace: (IC) => {
  const S = slide("CONTENT", SEC(), t("EDA · Where heavy rain happens, and how much it varies by year", "EDA · Mưa lớn xảy ra ở đâu và thay đổi theo năm ra sao"));
  S.addImage({ path: R("eda/fig_label_map.png"), x: MX, y: 1.2, w: 3.4, h: 3.4 * 1600 / 1040, altText: "heavy rain share per cell" });
  const sp = O.split, all = ["train", "validation", "test"].reduce((a, k) => [a[0] + sp[k].positives, a[1] + sp[k].rows], [0, 0]);
  const nat = all[0] / all[1], top = O.top_provinces, x = 4.75, w = W - MX - x;
  bigLine(S, x, 1.35, w, `${f(top[0].positive_rate / nat, 0)}×`, t(`${top[0].new_province}: ${pct(top[0].positive_rate, 1)} of hours lead to heavy rain, vs ${pct(nat, 1)} nationwide`,
    `${top[0].new_province}: ${pct(top[0].positive_rate, 1)} số giờ dẫn tới mưa lớn, so với ${pct(nat, 1)} cả nước`), C.accent2, 54);
  bigLine(S, x, 3.0, w, top.slice(0, 4).map((r) => r.new_province).join(" · "), t("the four wettest provinces all lie on the central coast",
    "bốn tỉnh mưa lớn nhiều nhất đều ở dải ven biển miền Trung"), C.text2, 30);
  bigLine(S, x, 4.35, w, `${pct(sp.validation.rate, 2)} → ${pct(sp.test.rate, 2)}`, t("validation 2023 had few heavy-rain hours; test 2024–2025 had many (Typhoon Yagi)",
    "validation 2023 ít giờ mưa lớn; test 2024–2025 nhiều (bão Yagi)"), C.accent1, 40);
  S.addText(t("→ forecast per cell, not per province · test on harder years than the threshold was tuned on",
    "→ dự báo theo ô, không theo tỉnh · test trên các năm khó hơn năm chọn ngưỡng"),
    { x, y: 5.85, w, h: 0.4, fontSize: 14, bold: true, color: C.text2, margin: 0, isTextBox: true });
},

// lưu trữ: số đo thật
storage: (IC) => {
  const S = slide("CONTENT", SEC(), t("Storage: four layers, where each lives", "Lưu trữ: bốn tầng, mỗi tầng nằm ở đâu"));
  // mỗi tầng là một trụ, chiều cao tỉ lệ dung lượng; dải màu bên dưới cho biết nằm ở đâu
  const L = [
    ["raw/", vol.raw_gb, t(`NetCDF zip · ${dl.zip_files} files`, `NetCDF zip · ${dl.zip_files} file`)],
    ["bronze/", vol.size_gb.bronze, t(`Parquet · ${mil(sv.bronze_rows)} rows`, `Parquet · ${mil(sv.bronze_rows)} dòng`)],
    ["silver/", vol.size_gb.silver, t(`Parquet · ${mil(sv.silver_rows)} rows`, `Parquet · ${mil(sv.silver_rows)} dòng`)],
    ["gold/", vol.size_gb.gold, t(`Parquet · 33 features`, `Parquet · 33 feature`)],
  ];
  const cw = 2.4, gap = (W - 2 * MX - 4 * cw) / 3, cx = (i) => MX + i * (cw + gap), base = 3.25, max = Math.max(...L.map((r) => r[1]));
  L.forEach(([name, size, sub], i) => {
    const h = 0.5 + 1.4 * size / max, x = cx(i) + 0.4, w = cw - 0.8;
    S.addShape("can", { x, y: base - h, w, h, fill: { color: i < 2 ? C.accent1 : C.text2 }, line: { color: i < 2 ? C.accent1 : C.text2 } });
    S.addText([{ text: name, options: { bold: true, fontSize: 18, color: C.text2 } }, { text: `  ${gb(size)}`, options: { bold: true, fontSize: 18, color: C.accent2 } }],
      { x: cx(i), y: base + 0.1, w: cw, h: 0.4, align: "center", margin: 0, isTextBox: true });
    S.addText(sub, { x: cx(i), y: base + 0.5, w: cw, h: 0.3, fontSize: 12, color: C.text1, align: "center", margin: 0, isTextBox: true });
    if (i < 3) arrow(S, C, cx(i) + cw - 0.3, base - 0.3, cx(i + 1) + 0.3, base - 0.3, { color: C.accent2, width: 1.75 });
  });
  // nơi lưu: raw + bronze có cả trên HDFS; silver + gold trên lake cục bộ
  const band = (i0, i1, color, text) => S.addText(text, { shape: "roundRect", rectRadius: 0.08, x: cx(i0), y: 4.12, w: cx(i1) + cw - cx(i0), h: 0.45,
    fill: { color }, line: { color }, fontSize: 12.5, bold: true, color: C.background1, align: "center", valign: "middle", margin: 0 });
  band(0, 1, C.accent1, t(`HDFS /rain-forecast/  +  local disk  ·  ${hd(0)} + ${hd(1)} on HDFS`, `HDFS /rain-forecast/  +  ổ đĩa  ·  ${hd(0)} + ${hd(1)} trên HDFS`));
  band(2, 3, C.text2, t("Lake D:/BDA501_data/lake → /data/lake in Spark", "Lake D:/BDA501_data/lake → /data/lake trong Spark"));
  // hình cây thư mục: cách chia thư mục theo năm
  S.addShape("roundRect", { x: MX, y: 4.85, w: 4.6, h: 1.6, rectRadius: 0.08, fill: { color: C.background2 }, line: { color: C.background2 } });
  S.addText([
    { text: "gold/", options: { bold: true, color: C.text2, breakLine: true } },
    { text: "├─ year=2015/  part-…parquet", options: { color: C.text1, breakLine: true } },
    { text: "├─ …", options: { color: C.text1, breakLine: true } },
    { text: t("├─ year=2023/   ← validation", "├─ year=2023/   ← validation"), options: { color: C.accent1, breakLine: true } },
    { text: t("└─ year=2024, 2025/   ← test", "└─ year=2024, 2025/   ← test"), options: { color: C.accent2 } },
  ], { x: MX + 0.25, y: 4.95, w: 4.2, h: 1.4, fontFace: "Consolas", fontSize: 12.5, valign: "middle", margin: 0, isTextBox: true });
  // ba lý do, mỗi cái một dòng ngắn
  const total = vol.raw_gb + vol.size_gb.bronze + vol.size_gb.silver + vol.size_gb.gold;
  [[t("Folder per year", "Thư mục theo năm"), t("training on 2015–2022 never opens 2023–2025", "train 2015–2022 không mở thư mục 2023–2025")],
   [t("Parquet = columns", "Parquet = theo cột"), t("training reads only label + feature columns", "train chỉ đọc cột nhãn + feature")],
   [t("Keep raw + bronze", "Giữ raw + bronze"), t("silver and gold can be rebuilt without downloading again", "dựng lại silver, gold không cần tải lại")],
   [gb(total), t(`all layers · ×3 replicas on a real cluster = ${gb(total * 3)}`, `cả bốn tầng · ×3 bản sao trên cụm thật = ${gb(total * 3)}`)],
  ].forEach(([head, sub], i) => {
    const y = 4.85 + i * 0.41;
    S.addText([{ text: head, options: { bold: true, color: i === 3 ? C.accent2 : C.text2 } }, { text: `   ${sub}`, options: { color: C.text1 } }],
      { x: MX + 4.9, y, w: W - 2 * MX - 4.9, h: 0.4, fontSize: 14, margin: 0, valign: "middle", isTextBox: true });
  });
},
};
