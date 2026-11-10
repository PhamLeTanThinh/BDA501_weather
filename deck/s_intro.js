// Section 1 — problem & data (each function is called once per language).
const path = require("path");
const K = require("./common");
const { pres, C, D, HEX, W, H, MX, t, f, pct, n, slide, card, bullets, table, source, pill, iconCircle, arrow, CT } = K;

const SEC = () => t("Problem & data", "Bài toán & dữ liệu");

module.exports = [
// 1 — title
(IC) => {
  const S = slide("TITLE_HERO", SEC(), t("Will it rain heavily in the next 24 hours?", "24 giờ tới có mưa lớn không?"));
  const INK = "3D4A52", PANEL = "EAF3F4";
  // bìa tối giản: tiêu đề bên trái, bản đồ tỷ lệ mưa lớn theo ô (dữ liệu thật) trên dải nền nhạt bên phải
  S.addShape("rect", { x: 8.75, y: 0, w: W - 8.75, h: H, fill: { color: PANEL }, line: { color: PANEL } });
  S.addImage({ path: path.join(K.ROOT, "reports", "official", "fig_hero_map.png"), x: 9.15, y: 0.45, w: 6.6 * 926 / 1908, h: 6.6,
    altText: "Vietnam: share of heavy-rain hours per ERA5 cell" });
  ["C9DEE0", "6FAAB0", "F2A65A", "C2501A"].forEach((c, i) =>
    S.addShape("rect", { x: 11.07 + i * 0.32, y: 6.55, w: 0.32, h: 0.1, fill: { color: c }, line: { color: c } }));
  S.addText(t("heavy-rain share per cell, low → high", "tỷ lệ mưa lớn theo ô, thấp → cao"),
    { x: 9.4, y: 6.7, w: 2.95, h: 0.25, fontSize: 9.5, color: INK, align: "right", margin: 0, isTextBox: true });
  S.addNotes(t("Open with the question a provincial disaster officer asks every hour. Every number in this deck comes from a run we executed; design-only parts are labelled.",
    "Mở đầu bằng câu hỏi mà cán bộ phòng chống thiên tai cần trả lời mỗi giờ. Mọi con số trong bộ slide đều từ lần chạy thật; phần chỉ là thiết kế được ghi rõ."));
},

// 2 — problem & definition (redesigned)
(IC) => {
  const S = slide("CONTENT", SEC(), t("What counts as heavy rain, and what exactly we predict", "Thế nào là mưa lớn, và mô hình dự báo chính xác điều gì"));
  S.addImage({ path: path.join(K.ROOT, "reports", "figures", "qd18_2021_heavy_rain_definition.png"), x: MX, y: 1.3, w: 6.3, h: 2.61,
    altText: "Decision 18/2021/QD-TTg, Article 5, clause 17" });
  S.addText(t("Decision 18/2021/QĐ-TTg, Art. 5, cl. 17: heavy rain = more than 50 mm in 24 hours",
    "Quyết định 18/2021/QĐ-TTg, Điều 5, khoản 17: mưa lớn = tổng lượng mưa trên 50 mm trong 24 giờ"),
    { x: MX, y: 4.0, w: 6.3, h: 0.5, fontSize: 14, bold: true, color: C.text2, margin: 0, isTextBox: true });
  [[t("Moderate", "Mưa vừa"), "16–50 mm", "accent5"], [t("Heavy", "Mưa to"), ">50–100 mm", "accent2"], [t("Very heavy", "Mưa rất to"), ">100 mm", "accent6"]]
    .forEach(([a, v, c], i) => S.addText([{ text: a, options: { bold: true, breakLine: true, fontSize: 14 } }, { text: v, options: { fontSize: 12 } }],
      { shape: "roundRect", rectRadius: 0.1, x: MX + i * 2.12, y: 4.6, w: 2.0, h: 0.75, fill: { color: C[c] }, color: C.background1,
        align: "center", valign: "middle", margin: 0, isTextBox: true }));
  S.addText(t("▲ our label: above 50 mm", "▲ nhãn của nhóm: trên 50 mm"), { x: MX + 2.12, y: 5.4, w: 4.2, h: 0.3, fontSize: 12,
    color: C.accent2, bold: true, margin: 0, isTextBox: true });

  // right: the question as a timeline
  const x0 = 7.3, w0 = W - MX - x0;
  S.addShape("roundRect", { x: x0, y: 1.3, w: w0, h: 4.95, rectRadius: 0.08, fill: { color: C.background2 }, line: { color: C.background2 } });
  S.addText(t("The question the model answers", "Câu hỏi mô hình trả lời"), { x: x0 + 0.2, y: 1.42, w: w0 - 0.4, h: 0.4, fontSize: 17,
    bold: true, color: C.text2, margin: 0, isTextBox: true });
  const ty = 2.05, tx = x0 + 0.25, tw = w0 - 0.5, past = tw * 0.62;
  S.addText(t("last 72 h of weather", "thời tiết 72 giờ qua"), { shape: "rect", x: tx, y: ty, w: past, h: 0.5, fill: { color: C.accent1 },
    color: C.background1, fontSize: 13, bold: true, align: "center", valign: "middle", margin: 0, isTextBox: true });
  S.addText(t("next 24 h", "24 giờ tới"), { shape: "rect", x: tx + past, y: ty, w: tw - past, h: 0.5, fill: { color: C.accent2 },
    color: C.background1, fontSize: 13, bold: true, align: "center", valign: "middle", margin: 0, isTextBox: true });
  S.addShape("line", { x: tx + past, y: ty - 0.12, w: 0, h: 0.74, line: { color: C.text1, width: 2 } });
  S.addText(t("now (t)", "bây giờ (t)"), { x: tx + past - 0.6, y: ty + 0.56, w: 1.2, h: 0.28, fontSize: 11, bold: true, align: "center",
    color: C.text1, margin: 0, isTextBox: true });
  const io = [[t("Input", "Đầu vào"), t("Rain, humidity, pressure trend, wind, cloud, temperature in this cell — only up to now",
    "Mưa, độ ẩm, xu hướng áp suất, gió, mây, nhiệt độ tại ô này — chỉ tính đến bây giờ"), "accent1"],
    [t("Output", "Đầu ra"), t("Probability that the next 24 h total exceeds 50 mm → alert yes / no",
      "Xác suất tổng mưa 24 giờ tới vượt 50 mm → cảnh báo có / không"), "accent2"]];
  io.forEach(([k, v, c], i) => {
    const y = 3.05 + i * 0.85;
    S.addText(k, { x: tx, y, w: 1.05, h: 0.7, fontSize: 14, bold: true, color: C[c], margin: 0, valign: "top", isTextBox: true });
    S.addText(v, { x: tx + 1.1, y, w: tw - 1.1, h: 0.75, fontSize: 13, color: C.text1, margin: 0, valign: "top", isTextBox: true });
  });
  const ex = (D.samples.gold_rows || []).find((r) => r.label === 1);
  if (ex) {
    // ví dụ thật từ gold: ba ô thông tin (nơi & giờ · lượng mưa 24 h tới · nhãn) thay cho một câu dài
    const date = `${ex.ts.slice(0, 5)}/2024`, hour = ex.ts.slice(6);
    S.addText(t("EXAMPLE", "VÍ DỤ"), { x: tx, y: 4.62, w: 2, h: 0.25, fontSize: 10, bold: true, charSpacing: 1.5,
      color: C.accent4, margin: 0, isTextBox: true });
    S.addShape("roundRect", { x: tx, y: 4.9, w: tw, h: 0.85, rectRadius: 0.06, fill: { color: C.background1 }, line: { color: C.background1 } });
    const cols = [[t("Hai Phong", "Hải Phòng"), `${date}, ${hour}`, C.text2],
      [`${f(ex.rain_next_24h, 1)} mm`, t("rain in the next 24 h", "mưa trong 24 giờ tới"), C.accent2],
      [t("label = 1", "nhãn = 1"), t("heavy rain", "mưa lớn"), C.text2]];
    const cw = tw / 3;
    cols.forEach(([big, small, color], i) => {
      S.addText([{ text: big, options: { fontSize: 16, bold: true, color, breakLine: true } },
        { text: small, options: { fontSize: 10.5, color: C.accent4 } }],
      { x: tx + i * cw + 0.15, y: 4.97, w: cw - 0.2, h: 0.72, margin: 0, valign: "middle", isTextBox: true });
      if (i < 2) S.addText("→", { x: tx + (i + 1) * cw - 0.2, y: 4.97, w: 0.3, h: 0.72, fontSize: 16, color: C.accent4,
        align: "center", valign: "middle", margin: 0, isTextBox: true });
    });
  }
  S.addText(t("Every hour · 493 cells across 34 provinces · not storms, floods or exact mm",
    "Cập nhật mỗi giờ · 493 ô trên 34 tỉnh · không dự báo bão, lũ hay số mm"),
    { x: tx, y: 5.85, w: tw, h: 0.35, fontSize: 11.5, italic: true, color: C.accent4, margin: 0, isTextBox: true });
  source(S, C, t("Source: Decision 18/2021/QĐ-TTg, original .doc published by vupc.mae.gov.vn (Department of Dyke Management and Disaster Prevention). The 16–50 mm band is the forecasting convention.",
    "Nguồn: Quyết định 18/2021/QĐ-TTg, bản .doc gốc trên vupc.mae.gov.vn (Cục Quản lý đê điều và PCTT). Mức 16–50 mm là quy ước dự báo."));
  S.addNotes(t("The threshold is a legal definition, not our choice. The model looks back up to 72 hours and answers one yes/no question about the next 24 hours.",
    "Ngưỡng 50 mm là định nghĩa pháp lý, không phải do nhóm chọn. Mô hình nhìn lại tối đa 72 giờ và trả lời một câu có/không cho 24 giờ tới."));
},

// 3 — users & success (redesigned)
(IC) => {
  const S = slide("CONTENT", SEC(), t("Who uses the forecast", "Ai dùng kết quả dự báo"));
  // ảnh thật của từng nhóm người dùng (Wikimedia Commons, CC BY-SA; xem deck/img_users/CREDITS.md)
  const img = (f) => path.join(__dirname, "img_users", f);
  [[img("user_disaster.jpg"), t("Provincial disaster committee", "Ban chỉ huy PCTT tỉnh"),
    t("“Which provinces are most at risk in the next 24 h?”", "“Tỉnh nào rủi ro cao nhất trong 24 giờ tới?”"),
    t("Gets: provinces ranked by risk, and which part of each province, refreshed every hour", "Nhận: tỉnh xếp theo rủi ro, kèm phần nào của tỉnh, cập nhật mỗi giờ")],
   [img("user_agriculture.jpg"), t("Agriculture", "Nông nghiệp"), t("“Should we harvest before tomorrow?”", "“Có nên thu hoạch trước ngày mai?”"),
    t("Gets: heavy-rain probability for their area + the usual rate for this month", "Nhận: xác suất mưa lớn ở khu vực + mức thường gặp của tháng")],
   [img("user_transport.jpg"), t("Transport & logistics", "Giao thông, vận tải"), t("“Will the route through province X get heavy rain?”",
    "“Tuyến qua tỉnh X có gặp mưa lớn không?”"), t("Gets: hourly probability for each ~26 km cell along the route", "Nhận: xác suất theo giờ cho từng ô ~26 km dọc tuyến")]]
    .forEach(([photo, who, q, gets], i) => {
      const x = MX + i * 4.1, w = 3.9;
      S.addShape("rect", { x, y: 1.4, w, h: 4.85, fill: { color: C.background2 }, line: { color: C.background2 } });
      S.addImage({ path: photo, x, y: 1.4, w, h: 2.2, altText: who });
      S.addText(who, { x: x + 0.25, y: 3.8, w: w - 0.5, h: 0.45, fontSize: 18, bold: true, color: C.text2, valign: "top", margin: 0, isTextBox: true });
      S.addText(q, { x: x + 0.25, y: 4.3, w: w - 0.5, h: 0.85, fontSize: 14, italic: true, color: C.text1, margin: 0, valign: "top", isTextBox: true });
      S.addText(gets, { x: x + 0.25, y: 5.2, w: w - 0.5, h: 0.95, fontSize: 13, color: C.accent1, margin: 0, valign: "top", isTextBox: true });
    });
  source(S, C, t("Photos: Wikimedia Commons — Nguyenngocthanhtoan (CC BY-SA 4.0), Adam Jones (CC BY-SA 2.0), Ilya Plekhanov (CC BY-SA 4.0)",
    "Ảnh: Wikimedia Commons — Nguyenngocthanhtoan (CC BY-SA 4.0), Adam Jones (CC BY-SA 2.0), Ilya Plekhanov (CC BY-SA 4.0)"));
  S.addNotes(t("Three users, one question each; every answer comes from the same hourly forecast per ERA5 cell.",
    "Ba nhóm người dùng, mỗi nhóm một câu hỏi; mọi câu trả lời đều lấy từ cùng một dự báo theo giờ cho từng ô ERA5."));
},

// 4 — why big data (redesigned)
(IC) => {
  // số đo bản chính thức (ERA5 CDS, 493 ô) — xem src/experiments/deck_official.py
  const O = JSON.parse(require("fs").readFileSync(path.join(K.ROOT, "reports", "official", "deck_official.json"), "utf8"));
  const V = O.volume, total = V.raw_gb + V.size_gb.bronze + V.size_gb.silver + V.size_gb.gold;
  const S = slide("CONTENT", SEC(), t("Why this needs a scalable data architecture", "Vì sao cần kiến trúc dữ liệu có khả năng mở rộng"));
  const tiles = [
    [IC.db, "accent1", "Volume", t(`${(V.silver_rows / 1e6).toFixed(1)} M rows`, `${(V.silver_rows / 1e6).toFixed(1).replace(".", ",")} triệu dòng`),
      t(`${O.cells.n} cells × 96,432 hours (2015–2025) × 13 ERA5 variables; ${total.toFixed(1)} GB across raw, bronze, silver and gold`,
        `${O.cells.n} ô × 96.432 giờ (2015–2025) × 13 biến ERA5; ${total.toFixed(1).replace(".", ",")} GB qua các tầng raw, bronze, silver, gold`)],
    [IC.clock, "accent2", "Velocity", t("every hour", "mỗi giờ"),
      t(`ERA5 adds a new hour for all ${O.cells.n} cells; the forecast refreshes hourly. Demo: 1 h of history replayed every 5 s`,
        `ERA5 thêm một giờ mới cho cả ${O.cells.n} ô; dự báo làm mới mỗi giờ. Demo: phát lại 1 giờ dữ liệu mỗi 5 giây`)],
    [IC.layers, "accent5", "Variety", t("5 formats", "5 định dạng"),
      t("NetCDF from CDS · GeoJSON province borders · CSV (rain gauges, cell map) · Parquet lake · MongoDB documents",
        "NetCDF từ CDS · GeoJSON ranh giới tỉnh · CSV (trạm đo mưa, bảng ô) · Parquet · document MongoDB")],
    [IC.search, "accent6", "Veracity", pct(D.noaa.hit / D.noaa.heavy_days),
      t("of heavy-rain days at NOAA rain gauges also exceed 50 mm in ERA5 — a cell average smooths local storms, so the data must be checked",
        "số ngày mưa lớn tại trạm NOAA mà ERA5 cũng vượt 50 mm — giá trị trung bình cả ô làm mượt mưa cục bộ, nên dữ liệu phải được kiểm chứng")],
  ];
  tiles.forEach(([ic, col, k, big, txt], i) => {
    const x = MX + (i % 2) * 6.1, y = 1.3 + Math.floor(i / 2) * 2.25;
    S.addShape("roundRect", { x, y, w: 5.95, h: 2.05, rectRadius: 0.08, fill: { color: C.background2 }, line: { color: C.background2 } });
    iconCircle(S, C, ic, x + 0.25, y + 0.25, 0.85, C[col]);
    S.addText(k, { x: x + 1.3, y: y + 0.2, w: 2.0, h: 0.35, fontSize: 14, bold: true, color: C[col], margin: 0, isTextBox: true });
    S.addText(big, { x: x + 1.3, y: y + 0.52, w: 4.5, h: 0.6, fontSize: 28, bold: true, color: C.text2, margin: 0, isTextBox: true });
    S.addText(txt, { x: x + 1.3, y: y + 1.15, w: 4.5, h: 0.8, fontSize: 12.5, color: C.text1, margin: 0, valign: "top", isTextBox: true });
  });
  S.addText([{ text: t("Why distributed: ", "Vì sao cần xử lý phân tán: "), options: { bold: true } },
    { text: t("72-hour rolling windows per cell over 47.5 M rows shuffle data across the cluster. Spark partitions by cell, so more cells or years means more tasks — not new code.",
      "cửa sổ trượt 72 giờ theo từng ô trên 47,5 triệu dòng phải shuffle dữ liệu. Spark chia partition theo ô, nên thêm ô hoặc năm chỉ là thêm task, không phải viết lại code.") }],
    { x: MX, y: 5.85, w: W - 2 * MX, h: 0.7, fontSize: 13, color: C.text1, margin: 0, isTextBox: true });
  S.addNotes(t("Size alone is not the argument; the shuffle-heavy windows and joins are.", "Kích thước không phải lý do chính; lý do là các cửa sổ thời gian và join cần shuffle."));
},

// 5 — experiment design: bốn bộ feature, một mô hình
(IC) => {
  const S = slide("CONTENT", SEC(), t("Experiment design: four feature sets, one model", "Thiết kế thí nghiệm: bốn bộ feature, cùng một mô hình"));
  const lx = MX + 0.75;
  // mỗi bộ = M1 + đúng một nhóm feature, nên mỗi phép so với M1 trả lời một câu hỏi
  const blocks = [
    ["M1", [[t("Local weather (24)", "Thời tiết tại chỗ (24)"), t("rain 3/24/72 h · humidity · pressure trend · wind · cloud · temperature · season",
      "mưa 3/24/72 h · độ ẩm · xu hướng áp suất · gió · mây · nhiệt độ · mùa"), 4.2, "accent1"]]],
    ["M2", [["M1", "", 4.2, "accent4"], [t("+ Terrain (3)", "+ Địa hình (3)"), t("elevation · coast · slope", "độ cao · cách biển · độ dốc"), 2.1, "accent5"]]],
    ["M3", [["M1", "", 4.2, "accent4"], [t("+ Terrain", "+ Địa hình"), "", 2.1, "accent4"],
      [t("+ Neighbours (4)", "+ Ô lân cận (4)"), t("rain · pressure · humidity around", "mưa · áp suất · độ ẩm xung quanh"), 2.1, "accent2"]]],
    ["M4", [["M1", "", 4.2, "accent4"], [t("+ Atmosphere (9)", "+ Khí quyển (9)"), t("water vapour · moisture flow and convergence · CAPE · K-index",
      "hơi nước · dòng ẩm và hội tụ ẩm · CAPE · chỉ số K"), 4.2, "accent3"]]],
  ];
  blocks.forEach(([name, parts], i) => {
    const y = 1.3 + i * 0.82;
    S.addText(name, { x: MX, y, w: 0.65, h: 0.72, fontSize: 20, bold: true, color: C.text2, valign: "middle", margin: 0, isTextBox: true });
    let x = lx;
    parts.forEach(([title, sub, w, col]) => {
      S.addText(sub ? [{ text: title, options: { bold: true, fontSize: 12.5, breakLine: true } }, { text: sub, options: { fontSize: 10 } }]
        : [{ text: title, options: { bold: true, fontSize: 12 } }],
        { shape: "rect", x, y, w: w - 0.04, h: 0.72, fill: { color: C[col] }, color: C.background1, align: "left", valign: "middle",
          margin: 0.08, isTextBox: true });
      x += w;
    });
  });
  // câu hỏi của từng bộ, so với M1; ghi rõ dữ liệu dùng để trả lời
  [[t("H1 · Does terrain help?", "H1 · Địa hình có giúp?"), t("M2 vs M1", "M2 so với M1"), "accent5"],
   [t("H2 · Do surrounding cells help?", "H2 · Ô xung quanh có giúp?"), t("M3 vs M1", "M3 so với M1"), "accent2"],
   [t("H3 · Does the atmosphere help?", "H3 · Khí quyển có giúp?"), t("M4 vs M1", "M4 so với M1"), "accent3"]]
    .forEach(([h, how, col], i) => {
      const w = (W - 2 * MX - 0.4) / 3, x = MX + i * (w + 0.2);
      S.addShape("rect", { x, y: 4.65, w, h: 0.95, fill: { color: C.background2 }, line: { color: C.background2 } });
      S.addText([{ text: h, options: { bold: true, color: C[col], fontSize: 14.5, breakLine: true } }, { text: how, options: { fontSize: 12 } }],
        { x: x + 0.15, y: 4.68, w: w - 0.3, h: 0.9, color: C.text1, margin: 0, valign: "middle", isTextBox: true });
    });
  // bản đồ ô thật quanh Hà Nội (minh họa M3)
  S.addImage({ path: require("path").join(K.ROOT, "reports", "figures", `neighbor_map_${K.ctx.L}.png`), x: 9.85, y: 1.3, w: 2.88, h: 3.08,
    altText: t("Map of Ha Noi with the ERA5 centre cell and four neighbour cells", "Bản đồ Hà Nội với ô ERA5 trung tâm và 4 ô lân cận") });
  const mm = D.ablation.manifest.model;
  S.addText([{ text: t("All four are trained the same way: ", "Cả bốn được train giống hệt nhau: "), options: { bold: true } },
    { text: t(`same rows of the official CDS data (483 land cells with neighbours, 2015–2025), Spark MLlib RandomForestClassifier (depth ${mm.maxDepth}, seed 42, positive class weighted N0/N1), train 2015–2022, threshold chosen on 2023, scored on 2024–2025.`,
      `cùng các dòng của dữ liệu CDS chính thức (483 ô đất liền có ô lân cận, 2015–2025), Spark MLlib RandomForestClassifier (độ sâu ${mm.maxDepth}, seed 42, lớp dương có trọng số N0/N1), train 2015–2022, chọn ngưỡng trên 2023, chấm trên 2024–2025.`), options: { breakLine: true } },
    { text: t("Rule fixed before seeing results: ", "Quy tắc chốt trước khi xem kết quả: "), options: { bold: true } },
    { text: t("keep M1 unless adding a group raises test PR-AUC with a 95% confidence interval entirely above zero.",
      "giữ M1, trừ khi thêm một nhóm feature làm PR-AUC tăng với khoảng tin cậy 95% nằm hẳn trên 0.") }],
    { x: MX, y: 5.8, w: W - 2 * MX, h: 0.9, fontSize: 12.5, color: C.text1, margin: 0, isTextBox: true, paraSpaceAfter: 4 });
  S.addNotes(t("Four sets, one model, one rule, all on the official data. The pilot asked H1 and H2 first on 12 points; the official run repeats them at full scale and adds H3.",
    "Bốn bộ feature, một mô hình, một quy tắc, đều trên dữ liệu chính thức. Pilot đã hỏi H1, H2 trên 12 điểm; bản chính thức hỏi lại ở quy mô đầy đủ và thêm H3."));
},

// 5b — what data we pull
(IC) => {
  const O = JSON.parse(require("fs").readFileSync(path.join(K.ROOT, "reports", "official", "deck_official.json"), "utf8"));
  const S = slide("CONTENT", SEC(), t("What data we pull: 13 ERA5 variables from Copernicus CDS", "Dữ liệu kéo về: 13 biến ERA5 từ Copernicus CDS"));
  const g = (txt, col) => ({ text: txt, options: { bold: true, color: C.background1, fill: { color: C[col] } } });
  const S1 = t("Surface weather (7)", "Thời tiết bề mặt (7)"), A1 = t("Atmosphere (6)", "Khí quyển (6)");
  table(S, C, [[t("Group", "Nhóm"), t("CDS variable", "Biến CDS"), t("Silver column · unit", "Cột silver · đơn vị"), t("What it measures", "Đo cái gì"), t("Used for", "Dùng cho")],
    [g(S1, "accent1"), "tp", "precipitation · mm", t("rain fallen in the hour before the timestamp", "lượng mưa trong 1 giờ trước mốc giờ"), t("label + M1", "nhãn + M1")],
    [g(S1, "accent1"), "t2m", "temperature_2m · °C", t("air temperature 2 m above ground", "nhiệt độ không khí ở độ cao 2 m"), "M1"],
    [g(S1, "accent1"), "d2m", "dew_point_2m · °C, relative_humidity_2m · %", t("dew point; humidity from T and Td (Magnus)", "điểm sương; độ ẩm tính từ T và Td (Magnus)"), "M1"],
    [g(S1, "accent1"), "msl", "pressure_msl · hPa", t("air pressure reduced to sea level", "áp suất quy về mực nước biển"), "M1"],
    [g(S1, "accent1"), "tcc", "cloud_cover · %", t("share of the sky covered by cloud", "phần trăm bầu trời bị mây che"), "M1"],
    [g(S1, "accent1"), "u10, v10", t("wind_speed_10m · km/h, direction · °", "wind_speed_10m · km/h, hướng · °"), t("wind 10 m above ground; direction it blows from", "gió ở 10 m; hướng gió thổi tới"), "M1"],
    [g(A1, "accent2"), "tcwv", "tcwv · mm", t("water vapour in the whole air column", "hơi nước trong cả cột khí quyển"), "M4"],
    [g(A1, "accent2"), "viwve, viwvn", "ivt_east, ivt_north · kg m⁻¹ s⁻¹", t("moisture carried east / north", "dòng ẩm vận chuyển về phía đông / bắc"), "M4"],
    [g(A1, "accent2"), "vimd", t("vimd · kg m⁻² per hour", "vimd · kg m⁻² mỗi giờ"), t("moisture divergence (negative = converging)", "phân kỳ ẩm (âm = ẩm đang hội tụ)"), "M4"],
    [g(A1, "accent2"), "cape", "cape · J/kg", t("energy available for storms", "năng lượng đối lưu cho dông"), "M4"],
    [g(A1, "accent2"), "kx", "k_index · K", t("thunderstorm potential", "khả năng xảy ra dông"), "M4"],
  ], { x: MX, y: 1.3, w: W - 2 * MX, colW: [2.2, 1.45, 3.6, 3.95, 0.93], fontSize: 11.5 });
  source(S, C, t(`Official pull: ${O.cells.n} land cells × 96,432 hours (2015–2025) × 13 variables = ${(O.volume.silver_rows / 1e6).toFixed(1)} M cell-hours. Terrain and neighbour-cell features were tested in the pilot and dropped.`,
    `Bản chính thức: ${O.cells.n} ô đất liền × 96.432 giờ (2015–2025) × 13 biến = ${(O.volume.silver_rows / 1e6).toFixed(1).replace(".", ",")} triệu ô-giờ. Feature địa hình và ô lân cận đã thử ở pilot và bị bỏ.`));
  S.addNotes(t("Seven surface variables feed the 24 M1 features; six column variables feed the M4 experiment.",
    "Bảy biến bề mặt tạo 24 feature M1; sáu biến cột khí quyển dùng cho thí nghiệm M4."));
},

// 6 — why ERA5 (redesigned)
(IC) => {
  const S = slide("CONTENT", SEC(), t("Why ERA5: comparing the candidate data sources", "Vì sao chọn ERA5: so sánh các nguồn dữ liệu"));
  const Y = { text: "✓", options: { color: C.accent3, bold: true, align: "center", fontSize: 15 } };
  const N = { text: "✗", options: { color: C.accent6, bold: true, align: "center", fontSize: 15 } };
  const P = (s) => ({ text: s, options: { color: C.accent2, align: "center" } });
  const hl = (s) => ({ text: s, options: { bold: true, fill: { color: "DFF0E4" } } });
  table(S, C, [
    [t("Source", "Nguồn"), t("What it is", "Loại"), t("Resolution", "Độ phân giải"), t("Hourly", "Theo giờ"), t("All predictors*", "Đủ biến*"),
      "2015–2025", t("Free", "Miễn phí"), t("Decision", "Quyết định")],
    [hl("ERA5"), hl(t("Reanalysis (ECMWF)", "Tái phân tích (ECMWF)")), hl(t("25 km · since 1940", "25 km · từ 1940")), Y, Y, Y, Y,
      { text: t("Chosen — main source", "Chọn — nguồn chính"), options: { bold: true, color: C.accent3, fill: { color: "DFF0E4" } } }],
    ["ERA5-Land", t("Land reanalysis", "Tái phân tích mặt đất"), t("9 km · since 1950", "9 km · từ 1950"), Y, P(t("no cloud, no MSL pressure", "thiếu mây, áp suất MSL")), Y, Y,
      t("Rejected: missing predictors", "Loại: thiếu biến")],
    ["MERRA-2 (NASA)", t("Reanalysis", "Tái phân tích"), t("~55 km · since 1980", "~55 km · từ 1980"), Y, Y, Y, Y, t("Rejected: twice as coarse", "Loại: lưới thô gấp đôi")],
    ["GPM IMERG", t("Satellite rain", "Mưa vệ tinh"), t("10 km · 30 min · since 2000", "10 km · 30 phút · từ 2000"), Y, P(t("rain only", "chỉ có mưa")), Y, Y,
      t("Rejected: rain only", "Loại: chỉ có mưa")],
    ["CHIRPS", t("Satellite + gauge rain", "Mưa vệ tinh + trạm"), t("5 km · daily · since 1981", "5 km · theo ngày · từ 1981"), N, P(t("rain only", "chỉ có mưa")), Y, Y,
      t("Rejected: daily, rain only", "Loại: theo ngày, chỉ có mưa")],
    ["NOAA GHCN-Daily", t("Rain gauges", "Trạm đo mưa"), t("14 VN stations · daily", "14 trạm VN · theo ngày"), N, P(t("rain only", "chỉ có mưa")), P(t("gaps", "có khoảng trống")), Y,
      { text: t("Used to check ERA5", "Dùng để kiểm chứng ERA5"), options: { color: C.accent1, bold: true } }],
    [t("VNMHA stations", "Trạm KTTV quốc gia"), t("National gauge network", "Mạng trạm quốc gia"), t("dense · hourly", "dày · theo giờ"), Y, Y, Y,
      P(t("paid, on request", "trả phí, theo yêu cầu")), t("Not obtainable in 3 weeks", "Không kịp xin trong 3 tuần")],
  ], { x: MX, y: 1.3, w: W - 2 * MX, colW: [1.75, 1.95, 2.05, 0.8, 1.85, 1.05, 1.1, 1.58], fontSize: 12 });
  S.addText([{ text: t("* Predictors: ", "* Các biến: "), options: { bold: true } },
    { text: t("rain, temperature, humidity, dew point, sea-level pressure, cloud cover, wind speed and direction.",
      "lượng mưa, nhiệt độ, độ ẩm, điểm sương, áp suất mực nước biển, độ che phủ mây, tốc độ và hướng gió.") }],
    { x: MX, y: 4.95, w: W - 2 * MX, h: 0.35, fontSize: 12, color: C.text1, margin: 0, isTextBox: true });
  card(S, C, MX, 5.4, W - 2 * MX, 0.95, { body: [{ text: t("Why ERA5: ", "Vì sao ERA5: "), options: { bold: true } },
    { text: t("the only free source that is hourly, has every predictor (plus 6 atmosphere variables), covers all of Vietnam for 2015–2025 and beyond, and feeds both training and the streaming replay with one schema. Cost: a ~26 km cell smooths local extremes (next slide).",
      "nguồn miễn phí duy nhất vừa theo giờ, vừa đủ biến (thêm 6 biến khí quyển), phủ cả Việt Nam cho 2015–2025 và xa hơn, và dùng chung một schema cho cả train lẫn streaming. Cái giá: ô ~26 km làm mượt mưa cục bộ (slide sau).") }], bodySize: 13.5 });
  source(S, C, t("Specs: Copernicus CDS (ERA5, ERA5-Land), NASA GES DISC (MERRA-2, IMERG), CHG UCSB (CHIRPS), NOAA NCEI (GHCN-Daily), Circular 197/2016/TT-BTC (VNMHA fees).",
    "Thông số: Copernicus CDS (ERA5, ERA5-Land), NASA GES DISC (MERRA-2, IMERG), CHG UCSB (CHIRPS), NOAA NCEI (GHCN-Daily), Thông tư 197/2016/TT-BTC (phí KTTV)."));
  S.addNotes(t("Five criteria: hourly, all predictors, national coverage, long record, free. Only ERA5 meets all five.",
    "Năm tiêu chí: theo giờ, đủ biến, phủ toàn quốc, chuỗi dài, miễn phí. Chỉ ERA5 đạt cả năm."));
},

// 7 — E0: ERA5 vs NOAA
(IC) => {
  const NO = D.noaa;
  const S = slide("CONTENT", SEC(), t("E0 · Can we trust ERA5 rain? A check against 14 real rain gauges", "E0 · ERA5 có đáng tin không? Đối chiếu với 14 trạm đo mưa thật"));
  S.addText([{ text: t("Why this check: ", "Vì sao phải kiểm tra: "), options: { bold: true, color: C.accent2 } },
    { text: t("our heavy-rain labels are computed from ERA5, which is a model estimate, not a measurement. NOAA GHCN-Daily stores what real rain gauges recorded. Comparing the two on the same days (2015–2025) shows how far ERA5 can be trusted before we train on it. E0 = experiment 0, the data check that comes before every model experiment.",
      "nhãn mưa lớn được tính từ ERA5 — số liệu ước tính từ mô hình khí tượng, không phải số đo. NOAA GHCN-Daily lưu lượng mưa mà trạm đo thật ghi được. So hai nguồn trên cùng các ngày (2015–2025) cho biết có tin được ERA5 để làm nhãn hay không. E0 = thí nghiệm số 0, bước kiểm chứng dữ liệu trước mọi thí nghiệm mô hình.") }],
    { x: MX, y: 1.2, w: W - 2 * MX, h: 0.85, fontSize: 13, color: C.text1, fill: { color: C.background2 }, margin: 0.1, isTextBox: true });
  S.addChart(pres.ChartType.bar, [{ name: t("correlation r", "hệ số tương quan r"), labels: NO.stations.map((s) => s.name.replace(/ \(.*\)/, "")),
    values: NO.stations.map((s) => s.corr) }], Object.assign(CT(), { x: MX, y: 2.2, w: 6.4, h: 4.35, barDir: "bar", chartColors: [HEX.accent1],
    showValue: true, dataLabelPosition: "outEnd", dataLabelFormatCode: "0.00", valAxisMinVal: 0, valAxisMaxVal: 1, showLegend: false,
    showTitle: true, title: t("Daily rain, ERA5 cell vs gauge: correlation r per station", "Mưa ngày, ô ERA5 so với trạm: hệ số tương quan r theo trạm"),
    valAxisTitle: t("r (no unit): 0 = unrelated, 1 = identical", "r (không đơn vị): 0 = không liên quan, 1 = trùng khớp"), showValAxisTitle: true,
    catAxisOrientation: "maxMin" }));
  const rows = [
    [`${f(100 * NO.median_ratio, 0)}%`, t("Totals match", "Tổng lượng khớp"), t(`Over 11 years, ERA5's total rain is ${f(100 * NO.median_ratio, 0)}% of the gauge total (median of 14 stations).`,
      `Tổng mưa 11 năm của ERA5 bằng ${f(100 * NO.median_ratio, 0)}% tổng của trạm (trung vị 14 trạm).`), "accent3"],
    [`r = ${f(NO.median_corr_vn, 2)}`, t("Day-to-day match is moderate", "Khớp theo từng ngày ở mức vừa"), t(`Counting days in Vietnam time gives ${f(NO.median_corr_vn, 2)} vs ${f(NO.median_corr_utc, 2)} in UTC → our time-zone handling is right.`,
      `Tính ngày theo giờ Việt Nam được ${f(NO.median_corr_vn, 2)}, theo giờ UTC chỉ ${f(NO.median_corr_utc, 2)} → cách xử lý múi giờ là đúng.`), "accent1"],
    [pct(NO.hit / NO.heavy_days), t("Heavy days often missed", "Hay bỏ sót ngày mưa lớn"), t(`Gauges recorded > 50 mm on ${n(NO.heavy_days)} days; ERA5 also showed > 50 mm on ${n(NO.hit)} of them (${pct(NO.hit_pm1 / NO.heavy_days)} if one day early/late is allowed).`,
      `Trạm đo > 50 mm trong ${n(NO.heavy_days)} ngày; ERA5 cũng > 50 mm ở ${n(NO.hit)} ngày trong số đó (${pct(NO.hit_pm1 / NO.heavy_days)} nếu cho lệch 1 ngày).`), "accent2"],
  ];
  rows.forEach(([big, head, txt, col], i) => {
    const y = 2.25 + i * 1.12;
    S.addText(big, { x: 7.25, y, w: 1.75, h: 0.95, fontSize: 26, bold: true, color: C[col], margin: 0, valign: "middle", isTextBox: true });
    S.addText([{ text: head, options: { bold: true, fontSize: 14, breakLine: true } }, { text: txt, options: { fontSize: 12 } }],
      { x: 9.05, y, w: 3.68, h: 1.0, color: C.text1, margin: 0, valign: "middle", isTextBox: true });
  });
  S.addText([{ text: t("Verdict: ", "Kết luận: "), options: { bold: true } },
    { text: t("ERA5 gets seasons, totals and timing right but averages local downpours over a ~26 km cell. So our label means “heavy rain over a ~26 km area”, and results are read that way.",
      "ERA5 đúng về mùa, tổng lượng và thời điểm, nhưng dàn mưa cục bộ ra cả ô ~26 km. Vì vậy nhãn của nhóm nghĩa là “mưa lớn trên diện ~26 km”, và kết quả được hiểu theo nghĩa đó.") }],
    { x: 7.25, y: 5.6, w: 5.48, h: 0.95, fontSize: 12.5, color: C.text1, fill: { color: C.background2 }, margin: 0.1, isTextBox: true });
  source(S, C, t("ERA5 from the official CDS download (land cell nearest each gauge). Script: validation/validate_era5_noaa.py --source cds · output: validation/report_cds.md",
    "ERA5 từ bộ dữ liệu CDS chính thức (ô đất liền gần trạm nhất). Script: validation/validate_era5_noaa.py --source cds · kết quả: validation/report_cds.md"), 6.72);
  S.addNotes(t("Purpose first: the label comes from a model, so we check it against gauges. Totals and timing are right; peaks are not.",
    "Nói mục đích trước: nhãn đến từ mô hình, nên phải đối chiếu với trạm đo. Tổng lượng và thời điểm đúng; đỉnh mưa thì không."));
},

// 8 — E0 sample
(IC) => {
  const e0 = D.samples.e0_noaa_vs_era5;
  const S = slide("CONTENT", SEC(), t("E0 data sample: the wettest gauge days and what ERA5 showed", "Dữ liệu mẫu E0: những ngày trạm đo mưa nhiều nhất và ERA5 ghi được bao nhiêu"));
  table(S, C, [[t("Station", "Trạm"), t("Date", "Ngày"), t("Gauge rain (mm/day)", "Mưa tại trạm (mm/ngày)"), t("ERA5 rain (mm/day)", "Mưa ERA5 (mm/ngày)"),
    t("ERA5 also > 50 mm?", "ERA5 cũng > 50 mm?"), t("ERA5 as % of gauge", "ERA5 bằng % trạm")],
    ...e0.map((r) => [r.station, r.date, { text: f(r.gauge_mm, 1), options: { align: "right" } }, { text: f(r.era5_mm, 1), options: { align: "right" } },
      { text: r.era5_heavy ? t("yes — event kept", "có — giữ được sự kiện") : t("no — event missed", "không — bỏ sót"), options: { bold: true, color: r.era5_heavy ? C.accent3 : C.accent6 } },
      { text: pct(r.era5_mm / r.gauge_mm), options: { align: "right" } }])],
    { x: MX, y: 1.35, w: 7.9, colW: [1.7, 1.2, 1.25, 1.25, 1.6, 0.9], fontSize: 12.5 });
  const kept = e0.find((r) => r.era5_heavy), miss = e0.find((r) => !r.era5_heavy);
  const ex = [];
  if (kept) ex.push(t(`${kept.station}, ${kept.date}: the gauge measured ${f(kept.gauge_mm, 1)} mm, ERA5 only ${f(kept.era5_mm, 1)} mm — but still above 50 mm, so the label is still “heavy rain”.`,
    `${kept.station}, ${kept.date}: trạm đo ${f(kept.gauge_mm, 1)} mm, ERA5 chỉ ${f(kept.era5_mm, 1)} mm — nhưng vẫn trên 50 mm, nên nhãn vẫn là “mưa lớn”.`));
  if (miss) ex.push(t(`${miss.station}, ${miss.date}: the gauge measured ${f(miss.gauge_mm, 1)} mm, ERA5 ${f(miss.era5_mm, 1)} mm — a very local storm that the ~26 km cell averaged away.`,
    `${miss.station}, ${miss.date}: trạm đo ${f(miss.gauge_mm, 1)} mm, ERA5 ${f(miss.era5_mm, 1)} mm — trận mưa rất cục bộ, bị ô ~26 km làm mờ đi.`));
  ex.push(t("Takeaway: ERA5 usually keeps the yes/no event on very wet days but loses most of the amount — that is why we predict yes/no (> 50 mm), not millimetres.",
    "Rút ra: ngày mưa rất to, ERA5 thường vẫn giữ được sự kiện có/không nhưng mất phần lớn lượng mưa — vì vậy nhóm dự báo có/không (> 50 mm), không dự báo số mm."));
  card(S, C, 8.75, 1.35, 3.98, 4.95, { title: t("How to read it", "Cách đọc bảng"), titleSize: 15, body: bullets(ex, 13) });
  source(S, C, t("Gauge: NOAA GHCN-Daily PRCP (stored in tenths of mm, ÷ 10; flagged values removed). ERA5 (CDS): hourly rain summed over the same Vietnam day, 2015–2025.",
    "Trạm: NOAA GHCN-Daily PRCP (lưu theo 1/10 mm, đã chia 10; bỏ giá trị bị gắn cờ). ERA5 (CDS): cộng mưa từng giờ trong cùng ngày theo giờ Việt Nam, 2015–2025."));
  S.addNotes(t("Three wettest gauge days at two stations, straight from the downloaded files.", "Ba ngày mưa nhiều nhất tại hai trạm, lấy thẳng từ file tải về."));
},
// 9 — gauge vs ERA5 cell vs province
(IC) => {
  const OC = JSON.parse(require("fs").readFileSync(path.join(K.ROOT, "reports", "official", "deck_official.json"), "utf8")).cells;
  const S = slide("CONTENT", SEC(), t("A rain gauge, an ERA5 cell and a province: three very different sizes", "Trạm đo, ô ERA5 và tỉnh: ba kích thước rất khác nhau"));
  S.addImage({ path: require("path").join(K.ROOT, "reports", "figures", `gauge_vs_cell_${K.ctx.L}.png`), x: MX, y: 1.25, w: 4.85, h: 5.2,
    altText: t("Map of Ha Noi: the Ha Dong rain gauge inside one ERA5 cell of about 26 by 28 km", "Bản đồ Hà Nội: trạm Hà Đông nằm trong một ô ERA5 khoảng 26 × 28 km") });
  const x = 5.75, w = W - MX - x;
  card(S, C, x, 1.25, w, 2.15, { title: t("What is NOAA GHCN-Daily?", "NOAA GHCN-Daily là gì?"), titleSize: 14.5, body: bullets([
    t("Global Historical Climatology Network – Daily, run by the US National Oceanic and Atmospheric Administration (NOAA)",
      "Mạng dữ liệu khí hậu lịch sử toàn cầu theo ngày, do Cơ quan Khí quyển và Đại dương Hoa Kỳ (NOAA) quản lý"),
    t("Collects what real weather stations measured: rain is caught in a gauge (funnel ≈ 20 cm) and recorded once a day",
      "Tập hợp số liệu đo thật từ trạm khí tượng: mưa được hứng bằng ống đo (miệng ≈ 20 cm) và ghi một lần mỗi ngày"),
    t("Free (CC0); 14 Vietnamese stations still report. We use it only to check ERA5, not to train",
      "Miễn phí (CC0); 14 trạm ở Việt Nam còn số liệu. Nhóm chỉ dùng để kiểm chứng ERA5, không dùng để train"),
  ], 11.5) });
  table(S, C, [[t("Level", "Cấp"), t("Size", "Kích thước"), t("Rain value means", "Lượng mưa nghĩa là")],
    [t("Gauge (NOAA)", "Trạm đo (NOAA)"), t("one point", "một điểm"), t("rain at that exact spot", "mưa đúng tại chỗ đặt trạm")],
    [t("ERA5 cell = one model point", "Ô ERA5 = 1 điểm của mô hình"), "≈ 26 × 28 km ≈ 720 km²", t("average over the whole cell", "trung bình trên cả ô")],
    [t("New province (34)", "Tỉnh mới (34)"), t(`${OC.cells_per_new_province.min}–${OC.cells_per_new_province.max} cells`, `${OC.cells_per_new_province.min}–${OC.cells_per_new_province.max} ô`), t("each of its cells is forecast", "dự báo từng ô của tỉnh")]],
    { x, y: 3.5, w, colW: [2.15, 1.75, 2.98], fontSize: 11.5 });
  table(S, C, [[t("New province", "Tỉnh mới"), t("Area", "Diện tích"), t("ERA5 cells", "Số ô ERA5")],
    [t("Hanoi", "Hà Nội"), "~3,360 km²", `${OC.examples["Hà Nội"]}`], [t("Nghe An", "Nghệ An"), "~16,500 km²", `${OC.examples["Nghệ An"]}`],
    [t("Lam Dong (merged)", "Lâm Đồng (mới)"), "~24,200 km²", `${OC.examples["Lâm Đồng"]}`]],
    { x, y: 5.05, w: 3.6, colW: [1.55, 1.1, 0.95], fontSize: 11 });
  S.addText(t(`All ${OC.n} land cells are forecast, not one cell per province: heavy rain in the western mountains of Nghe An falls in its own cell, so an alert can say which part of the province is at risk.`,
    `Dự báo cả ${OC.n} ô đất liền, không chỉ một ô mỗi tỉnh: mưa lớn ở vùng núi phía tây Nghệ An nằm trong ô riêng của nó, nên cảnh báo chỉ ra được phần nào của tỉnh có nguy cơ.`),
    { x: x + 3.75, y: 5.05, w: w - 3.75, h: 1.45, fontSize: 11, color: C.text1, fill: { color: "FDF1E7" }, margin: 0.08, valign: "top", isTextBox: true });
  source(S, C, t("Areas are approximate; cell counts from data/reference/cells_land.csv. Cell size from 0.25° × 0.25° at 21° N. Map: OpenStreetMap; gauge VMM00048825 (Ha Dong).",
    "Diện tích là xấp xỉ; số ô lấy từ data/reference/cells_land.csv. Kích thước ô tính từ 0,25° × 0,25° ở vĩ độ 21° B. Bản đồ: OpenStreetMap; trạm VMM00048825 (Hà Đông)."), 6.72);
  S.addNotes(t("Why ERA5 and gauges disagree: one measures a 20 cm funnel, the other averages 720 km². This is why peaks shrink in ERA5.",
    "Vì sao ERA5 và trạm lệch nhau: một bên đo ống 20 cm, một bên lấy trung bình 720 km². Đó là lý do đỉnh mưa bị giảm trong ERA5."));
},
];
