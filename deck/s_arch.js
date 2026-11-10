// Section 2 — architecture & pipeline.
const K = require("./common");
const { pres, C, D, HEX, W, H, MX, t, f, pct, n, slide, card, bullets, table, source, pill, iconCircle, arrow, badge, CT, SEL } = K;
const SEC = () => t("Architecture & pipeline", "Kiến trúc & pipeline");
const SR = (() => { const q = require("path").join(K.ROOT, "reports", "streaming", "stream_report.json");
  return require("fs").existsSync(q) ? JSON.parse(require("fs").readFileSync(q, "utf8")) : null; })();
const FINAL = (() => {
  const d = D.official_ablation && D.official_ablation.deltas.find((x) => x.comparison === "M4 - M1");
  return d && d.ci_lo > 0 ? "M4" : "M1";
})();
const code = (s, x, y, w, h, text, size = 11) => s.addText(text, { x, y, w, h, fontFace: "Courier New", fontSize: size, color: C.text1,
  fill: { color: "F3F6F7" }, margin: 0.1, valign: "top", isTextBox: true, line: { color: "D5E2E4", width: 0.5 } });

// Data-flow diagram primitives: rounded box = process, cylinder = data store, cloud = external, dashed = proposed.
function dfd(S) {
  const line = (proposed) => ({ color: proposed ? C.accent4 : C.text2, width: 1.25, dashType: proposed ? "dash" : "solid" });
  return {
    proc(x, y, w, title, tech, proposed = false) {
      S.addText([{ text: title, options: { bold: true, fontSize: 13, color: C.text2, breakLine: true } }, { text: tech, options: { fontSize: 10, color: C.accent1 } }],
        { shape: "roundRect", rectRadius: 0.12, x, y, w, h: 0.95, fill: { color: C.background1 }, line: line(proposed), align: "center", valign: "middle", margin: 0.05, isTextBox: true });
    },
    store(x, y, w, name, sub) {
      S.addText([{ text: name, options: { bold: true, fontSize: 12, color: C.background1, breakLine: true } }, { text: sub, options: { fontSize: 9.5, color: C.background1 } }],
        { shape: "can", x, y, w, h: 1.1, fill: { color: C.accent1 }, line: { color: C.accent1 }, align: "center", valign: "bottom", margin: [0, 0.04, 0.08, 0.04], isTextBox: true });
    },
    ext(x, y, w, h, name, sub) {
      S.addText([{ text: name, options: { bold: true, fontSize: 11.5, breakLine: true } }, { text: sub, options: { fontSize: 9.5 } }],
        { shape: "cloud", x, y, w, h, fill: { color: "EEF2F3" }, line: { color: C.accent4, width: 1 }, color: C.text1, align: "center", valign: "middle", margin: 0, isTextBox: true });
    },
    zone(x, y, w, h, label) {
      S.addShape("roundRect", { x, y, w, h, rectRadius: 0.05, fill: { color: "F4F8F9" }, line: { color: C.accent4, width: 1, dashType: "dash" } });
      S.addText(label, { x: x + 0.1, y: y + h - 0.3, w: w - 0.2, h: 0.26, fontSize: 9.5, italic: true, color: C.accent4, align: "right", margin: 0, isTextBox: true });
    },
    flow(x1, y1, x2, y2, label, o = {}) {
      const col = o.color || C.accent2;
      arrow(S, C, x1, y1, x2, y2, { color: col, width: 1.75, dash: o.dash });
      if (label) S.addText(label, { x: (x1 + x2) / 2 - 0.6 + (o.dx || 0), y: (y1 + y2) / 2 - 0.27 + (o.dy || 0), w: 1.2, h: 0.22, fontSize: 9, color: col, bold: true, align: "center", margin: 0, isTextBox: true });
    },
    layer(x, y, w, h, title) {
      S.addShape("roundRect", { x, y, w, h, rectRadius: 0.05, fill: { color: "F4F8F9" }, line: { color: C.accent4, width: 1, dashType: "dash" } });
      S.addText(title, { x, y: y + 0.05, w, h: 0.28, fontSize: 11.5, bold: true, color: C.text2, align: "center", margin: 0, isTextBox: true });
    },
    legend(y) {
      S.addShape("roundRect", { x: MX, y, w: 0.45, h: 0.3, rectRadius: 0.1, fill: { color: C.background1 }, line: { color: C.text2, width: 1 } });
      S.addText(t("job / process", "job xử lý"), { x: MX + 0.5, y, w: 1.6, h: 0.3, fontSize: 10.5, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
      S.addShape("can", { x: MX + 2.2, y: y - 0.05, w: 0.32, h: 0.4, fill: { color: C.accent1 }, line: { color: C.accent1 } });
      S.addText(t("data store", "nơi lưu dữ liệu"), { x: MX + 2.6, y, w: 1.6, h: 0.3, fontSize: 10.5, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
      S.addShape("cloud", { x: MX + 4.3, y: y - 0.03, w: 0.5, h: 0.36, fill: { color: "EEF2F3" }, line: { color: C.accent4, width: 1 } });
      S.addText(t("external source", "nguồn bên ngoài"), { x: MX + 4.9, y, w: 1.7, h: 0.3, fontSize: 10.5, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
      arrow(S, C, MX + 6.7, y + 0.15, MX + 7.3, y + 0.15, { color: C.accent2, width: 1.75 });
      S.addText(t("data flow", "luồng dữ liệu"), { x: MX + 7.4, y, w: 1.4, h: 0.3, fontSize: 10.5, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
      S.addShape("roundRect", { x: MX + 8.9, y, w: 0.45, h: 0.3, rectRadius: 0.1, fill: { color: C.background1 }, line: { color: C.accent4, width: 1, dashType: "dash" } });
      S.addText(t("design only", "chỉ là thiết kế"), { x: MX + 9.4, y, w: 1.6, h: 0.3, fontSize: 10.5, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
    },
  };
}

module.exports = [
// divider
(IC) => {
  const S = slide("SECTION_DARK", SEC(), t("Architecture & pipeline", "Kiến trúc & pipeline"));
  S.addText(t("From the API to a MongoDB document — what runs, where, and how we checked it",
    "Từ API đến document MongoDB — thành phần nào chạy, chạy ở đâu, kiểm chứng ra sao"), { placeholder: "body" });
},

// architecture overview — layered component diagram
(IC) => {
  const S = slide("CONTENT", SEC(), t("Architecture overview: components and data flows", "Kiến trúc tổng thể: các thành phần và luồng dữ liệu"));
  const df = dfd(S), Y = 1.2, HZ = 3.95;
  const B = { color: C.accent2 }, ST = { color: C.accent5 }, V = { color: C.accent4, dash: "dash" };
  df.layer(0.5, Y, 1.85, HZ, t("Sources", "Nguồn"));
  df.layer(2.45, Y, 1.95, HZ, t("Ingestion", "Thu thập"));
  df.layer(4.5, Y, 2.4, HZ, t("Storage", "Lưu trữ"));
  df.layer(7.0, Y, 3.3, HZ, t("Processing", "Xử lý"));
  df.layer(10.4, Y, 2.43, HZ, t("Serving", "Phục vụ"));
  // sources
  df.ext(0.6, 1.55, 1.65, 0.85, "Copernicus CDS", t("ERA5 · NetCDF, 13 vars", "ERA5 · NetCDF, 13 biến"));
  df.ext(0.6, 2.9, 1.65, 0.85, "NOAA", t("rain gauges", "trạm đo mưa"));
  // NOAA không vào pipeline: chỉ dùng để đối chiếu nhãn ERA5 (E0) → đường đứt, không mũi tên
  S.addShape("line", { x: 1.42, y: 2.4, w: 0.0001, h: 0.5, line: { color: C.accent4, width: 1.25, dashType: "dash" } });
  S.addText(t("label check", "đối chiếu nhãn"), { x: 1.5, y: 2.52, w: 0.9, h: 0.25, fontSize: 8.5, italic: true, color: C.accent4, margin: 0, isTextBox: true });
  df.proc(0.6, 4.0, 1.65, "Forecast API", t("live feed", "dữ liệu trực tiếp"), true);
  // ingestion
  df.proc(2.55, 1.6, 1.75, t("Download + bronze", "Tải + bronze"), "cds_jobs · cds_bronze");
  df.store(2.55, 2.75, 1.2, "cells_land.csv", t("493 cells → 34 prov.", "493 ô → 34 tỉnh"));
  df.proc(2.55, 4.0, 1.75, t("Replay producer", "Bộ phát lại"), t("silver → Kafka + junk", "silver → Kafka + rác"));
  // storage
  df.store(4.7, 1.5, 2.0, t("Data lake", "Data lake"), t("raw · bronze (HDFS) · silver · gold", "raw · bronze (HDFS) · silver · gold"));
  df.store(4.7, 2.7, 2.0, "models/", t(`${FINAL} + threshold`, `${FINAL} + ngưỡng`));
  df.store(4.7, 3.9, 2.0, "Kafka", t("topic · 3 partitions", "topic · 3 partition"));
  // processing
  df.proc(7.15, 1.6, 3.0, t("Spark batch", "Spark batch"), t("clean · EDA · features · Spark SQL", "làm sạch · EDA · feature · Spark SQL"));
  df.proc(7.15, 2.8, 1.45, "Spark ML", t("train RF M1–M4", "train RF M1–M4"));
  df.proc(8.7, 2.8, 1.45, "MapReduce", "YARN · HDFS");
  df.proc(7.15, 4.0, 3.0, "Structured Streaming", t("watermark · dedup · M4", "watermark · khử trùng · M4"));
  // serving
  df.store(10.55, 2.1, 2.1, "MongoDB", t("forecasts + batch stats", "dự báo + thống kê batch"));
  df.proc(10.55, 3.65, 2.1, t("Web dashboard", "Web dashboard"), t("FastAPI · map", "FastAPI · bản đồ"));
  // batch flows
  df.flow(2.25, 1.98, 2.55, 1.98, "", B);
  df.flow(3.15, 2.75, 3.15, 2.55, "", B);
  df.flow(4.3, 1.98, 4.7, 1.98, "Parquet", { ...B, dy: -0.22 });
  df.flow(6.7, 1.9, 7.15, 1.9, "", B);
  df.flow(7.15, 2.3, 6.7, 2.3, "", B);
  df.flow(10.15, 2.1, 10.55, 2.45, "daily_stats", { ...B, dx: -0.05, dy: -0.15 });
  // Spark ML và MapReduce đọc từ lake (gold / mr/ CSV trên HDFS), không nhận trực tiếp từ Spark batch
  df.flow(6.72, 2.5, 7.15, 3.1, "", B);                 // lake (gold) → Spark ML
  df.flow(6.72, 2.6, 9.4, 2.8, "", B);                  // lake (mr/ CSV trên HDFS) → MapReduce, đi qua khe giữa các khung
  S.addText("gold", { x: 6.7, y: 2.82, w: 0.3, h: 0.2, fontSize: 8.5, bold: true, color: C.accent2, margin: 0, isTextBox: true });
  S.addText("mr/ CSV", { x: 8.75, y: 2.55, w: 0.7, h: 0.2, fontSize: 9, bold: true, color: C.accent2, margin: 0, isTextBox: true });
  df.flow(7.15, 3.27, 6.7, 3.27, "", B);
  df.flow(10.15, 3.15, 10.55, 2.95, "climatology", { ...B, dy: 0.42 });
  // streaming flows
  df.flow(2.25, 4.47, 2.55, 4.47, "", { ...ST, dash: "dash" });
  df.flow(4.75, 2.45, 4.1, 4.0, "silver", { ...ST, dx: 0.1 });
  df.flow(4.3, 4.47, 4.7, 4.47, "", ST);
  df.flow(6.7, 4.47, 7.15, 4.47, "", ST);
  df.flow(6.7, 3.6, 7.45, 4.0, "model", { ...ST, dx: 0.25 });
  df.flow(10.15, 4.3, 10.6, 3.2, "", ST);
  df.flow(11.6, 3.2, 11.6, 3.65, "", ST);
  df.legend(5.4);
  S.addText([{ text: "→ ", options: { color: C.accent2, bold: true } }, { text: t("batch: daily ETL, monthly retrain   ", "batch: ETL hằng ngày, train lại hằng tháng   ") },
    { text: "→ ", options: { color: C.accent5, bold: true } }, { text: t("streaming: every hour   ", "streaming: mỗi giờ   ") },
    { text: "⇢ ", options: { color: C.accent4, bold: true } }, { text: t("cross-check only", "chỉ để đối chiếu") }],
    { x: MX, y: 5.85, w: W - 2 * MX, h: 0.3, fontSize: 11.5, color: C.text1, margin: 0, isTextBox: true });
  source(S, C, t("cells_land.csv: the 493 ERA5 cells over Vietnam's land with their old and new province — used to keep land cells and to roll forecasts up to 34 provinces.",
    "cells_land.csv: 493 ô ERA5 trên đất liền Việt Nam kèm tỉnh cũ và tỉnh mới — dùng để giữ ô đất liền và gộp dự báo lên 34 tỉnh."), 6.72);
  S.addNotes(t("Orange is batch, blue is streaming. MongoDB serves both: hourly forecasts from streaming, climatology from MapReduce and daily statistics from Spark SQL.",
    "Cam là batch, xanh là streaming. MongoDB phục vụ cả hai: dự báo theo giờ từ streaming, climatology từ MapReduce và thống kê ngày từ Spark SQL."));
},

// batch lane in detail — data-flow diagram
(IC) => {
  const st = D.storage, q = D.quality, mv = (D.mapreduce || {}).validation || {};
  const S = slide("CONTENT", SEC(), t("Batch lane in detail: data-flow diagram", "Chi tiết làn batch: sơ đồ luồng dữ liệu"));
  const df = dfd(S), sw = 1.45, sy = 3.0;
  const O = JSON.parse(require("fs").readFileSync(require("path").join(K.ROOT, "reports", "official", "deck_official.json"), "utf8"));
  const V = O.volume, mil = (x) => t(`${(x / 1e6).toFixed(1)} M rows`, `${(x / 1e6).toFixed(1).replace(".", ",")} tr dòng`);
  df.zone(2.6, 2.75, 10.15, 1.6, t("Data lake · raw/ and bronze/ also on HDFS", "Data lake · raw/ và bronze/ có cả trên HDFS"));
  df.ext(0.55, 1.35, 1.9, 1.0, "Copernicus CDS", t("ERA5 · NetCDF", "ERA5 · NetCDF"));
  df.store(0.7, 3.0, 1.65, "cells_land.csv", t("493 cells → 34 prov.", "493 ô → 34 tỉnh"));
  const px = [2.6, 4.65, 6.7, 8.75, 10.8], pw = 1.85, sx = (i) => px[i] + (pw - 1.45) / 2;
  df.proc(px[0], 1.35, pw, t("Download", "Tải về"), "cds_jobs.py · Python");
  df.proc(px[1], 1.35, pw, t("To Parquet", "Sang Parquet"), "cds_bronze.py · Python");
  df.proc(px[2], 1.35, pw, t("Clean", "Làm sạch"), "cds_silver.py · Spark");
  df.proc(px[3], 1.35, pw, t("Build features", "Tạo feature"), "cds_gold.py · Spark");
  df.proc(px[4], 1.35, pw, t("Train model", "Train model"), "RF · Spark ML");
  df.store(sx(0), sy, sw, "raw/", t(`NetCDF zip · ${V.raw_gb.toFixed(1)} GB`, `NetCDF zip · ${V.raw_gb.toFixed(1)} GB`));
  df.store(sx(1), sy, sw, "bronze/", mil(O.silver.bronze_rows));
  df.store(sx(2), sy, sw, "silver/", mil(O.silver.silver_rows));
  df.store(sx(3), sy, sw, "gold/", t("Parquet · 33 features", "Parquet · 33 feature"));
  df.store(sx(4), sy, sw, "models/", t(`Spark ML · ${FINAL}`, `Spark ML · ${FINAL}`));
  const mid = (i) => px[i] + pw / 2;
  df.flow(2.45, 1.85, px[0], 1.85, "NetCDF", { dy: -0.35 });
  df.flow(mid(0), 2.3, mid(0), sy);                      // tải về → raw
  df.flow(mid(0) + 0.35, sy, mid(1) - 0.35, 2.3);        // raw → sang Parquet
  df.flow(mid(1), 2.3, mid(1), sy);                      // → bronze
  df.flow(mid(1) + 0.35, sy, mid(2) - 0.35, 2.3);        // bronze → làm sạch
  df.flow(mid(2), 2.3, mid(2), sy);                      // → silver
  df.flow(mid(2) + 0.35, sy, mid(3) - 0.35, 2.3);        // silver → tạo feature
  df.flow(mid(3), 2.3, mid(3), sy);                      // → gold
  df.flow(mid(3) + 0.35, sy, mid(4) - 0.35, 2.3);        // gold → train
  df.flow(mid(4), 2.3, mid(4), sy);                      // → models
  df.flow(2.15, 3.05, px[1] + 0.15, 2.3, t("land cells", "ô đất liền"), { dx: -1.15, dy: 0.32 });
  // bên dưới lake: ai đọc tầng nào, ghi ra đâu
  const ry = 4.8;
  df.store(0.6, ry - 0.05, 1.4, "reports/", t("EDA + quality", "EDA + chất lượng"));
  df.proc(2.25, ry, 1.9, t("EDA + data checks", "EDA + kiểm tra dữ liệu"), t("Spark · stop if wrong", "Spark · sai thì dừng"));
  df.proc(4.55, ry, 1.9, t("Daily stats", "Thống kê ngày"), t("Spark SQL on gold", "Spark SQL trên gold"));
  df.store(6.85, ry - 0.05, 1.8, "MongoDB", "daily_stats · climatology");
  df.proc(9.05, ry, 1.8, "MapReduce", t(`YARN · ${mv.mismatches ?? "–"} diff vs Spark`, `YARN · lệch ${mv.mismatches ?? "–"} so với Spark`));
  df.store(11.25, ry - 0.05, 1.5, "mr/", t("CSV on HDFS", "CSV trên HDFS"));
  df.flow(mid(2) - 0.25, 4.1, 3.4, ry);     // silver → EDA
  df.flow(mid(3) - 0.35, 4.1, 3.85, ry);     // gold → EDA
  df.flow(mid(3) - 0.1, 4.1, 6.0, ry);                                         // gold → thống kê ngày
  df.flow(mid(3) + 0.3, 4.1, 11.75, ry);       // gold → mr/
  df.flow(2.25, ry + 0.47, 2.0, ry + 0.47);                                    // EDA → reports
  df.flow(6.45, ry + 0.47, 6.85, ry + 0.47);      // → MongoDB
  df.flow(11.25, ry + 0.47, 10.85, ry + 0.47);                                 // mr/ → MapReduce
  df.flow(9.05, ry + 0.47, 8.65, ry + 0.47);      // → MongoDB
  const lab = (txt, x, y) => S.addText(txt, { x, y, w: 0.55, h: 0.2, fontSize: 9, bold: true, color: C.accent2, align: "center", margin: 0, isTextBox: true });
  lab("silver", 6.32, 3.92); lab("gold", 8.37, 3.92); lab("CSV", 10.45, 4.5);
  // phục vụ: API + dashboard đọc MongoDB (thiết kế, viền đứt) — giống slide tổng quan
  S.addText([{ text: "API + dashboard", options: { bold: true, fontSize: 13, color: C.text2 } },
    { text: t("   reads MongoDB · design", "   đọc MongoDB · thiết kế"), options: { fontSize: 10, color: C.accent1 } }],
    { shape: "roundRect", rectRadius: 0.1, x: 6.3, y: 6.05, w: 2.9, h: 0.55, fill: { color: C.background1 },
      line: { color: C.accent4, width: 1.25, dashType: "dash" }, align: "center", valign: "middle", margin: 0 });
  df.flow(7.75, 5.85, 7.75, 6.05);
  S.addNotes(t("Below the lake, who reads which layer. EDA and data checks read silver (row counts, missing hours, units, time zone, cross-check with Open-Meteo) and gold (label rates, feature correlation); they write only reports, and the pipeline stops if a check fails. Users reach batch results through an API + dashboard that reads MongoDB (design, not built). silver/, gold/, models/ live on the local Spark volume; raw/, bronze/ and mr/ are also on the lab HDFS; models/ feeds the streaming lane. Daily stats: Spark SQL on gold sums hourly rain into daily rain and a heavy-day flag per cell, upserted into MongoDB daily_stats. MapReduce: gold is exported as CSV to mr/ on HDFS because Hadoop Streaming reads text lines; the job writes the climatology table, loaded into MongoDB and checked against Spark SQL with zero differences.",
    "Bên dưới lake: ai đọc tầng nào. EDA và kiểm tra dữ liệu đọc silver (số dòng, giờ thiếu, đơn vị, múi giờ, đối chiếu Open-Meteo) và gold (tỷ lệ nhãn, tương quan feature); chỉ ghi ra báo cáo, kiểm tra sai thì pipeline dừng. Người dùng xem kết quả batch qua API + dashboard đọc MongoDB (thiết kế, chưa dựng). silver/, gold/, models/ nằm trên volume Spark cục bộ; raw/, bronze/ và mr/ có cả trên HDFS của cụm lab; models/ cấp model cho làn streaming. Thống kê ngày: Spark SQL trên gold cộng mưa giờ thành mưa ngày và cờ ngày mưa lớn theo ô, upsert vào MongoDB daily_stats. MapReduce: gold được export thành CSV vào mr/ trên HDFS vì Hadoop Streaming đọc từng dòng chữ; job ghi ra bảng climatology, nạp vào MongoDB và đối chiếu với Spark SQL, không khác biệt."));
},

// streaming lane in detail — data-flow diagram
(IC) => {
  const S = slide("CONTENT", SEC(), t("Streaming lane in detail: data-flow diagram", "Chi tiết làn streaming: sơ đồ luồng dữ liệu"));
  const df = dfd(S);
  df.zone(4.35, 1.15, 5.35, 3.45, t("Spark Structured Streaming · micro-batch every 10 s", "Spark Structured Streaming · micro-batch mỗi 10 giây"));
  df.store(0.55, 1.4, 1.35, "silver/", t("Yagi 01–10/09/2024", "Yagi 01–10/09/2024"));
  df.proc(2.15, 1.45, 1.95, t("Producer", "Producer"), t("replay 1 h / 5 s + junk", "phát 1 giờ / 5 s + rác"));
  df.store(2.4, 3.15, 1.5, "Kafka", t("topic · 3 partitions", "topic · 3 partition"));
  df.proc(4.5, 1.55, 1.55, t("Parse + check", "Đọc + kiểm tra"), t("schema, range, cell", "schema, khoảng, ô"));
  df.proc(6.25, 1.55, 1.6, t("Watermark 6 h", "Watermark 6 giờ"), t("+ dedup (cell, hour)", "+ khử trùng (ô, giờ)"));
  df.proc(8.05, 1.55, 1.5, t("Features + M4", "Feature + M4"), t("same code as batch", "cùng code batch"));
  df.store(4.5, 3.25, 1.45, "checkpoint/", t("offsets + state", "offset + state"));
  df.store(6.3, 3.25, 1.5, "history/", t("last 120 h", "120 giờ gần nhất"));
  df.store(8.1, 3.25, 1.4, "models/", t("M4 from batch", "M4 từ batch"));
  df.store(10.05, 1.4, 1.6, "MongoDB", t("predictions · rejects", "dự báo · bị cách ly"));
  df.proc(10.0, 3.25, 1.75, t("Web dashboard", "Web dashboard"), t("FastAPI · map", "FastAPI · bản đồ"));
  df.flow(1.9, 1.95, 2.15, 1.95);
  df.flow(3.15, 2.4, 3.15, 3.15, "JSON", { dx: 0.45 });
  df.flow(3.9, 3.6, 4.5, 2.25);                       // Kafka → đọc
  df.flow(6.05, 2.02, 6.25, 2.02);
  df.flow(7.85, 2.02, 8.05, 2.02);
  df.flow(7.05, 2.5, 7.05, 3.25);                     // state → checkpoint (qua history)
  df.flow(8.4, 2.5, 7.75, 3.25);                      // ghi bộ đệm
  df.flow(7.3, 3.25, 8.2, 2.5);                       // đọc lại bộ đệm 120 giờ
  df.flow(8.8, 3.25, 8.8, 2.5);                       // nạp model
  df.flow(9.55, 1.95, 10.05, 1.95, "upsert");
  // cách ly: đi vòng phía trên các khung xử lý, không cắt ngang
  S.addShape("line", { x: 5.27, y: 1.27, w: 0, h: 0.28, line: { color: C.accent2, width: 1.75 } });
  S.addShape("line", { x: 5.27, y: 1.27, w: 5.53, h: 0, line: { color: C.accent2, width: 1.75 } });
  arrow(S, C, 10.8, 1.27, 10.8, 1.48, { color: C.accent2, width: 1.75 });
  S.addText(t("quarantine (broken records)", "cách ly (bản ghi hỏng)"), { x: 6.3, y: 1.31, w: 2.6, h: 0.22, fontSize: 9, bold: true, color: C.accent2, align: "center", margin: 0, isTextBox: true });
  df.flow(10.85, 2.5, 10.85, 3.25);
  const s = SR ? Object.fromEntries(SR.junk.map((r) => [r.case, r])) : null;
  const gu = [
    [t("Duplicates removed", "Bỏ bản trùng"), s ? t(`${n(s["duplicate (immediate + later)"].handled)} of ${n(s["duplicate (immediate + later)"].injected)} injected`, `${n(s["duplicate (immediate + later)"].handled)} / ${n(s["duplicate (immediate + later)"].injected)} đã chèn`) : ""],
    [t("Late data", "Dữ liệu trễ"), t("≤ 6 h: accepted, recomputed · > 6 h: dropped", "≤ 6 giờ: nhận, tính lại · > 6 giờ: bỏ")],
    [t("Broken records quarantined", "Bản ghi hỏng bị cách ly"), SR ? t(`${n(SR.rows.rejected_records)} records`, `${n(SR.rows.rejected_records)} bản ghi`) : ""],
    [t("Crash-safe", "Chịu được sự cố"), t("kill + restart: 0 lost, 0 duplicates", "tắt + chạy lại: 0 mất, 0 trùng")],
  ];
  const gw = (W - 2 * MX - 0.3) / 4;
  gu.forEach(([h, d], i) => {
    const x = MX + i * (gw + 0.1);
    iconCircle(S, C, IC.check, x, 5.0, 0.42, C.accent3);
    S.addText([{ text: h, options: { bold: true, fontSize: 12.5, breakLine: true } }, { text: d, options: { fontSize: 11.5 } }],
      { x: x + 0.5, y: 4.92, w: gw - 0.55, h: 0.75, color: C.text1, margin: 0, valign: "top", isTextBox: true });
  });
  df.legend(6.1);
  S.addNotes(t("Producer replays silver for 01–10/09/2024 (493 cells) into a Kafka topic, one hour of data every 5 seconds, and injects junk: duplicates, late and out-of-order messages, messages more than 6 hours late, broken JSON, missing fields, impossible values, unknown cells, a 20-hour burst and 5 cells with missing hours. Spark runs two queries on the topic: rejects (quarantine to MongoDB) and forecast (watermark 6 h + dropDuplicatesWithinWatermark on cell and hour, then foreachBatch: 120-hour history buffer on the lake, the same feature functions as batch, model M4 with the 2023 threshold, upsert to MongoDB by cell|hour). Offsets and dedup state live in the checkpoint, so after a crash the job continues where it stopped. In the demo the producer first replays 72 hours at full speed to warm up the features, then waits for the Start button on the web page.",
    "Producer phát lại silver 01–10/09/2024 (493 ô) vào một topic Kafka, mỗi giờ dữ liệu 5 giây, và chèn rác: bản trùng, bản tin trễ và sai thứ tự, bản tin trễ quá 6 giờ, JSON hỏng, thiếu trường, giá trị vô lý, ô không tồn tại, một đợt dồn 20 giờ và 5 ô bị thiếu giờ. Spark chạy hai truy vấn trên topic: rejects (cách ly vào MongoDB) và forecast (watermark 6 giờ + dropDuplicatesWithinWatermark theo ô và giờ, rồi foreachBatch: bộ đệm lịch sử 120 giờ trên lake, cùng hàm feature với batch, model M4 với ngưỡng 2023, upsert vào MongoDB theo ô|giờ). Offset và state khử trùng nằm trong checkpoint nên sau sự cố job chạy tiếp từ chỗ dừng. Khi demo, producer phát dồn 72 giờ đầu để làm nóng feature rồi chờ nút Bắt đầu trên trang web."));
},
// ingestion
(IC) => {
  const ing = D.ingest;
  const S = slide("CONTENT", SEC(), t("Ingestion: how we download the weather data", "Thu thập: cách tải dữ liệu thời tiết về"));
  S.addText(t("ERA5 splits Vietnam into squares of about 25 × 28 km (“cells”). Every cell has its own hourly weather, so we download cells, not cities.",
    "ERA5 chia Việt Nam thành các ô vuông khoảng 25 × 28 km (“ô lưới”). Mỗi ô có thời tiết riêng theo giờ, nên nhóm tải theo ô, không theo thành phố."),
    { x: MX, y: 1.2, w: W - 2 * MX, h: 0.55, fontSize: 13.5, italic: true, color: C.accent1, margin: 0, isTextBox: true });
  [[t("Find the cell of each city", "Tìm ô chứa mỗi thành phố"), t("Ask the API which cell contains the city (63 old provinces). For coastal cities, take the land cell, not the sea cell.",
    "Hỏi API ô nào chứa thành phố (63 tỉnh cũ). Với thành phố ven biển, lấy ô trên đất liền thay vì ô ngoài biển.")],
   [t("Download each cell only once", "Mỗi ô chỉ tải một lần"), t("Nearby cities can fall in the same cell; the cell is downloaded once and shared.",
    "Hai thành phố gần nhau có thể cùng một ô; ô đó chỉ tải một lần và dùng chung.")],
   [t("One file = one cell × one year", "Mỗi file = một ô × một năm"), t("Official run: 63 cells × 21 years = 1,323 files. If the download stops, running it again continues where it left off.",
    "Bản chính thức: 63 ô × 21 năm = 1.323 file. Nếu đang tải bị dừng, chạy lại sẽ tải tiếp phần còn thiếu.")],
   [t("Check every file before keeping it", "Kiểm tra từng file trước khi giữ"), t("Vietnam time (+7 h); units mm, °C, hPa, km/h; all 8,760 hours of the year (8,784 in leap years); missing values counted in a log.",
    "Giờ Việt Nam (+7 h); đơn vị mm, °C, hPa, km/h; đủ 8.760 giờ trong năm (8.784 nếu năm nhuận); giá trị thiếu ghi vào log.")]]
    .forEach(([a, b], i) => {
      const y = 1.95 + i * 1.05;
      badge(S, C, MX, y + 0.04, i + 1, HEX.accent1);
      S.addText([{ text: a, options: { bold: true, fontSize: 15, breakLine: true } }, { text: b, options: { fontSize: 13 } }],
        { x: MX + 0.45, y, w: 7.1, h: 0.95, color: C.text1, margin: 0, valign: "top", isTextBox: true });
    });
  card(S, C, 8.4, 1.95, 4.33, 2.35, { title: t("Why it takes time: free API limits", "Vì sao tốn thời gian: giới hạn API miễn phí"), titleSize: 14, body: bullets([
    t("At most 10,000 calls per day", "Tối đa 10.000 lượt mỗi ngày"),
    t("1 cell × 1 year counts as 26 calls", "1 ô × 1 năm tính là 26 lượt"),
    t("Official run: 63 cells × 21 years ≈ 34,000 calls ≈ 4 days", "Bản chính thức: 63 ô × 21 năm ≈ 34.000 lượt ≈ 4 ngày"),
  ], 12.5) });
  card(S, C, 8.4, 4.45, 4.33, 1.9, { title: t("Pilot only (dropped after the decision)", "Chỉ có ở pilot (đã bỏ sau quyết định)"), titleSize: 14, titleColor: C.accent4, body: bullets([
    t(`4 neighbour cells per city → ${ing.grids} cells for ${ing.locations} cities`, `4 ô lân cận cho mỗi thành phố → ${ing.grids} ô cho ${ing.locations} thành phố`),
    t("Elevation, slope and coast distance per city", "Độ cao, độ dốc, khoảng cách biển cho mỗi thành phố"),
  ], 12.5) });
  source(S, C, t("Code: src/ingest/collect_era5.py · log: data/raw/era5/collect_log.csv", "Code: src/ingest/collect_era5.py · log: data/raw/era5/collect_log.csv"));
},

// storage
(IC) => {
  const st = D.storage, q = D.quality, ing = D.ingest;
  const S = slide("CONTENT", SEC(), t("Storage: layers, layout and size for the official run", "Lưu trữ: các tầng, cách tổ chức và dung lượng cho bản chính thức"));
  // Số đo bản chính thức: điền sau khi tải đủ 63 ô × 2005–2025; chưa có thì để xxx.
  const off = D.storage_official || {};
  const X = (v) => ({ text: v == null ? "xxx" : v, options: { bold: true, color: v == null ? C.accent6 : C.text1 } });
  const mb = (x) => (x >= 1000 ? `${f(x / 1000, 2)} GB` : `${f(x, 0)} MB`);
  const offv = (k) => (off[k] == null ? null : mb(off[k]));
  table(S, C, [[t("Layer", "Tầng"), t("What it holds", "Chứa gì"), t("Format · folders", "Định dạng · thư mục"),
    t("Pilot (measured)", "Pilot (đo thật)"), t("Official run (measured)", "Bản chính thức (đo thật)")],
    [{ text: "raw/", options: { bold: true } }, t("API files exactly as downloaded", "file API đúng như khi tải"), "JSON · grid=…/year=…", `${mb(st.raw_json)} · ${n(ing.raw_files)} ${t("files", "file")}`, X(offv("raw"))],
    [{ text: "bronze/", options: { bold: true } }, t("one row per cell-hour, values untouched", "mỗi dòng = ô × giờ, giữ nguyên giá trị"), "Parquet · year=…", `${mb(st.bronze)} · ${n(q.silver.bronze_rows)} ${t("rows", "dòng")}`, X(offv("bronze"))],
    [{ text: "silver/", options: { bold: true } }, t("cleaned cell-hours, Vietnam time", "ô × giờ đã làm sạch, giờ VN"), "Parquet · year=…", `${mb(st.silver)}`, X(offv("silver"))],
    [{ text: "gold/", options: { bold: true } }, t("city-hour: label + 24 features", "thành phố × giờ: nhãn + 24 feature"), "Parquet · year=…", `${mb(st.gold)} · ${n(q.gold.rows)} ${t("rows", "dòng")}`, X(offv("gold"))],
    [{ text: "models/", options: { bold: true } }, t("trained model + threshold + manifest", "model đã train + ngưỡng + manifest"), "Spark ML + JSON", `${mb(st.models)} ${t("(all pilot runs)", "(mọi lần chạy pilot)")}`, X(offv("models"))],
  ], { x: MX, y: 1.3, w: W - 2 * MX, colW: [1.2, 3.3, 2.1, 2.85, 2.68], fontSize: 12.5 });
  const total = ["raw", "bronze", "silver", "gold"].every((k) => off[k] != null) ? off.raw + off.bronze + off.silver + off.gold : null;
  const blocks = (k) => (off[k] == null ? "xxx" : Math.ceil(off[k] / 128));
  card(S, C, MX, 4.15, 6.0, 2.2, { title: t("On HDFS (cluster)", "Trên HDFS (cụm)"), titleSize: 15, body: bullets([
    t(`Total ${total == null ? "xxx" : mb(total)}; with replication 3: ${total == null ? "xxx" : mb(total * 3)} of disk`,
      `Tổng ${total == null ? "xxx" : mb(total)}; nhân 3 bản sao: ${total == null ? "xxx" : mb(total * 3)} ổ đĩa`),
    t(`128 MB blocks: raw ${blocks("raw")} blocks, gold ${blocks("gold")} blocks → that many parallel read tasks`,
      `Block 128 MB: raw ${blocks("raw")} block, gold ${blocks("gold")} block → bấy nhiêu task đọc song song`),
    t("NameNode keeps the file list; DataNodes store the blocks", "NameNode giữ danh sách file; DataNode lưu các block"),
  ], 12.5) });
  card(S, C, 6.73, 4.15, 6.0, 2.2, { title: t("Why this layout", "Vì sao tổ chức như vậy"), titleSize: 15, body: bullets([
    t("Folders by year: reading train 2005–2022 skips the 2023–2025 folders", "Thư mục theo năm: đọc train 2005–2022 bỏ qua thư mục 2023–2025"),
    t("Parquet is columnar: the model reads only the 24 feature columns", "Parquet lưu theo cột: model chỉ đọc 24 cột feature"),
    t("Raw is kept so any later step can be rebuilt without re-downloading", "Giữ raw để làm lại mọi bước sau mà không cần tải lại"),
  ], 12.5) });
  source(S, C, t("xxx = not measured yet; filled in after the full download (63 cells × 2005–2025).",
    "xxx = chưa đo; sẽ điền sau khi tải đủ bộ dữ liệu (63 ô × 2005–2025)."));
},

// processing & quality
(IC) => {
  const sq = D.quality.silver, ing = D.ingest, nl = D.ablation.manifest.locations.length;
  const S = slide("CONTENT", SEC(), t("Processing and data quality: what each step does to the rows", "Xử lý và chất lượng dữ liệu: mỗi bước làm gì với các dòng"));
  const excl = D.quality.exclusions.filter((e) => e.split_excluded_reason !== "kept");
  const per = excl.length ? excl[0].count : 0;
  const steps = [
    ["Bronze", n(sq.bronze_rows), t(`every hour of every cell (${ing.grids} cells × 7 years)`, `mỗi giờ của mỗi ô (${ing.grids} ô × 7 năm)`), "accent6"],
    ["Silver", n(sq.silver_rows), t(`after cleaning: ${sq.duplicates_removed} duplicates, ${sq.grid_years_incomplete} incomplete years found`, `sau làm sạch: ${sq.duplicates_removed} dòng trùng, ${sq.grid_years_incomplete} năm thiếu giờ`), "accent5"],
    ["Gold", n(D.quality.gold.rows), t(`one row per city-hour (${nl} cities); neighbour cells become columns, not rows`, `mỗi dòng = thành phố × giờ (${nl} thành phố); ô lân cận thành cột, không thành dòng`), "accent2"],
    [t("Model input", "Đầu vào model"), n(D.quality.gold.rows - excl.reduce((a, e) => a + e.count, 0)), t(`minus ${per} rows at each split boundary (24 h × ${nl} cities)`, `bớt ${per} dòng ở mỗi ranh giới split (24 giờ × ${nl} thành phố)`), "accent1"],
  ];
  const bw = (W - 2 * MX - 0.45) / 4;
  steps.forEach(([h, v, d, col], i) => {
    const x = MX + i * (bw + 0.15);
    S.addShape("roundRect", { x, y: 1.3, w: bw, h: 1.75, rectRadius: 0.08, fill: { color: C.background2 }, line: { color: C[col], width: 1.5 } });
    S.addText(h, { x: x + 0.12, y: 1.38, w: bw - 0.24, h: 0.35, fontSize: 14, bold: true, color: C[col], margin: 0, isTextBox: true });
    S.addText(`${v} ${t("rows", "dòng")}`, { x: x + 0.12, y: 1.73, w: bw - 0.24, h: 0.5, fontSize: 20, bold: true, color: C.text2, margin: 0, isTextBox: true });
    S.addText(d, { x: x + 0.12, y: 2.25, w: bw - 0.24, h: 0.75, fontSize: 11.5, color: C.text1, margin: 0, valign: "top", isTextBox: true });
    if (i < 3) arrow(S, C, x + bw, 2.17, x + bw + 0.15, 2.17, { width: 2 });
  });
  table(S, C, [[t("Problem", "Vấn đề"), t("What would go wrong", "Nếu không xử lý thì sai thế nào"), t("Our rule", "Cách nhóm xử lý")],
    [t("Rain time stamp", "Mốc giờ của lượng mưa"), t("The value at 14:00 is rain from 13:00 to 14:00; reading it as 14:00–15:00 leaks the future into the features",
      "Giá trị lúc 14:00 là mưa từ 13:00 đến 14:00; hiểu nhầm thành 14:00–15:00 là đưa tương lai vào feature"), t("Features use hours up to t; the label uses the 24 hours after t", "Feature chỉ dùng giờ ≤ t; nhãn dùng 24 giờ sau t")],
    [t("Time zone", "Múi giờ"), t("UTC days are shifted 7 hours from Vietnam days", "Ngày theo UTC lệch 7 giờ so với ngày Việt Nam"), t("Request Vietnam time; check the +7 h offset in every file", "Lấy dữ liệu theo giờ Việt Nam; kiểm tra lệch +7 h ở mọi file")],
    [t("Missing hour", "Giờ bị thiếu"), t("Counting it as 0 mm makes a rainy day look dry", "Coi là 0 mm thì ngày mưa thành ngày khô"), t("Keep it missing; a window with a missing hour gets no value", "Giữ là thiếu; cửa sổ có giờ thiếu thì không tính")],
    [t("Impossible value", "Giá trị vô lý"), t("Humidity above 100%, dew point above temperature", "Độ ẩm trên 100%, điểm sương cao hơn nhiệt độ"), t("Set to missing; counted in the quality report", "Đặt thành thiếu; đếm trong báo cáo chất lượng")],
    [t("Duplicate rows", "Dòng trùng"), t("A cell-hour downloaded twice would count twice", "Một ô-giờ tải hai lần sẽ bị đếm hai lần"), t("Keep one row per (cell, hour)", "Giữ một dòng cho mỗi (ô, giờ)")],
    [t("Windows mixing cities", "Cửa sổ lẫn thành phố"), t("A 72 h sum could add rain from two cities", "Tổng 72 giờ có thể cộng lẫn mưa của hai thành phố"), t("Every window is computed per city, along time", "Mỗi cửa sổ tính riêng từng thành phố, theo thời gian")],
    [t("Split boundary", "Ranh giới split"), t("A label on 31/12 would use rain from the next year's test set", "Nhãn ngày 31/12 sẽ dùng mưa của tập test năm sau"), t("Drop the last 24 h before each boundary", "Bỏ 24 giờ cuối trước mỗi ranh giới")],
  ], { x: MX, y: 3.3, w: W - 2 * MX, colW: [2.2, 5.6, 4.33], fontSize: 11.5 });
  source(S, C, t("Pilot numbers. Reports: reports/quality/silver_funnel.csv, silver_nulls.csv, gold_split_exclusions.csv", "Số liệu pilot. Báo cáo: reports/quality/silver_funnel.csv, silver_nulls.csv, gold_split_exclusions.csv"));
},

// EDA
(IC) => {
  const S = slide("CONTENT", SEC(), t("EDA: heavy rain is rare, seasonal and regional", "EDA: mưa lớn hiếm, theo mùa và theo vùng"));
  const eda = D.eda_region_month || [];
  const RN = { Bac: t("North", "Bắc Bộ"), BacTrungBo: t("North Central coast", "Bắc Trung Bộ"), NamTrungBo: t("South Central coast", "Nam Trung Bộ"),
    TayNguyen: t("Central Highlands", "Tây Nguyên"), Nam: t("South", "Nam Bộ") };
  const regions = [...new Set(eda.map((r) => r.region))];
  const months = t("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec", "T1 T2 T3 T4 T5 T6 T7 T8 T9 T10 T11 T12").split(" ");
  S.addChart(pres.ChartType.line, regions.map((rg) => ({ name: RN[rg] || rg, labels: months,
    values: months.map((_, i) => { const r = eda.find((e) => e.region === rg && e.month === i + 1); return r ? r.heavy_pct : 0; }) })),
    Object.assign(CT(), { x: MX, y: 1.3, w: 8.0, h: 5.0, chartColors: [HEX.accent1, HEX.accent2, HEX.accent5, HEX.accent6, HEX.accent3],
      lineSize: 2, lineDataSymbol: "circle", lineDataSymbolSize: 5, showLegend: true, legendPos: "b", showTitle: true,
      title: t("Share of hours followed by heavy rain (> 50 mm in the next 24 h), 2019–2025", "Tỷ lệ số giờ có mưa lớn ngay sau đó (> 50 mm trong 24 h tới), 2019–2025"),
      valAxisTitle: t("% of hours", "% số giờ"), showValAxisTitle: true, catAxisTitle: t("month", "tháng"), showCatAxisTitle: true,
      valAxisLabelFormatCode: "0\"%\"" }));
  const top = eda.reduce((a, b) => (b.heavy_pct > a.heavy_pct ? b : a), eda[0] || { heavy_pct: 0, month: 1, region: "" });
  const allH = eda.reduce((a, r) => a + r.hours, 0), allP = eda.reduce((a, r) => a + r.heavy_hours, 0);
  S.addText(pct(allP / Math.max(allH, 1), 1), { x: 8.9, y: 1.35, w: 3.8, h: 0.8, fontSize: 40, bold: true, color: C.accent2, margin: 0, isTextBox: true });
  S.addText(t("of all city-hours are followed by heavy rain — about 1 hour in 75", "số giờ (theo thành phố) có mưa lớn ngay sau đó — khoảng 1 giờ trong 75 giờ"),
    { x: 8.9, y: 2.15, w: 3.83, h: 0.65, fontSize: 13, color: C.text1, margin: 0, isTextBox: true });
  card(S, C, 8.9, 2.95, 3.83, 1.35, { title: t("How to read the chart", "Cách đọc biểu đồ"), titleSize: 13.5, body:
    t(`Example: in ${months[top.month - 1]}, ${f(top.heavy_pct, 1)}% of hours in the ${RN[top.region]} are followed by more than 50 mm in the next 24 h.`,
      `Ví dụ: vào ${months[top.month - 1]}, ${f(top.heavy_pct, 1)}% số giờ ở ${RN[top.region]} có mưa trên 50 mm trong 24 giờ sau đó.`), bodySize: 12 });
  card(S, C, 8.9, 4.45, 3.83, 1.85, { title: t("What it changes", "Điều này thay đổi gì"), titleSize: 13.5, body: bullets([
    t("“Always no” would be 98.7% accurate → use PR-AUC, recall, precision", "Đoán “luôn không” đã đúng 98,7% → dùng PR-AUC, recall, precision"),
    t("Rare positives get a higher training weight", "Dòng dương hiếm được tăng trọng số khi train"),
    t("Report results by region and season", "Báo cáo kết quả theo vùng và mùa")], 12) });
  source(S, C, t("Pilot data (12 cities). Query Q1 (Spark SQL) → reports/spark_sql/q1_heavy_rate_region_month_result.csv", "Dữ liệu pilot (12 thành phố). Truy vấn Q1 (Spark SQL) → reports/spark_sql/q1_heavy_rate_region_month_result.csv"));
},

// features
(IC) => {
  const GROUPS = require("./s_pilot").GROUPS();
  const S = slide("CONTENT", SEC(), t("Feature engineering: 24 features from the past 72 hours", "Tạo feature: 24 feature từ 72 giờ qua"));
  const tlY = 1.4, tx0 = MX + 0.3, tx1 = W - MX - 0.3, tp = (h) => tx0 + ((h + 72) / 96) * (tx1 - tx0);
  S.addText(t("features use only (t − 72 h, t]", "feature chỉ dùng (t − 72 h, t]"), { shape: "rect", x: tp(-72), y: tlY, w: tp(0) - tp(-72), h: 0.42, fill: { color: C.accent1 },
    color: C.background1, fontSize: 14, bold: true, align: "center", valign: "middle", margin: 0, isTextBox: true });
  S.addText(t("label: Σ rain (t, t+24 h] > 50 mm", "nhãn: Σ mưa (t, t+24 h] > 50 mm"), { shape: "rect", x: tp(0), y: tlY, w: tp(24) - tp(0), h: 0.42, fill: { color: C.accent2 },
    color: C.background1, fontSize: 12, bold: true, align: "center", valign: "middle", margin: 0, isTextBox: true });
  [[-72, "t−72h"], [-24, "t−24h"], [-6, "t−6h"], [0, t("t (now)", "t (hiện tại)")], [24, "t+24h"]].forEach(([h, l]) =>
    S.addText(l, { x: tp(h) - 0.6, y: tlY + 0.48, w: 1.2, h: 0.3, fontSize: 11, color: C.text1, align: "center", margin: 0, isTextBox: true }));
  GROUPS.forEach(([g, feats, col, desc], i) => {
    const x = MX + (i % 3) * 4.1, y = 2.4 + Math.floor(i / 3) * 1.55;
    S.addShape("roundRect", { x, y, w: 3.95, h: 1.42, rectRadius: 0.06, fill: { color: C.background2 }, line: { color: C[col], width: 1.25 } });
    S.addText([{ text: `${g} (${feats.length})`, options: { bold: true, fontSize: 13.5, color: C[col], breakLine: true } },
      { text: desc, options: { fontSize: 13, breakLine: true } }, { text: feats.join(", "), options: { fontSize: 10.5, fontFace: "Courier New" } }],
      { x: x + 0.12, y: y + 0.06, w: 3.7, h: 1.3, color: C.text1, margin: 0, valign: "top", isTextBox: true });
  });
  S.addText([{ text: t("Example: ", "Ví dụ: "), options: { bold: true } },
    { text: t("rain_24h at 14:00 = sum of the 24 hourly values from 14:00 yesterday (exclusive) to 14:00 today; pressure_change_24h = pressure now − pressure 24 h ago. Every rule has a unit test.",
      "rain_24h lúc 14:00 = tổng 24 giá trị mưa giờ từ sau 14:00 hôm qua đến 14:00 hôm nay; pressure_change_24h = áp suất hiện tại − áp suất 24 giờ trước. Mỗi quy tắc đều có unit test.") }],
    { x: MX, y: 5.6, w: W - 2 * MX, h: 0.7, fontSize: 12.5, color: C.text1, margin: 0, isTextBox: true });
  source(S, C, t("Terrain and neighbour groups were tested in the pilot and dropped. Code: src/features/local_weather.py (shared by batch and streaming)",
    "Nhóm địa hình và ô lân cận đã thử ở pilot và bị bỏ. Code: src/features/local_weather.py (dùng chung cho batch và streaming)"));
},

// MapReduce
(IC) => {
  const mr = D.mapreduce || {}, mv = mr.validation || {};
  const logv = (k) => { const l = (mr.log || []).find((x) => x.startsWith(k)); return l ? l.split("=").pop() : "–"; };
  const cin = Number(logv("Combine input records")), cout = Number(logv("Combine output records"));
  const S = slide("CONTENT", SEC(), t("MapReduce on Hadoop: heavy-rain hours per city and month", "MapReduce trên Hadoop: giờ mưa lớn theo thành phố, tháng"));
  // bài toán: một câu hỏi lớn, nhìn là hiểu
  S.addText(t("For each city and each month: what share of hours is followed by > 50 mm of rain?",
    "Mỗi thành phố, mỗi tháng: bao nhiêu % số giờ có mưa > 50 mm ngay sau đó?"),
    { x: MX, y: 1.2, w: W - 2 * MX, h: 0.6, fontSize: 22, bold: true, color: C.text2, margin: 0, isTextBox: true, fit: "shrink" });
  S.addText(t("→ a climatology table: the baseline the model must beat, and an EDA table of when and where heavy rain happens",
    "→ bảng climatology: baseline mà model phải vượt, và bảng EDA cho biết mưa lớn xảy ra khi nào, ở đâu"),
    { x: MX, y: 1.85, w: W - 2 * MX, h: 0.4, fontSize: 14, color: C.text1, margin: 0, isTextBox: true });
  // luồng: HDFS → Map → Combine → Shuffle → Reduce → kết quả
  const y = 2.85, hb = 1.45, sw = 1.55, gap = 0.25, pw = (W - 2 * MX - 2 * sw - 5 * gap) / 4;
  const cyl = (x, name, sub) => S.addText([{ text: name, options: { bold: true, fontSize: 13, breakLine: true } }, { text: sub, options: { fontSize: 10 } }],
    { shape: "can", x, y: y - 0.1, w: sw, h: hb + 0.2, fill: { color: C.accent1 }, line: { color: C.accent1 }, color: C.background1,
      align: "center", valign: "bottom", margin: [0, 0.05, 0.12, 0.05], isTextBox: true });
  cyl(MX, "HDFS", t(`mr/ CSV · train years`, `mr/ CSV · các năm train`));
  const xs = [MX];
  [["Map", t("each hourly row → key city|month, value (1, label, rain)", "mỗi dòng giờ → khóa thành phố|tháng, giá trị (1, nhãn, mưa)"), t(`${n(cin)} rows in`, `${n(cin)} dòng vào`)],
   ["Combine", t("sum count, labels, rain and take max inside each map task", "cộng số giờ, nhãn, mưa và lấy max ngay trong mỗi map task"), t(`→ ${n(cout)} records (${Math.round(cin / cout)}× less)`, `→ ${n(cout)} bản ghi (giảm ${Math.round(cin / cout)} lần)`)],
   ["Shuffle", t("Hadoop groups all records of one key on one reducer", "Hadoop gom mọi bản ghi cùng khóa về một reducer"), t(`${logv("Launched map tasks")} map → ${logv("Launched reduce tasks")} reduce on YARN`, `${logv("Launched map tasks")} map → ${logv("Launched reduce tasks")} reduce trên YARN`)],
   ["Reduce", t("add the parts, then rate = labels / hours, mean, max", "cộng các phần, rồi tỷ lệ = nhãn / số giờ, trung bình, max"), t(`${logv("Reduce output records")} rows · 12 cities × 12 mo.`, `${logv("Reduce output records")} dòng · 12 TP × 12 tháng`)],
  ].forEach(([head, sub, num], i) => {
    const x = MX + sw + gap + i * (pw + gap);
    xs.push(x);
    S.addShape("roundRect", { x, y, w: pw, h: hb, rectRadius: 0.1, fill: { color: C.background1 }, line: { color: C.text2, width: 1.25 } });
    S.addText([{ text: `${i + 1}. ${head}`, options: { bold: true, fontSize: 15, color: C.text2, breakLine: true } }, { text: sub, options: { fontSize: 11, color: C.text1 } }],
      { x: x + 0.1, y: y + 0.08, w: pw - 0.2, h: hb - 0.16, align: "center", valign: "middle", margin: 0, isTextBox: true });
    S.addText(num, { x: x - 0.2, y: y + hb + 0.12, w: pw + 0.4, h: 0.35, fontSize: 12, bold: true, color: C.accent2, align: "center", margin: 0, isTextBox: true });
  });
  const xo = W - MX - sw;
  xs.push(xo);
  cyl(xo, "climatology", t("MongoDB + HDFS", "MongoDB + HDFS"));
  const widths = [sw, pw, pw, pw, pw, sw];
  for (let i = 0; i < 5; i++) arrow(S, C, xs[i] + widths[i] + 0.03, y + hb / 2, xs[i + 1] - 0.03, y + hb / 2, { color: C.accent2, width: 1.75 });
  // kiểm chứng
  S.addText([{ text: t("Checked: ", "Kiểm chứng: "), options: { bold: true, color: C.accent3 } },
    { text: t(`same table from Spark SQL — ${mv.keys_mapreduce}/${mv.keys_spark} keys, ${mv.mismatches} differences`,
      `Spark SQL tính lại cùng bảng — ${mv.keys_mapreduce}/${mv.keys_spark} khóa, ${mv.mismatches} khác biệt`) }],
    { x: MX, y: 5.4, w: W - 2 * MX, h: 0.45, fontSize: 15, color: C.text1, margin: 0, isTextBox: true });
  // ghi chú cho người trình bày: shuffle là gì + vì sao combiner vẫn đúng
  const nm = logv("Launched map tasks"), nr = logv("Launched reduce tasks"), nk = logv("Reduce output records");
  S.addNotes(t(
`Shuffle (step 3) is done by Hadoop itself, not by our code: it moves and groups data over the network so that every record with the same key reaches one reducer.
Why it is needed: the input is split across ${nm} mappers running in parallel, each reading part of the file, so the same key (e.g. HUE|10, Hue in October) appears in all ${nm} mappers. To compute Hue-October correctly, all its pieces must meet in one place.
What it does: (1) partition — decide which reducer gets each key, by hash(key) mod ${nr}, so a key always goes to the same reducer; (2) transfer — send the data from mapper machines to reducer machines over the network, the most expensive part of MapReduce; (3) sort — each reducer receives its records sorted by key and adds them up key by key.
"${nm} map → ${nr} reduce on YARN": YARN ran ${nm} map tasks and ${nr} reduce tasks; shuffle carries data from the ${nm} on the left to the ${nr} on the right (${nk} keys in total = 12 cities × 12 months).
Why the combiner matters: because each mapper adds up its own records first, shuffle only sends ${n(cout)} records instead of ${n(cin)} raw rows (${Math.round(cin / cout)}× less).
Why the combiner is safe: adding and taking the max give the same answer whether done in parts or all at once. Rate and mean are computed only at the very end in the reducer — an average of averages would be wrong. A test runs the job with and without the combiner and gets identical output.`,
`Shuffle (bước 3) do Hadoop tự làm, nhóm không viết code: chuyển và gom dữ liệu qua mạng sao cho mọi bản ghi cùng một khóa đến đúng một reducer.
Vì sao cần: dữ liệu được chia cho ${nm} mapper chạy song song, mỗi mapper đọc một phần file, nên cùng một khóa (ví dụ HUE|10, Huế tháng 10) xuất hiện ở cả ${nm} mapper. Muốn tính đúng Huế tháng 10 thì phải gom mọi mảnh của khóa đó về một chỗ.
Shuffle làm ba việc: (1) chia — quyết định khóa nào về reducer nào bằng hash(khóa) chia lấy dư cho ${nr}, cùng khóa luôn về cùng reducer; (2) chuyển — gửi dữ liệu từ máy chạy mapper sang máy chạy reducer qua mạng, đây là phần tốn kém nhất của MapReduce; (3) sắp xếp — mỗi reducer nhận dữ liệu đã xếp theo khóa và cộng dồn lần lượt từng khóa.
"${nm} map → ${nr} reduce trên YARN": YARN chạy ${nm} task map và ${nr} task reduce; shuffle chuyển dữ liệu từ ${nm} task bên trái sang ${nr} task bên phải (tổng ${nk} khóa = 12 thành phố × 12 tháng).
Vì sao cần combiner: nhờ mỗi mapper cộng sẵn phần của mình, shuffle chỉ gửi ${n(cout)} bản ghi thay vì ${n(cin)} dòng gốc (ít hơn ${Math.round(cin / cout)} lần).
Vì sao dùng combiner vẫn đúng: phép cộng và lấy max cho cùng kết quả dù làm từng phần hay làm một lần. Tỷ lệ và trung bình chỉ tính ở bước cuối trong reducer — lấy trung bình của các trung bình sẽ sai. Có test chạy job khi bật và tắt combiner, kết quả giống hệt.`));
},

// MapReduce worked example
(IC) => {
  const SM = D.samples, inp = SM.mr_input || [], outp = SM.mr_output || [];
  const mrl = (D.mapreduce || {}).log || [], logv = (k) => { const l = mrl.find((x) => x.startsWith(k)); return l ? l.split("=").pop() : "–"; };
  const S = slide("CONTENT", SEC(), t("MapReduce, step by step on real records", "MapReduce từng bước với dữ liệu thật"));
  const parse = (l) => { const [c, m, lab, p] = l.split(","); return { c, m: String(m).padStart(2, "0"), lab: Number(lab), p: Number(p) }; };
  const rows = inp.map(parse), key = rows.length ? `${rows[0].c}|${rows[0].m}` : "";
  const sum = rows.reduce((a, r) => [a[0] + 1, a[1] + r.lab, a[2] + r.p, Math.max(a[3], r.p)], [0, 0, 0, 0]);
  const fin = outp.find((l) => l.startsWith(key)), [fk, fv] = fin ? fin.split("\t") : ["", ""];
  const [fn, fh, frate, fmean, fmax] = fv ? fv.split(",") : [];
  const hue = outp.find((l) => l.startsWith("HUE|10"));
  // một khóa thật đi qua từng bước, mỗi bước một khung, nối bằng mũi tên
  const boxes = [
    [t("Input on HDFS", "Input trên HDFS"), inp.slice(0, 4).join("\n"), t("city, month, label, rain", "thành phố, tháng, nhãn, mưa")],
    ["Map", rows.slice(0, 4).map((r) => `${key} → (1,${r.lab},${r.p},${r.p})`).join("\n"), t("key = city|month", "khóa = thành phố|tháng")],
    ["Combine", `${key} →\n(${sum[0]},${sum[1]},${f(sum[2], 1)},${sum[3]})`, t(`${rows.length} records → 1`, `${rows.length} bản ghi → 1`)],
    ["Shuffle", t(`every ${key}\n→ one reducer`, `mọi ${key}\n→ một reducer`), t("group by key", "gom theo khóa")],
    ["Reduce", fin ? t(`${fk} →\n${fn} h, ${fh} heavy\nmax ${fmax} mm/h`, `${fk} →\n${fn} giờ, ${fh} mưa lớn\nmax ${fmax} mm/giờ`) : "–", t("rate, mean, max", "tỷ lệ, trung bình, max")],
  ];
  const cols = [C.accent4, C.accent1, C.accent5, C.accent4, C.accent2], gap = 0.3, bw = (W - 2 * MX - 4 * gap) / 5, y0 = 1.4;
  boxes.forEach(([h, body, cap], i) => {
    const x = MX + i * (bw + gap);
    S.addText(h, { shape: "roundRect", rectRadius: 0.08, x, y: y0, w: bw, h: 0.5, fill: { color: cols[i] }, color: C.background1, fontSize: 15, bold: true,
      align: "center", valign: "middle", margin: 0 });
    S.addText(body, { x, y: y0 + 0.6, w: bw, h: 1.5, fontFace: "Consolas", fontSize: i < 2 ? 9.5 : 12, color: C.text1, fill: { color: C.background2 },
      align: "center", valign: "middle", margin: 0.08, isTextBox: true });
    S.addText(cap, { x, y: y0 + 2.18, w: bw, h: 0.35, fontSize: 12.5, bold: true, color: C.accent2, align: "center", margin: 0, isTextBox: true });
    if (i < 4) arrow(S, C, x + bw + 0.03, y0 + 1.35, x + bw + gap - 0.03, y0 + 1.35, { color: C.accent2, width: 1.75 });
  });
  // kết quả đáng nhớ: chữ lớn
  if (hue) {
    const [hk, hv] = hue.split("\t"), [hn, hh, hr] = hv.split(",");
    S.addText(`${pct(Number(hr), 1)}`, { x: MX, y: 4.55, w: 3.2, h: 1.0, fontSize: 60, bold: true, color: C.accent2, margin: 0, valign: "middle", isTextBox: true });
    S.addText([{ text: t(`${hk}: in October at Hue, about 1 hour in 6 is followed by > 50 mm`, `${hk}: tháng 10 ở Huế, cứ khoảng 6 giờ có 1 giờ có mưa > 50 mm sau đó`), options: { bold: true, color: C.text2, fontSize: 18, breakLine: true } },
      { text: t(`${n(Number(hh))} of ${n(Number(hn))} hours · one of the ${logv("Reduce output records")} rows the job writes`, `${n(Number(hh))} trên ${n(Number(hn))} giờ · một trong ${logv("Reduce output records")} dòng job ghi ra`), options: { color: C.text1, fontSize: 14 } }],
      { x: MX + 3.3, y: 4.55, w: W - 2 * MX - 3.3, h: 1.0, margin: 0, valign: "middle", isTextBox: true });
  }
  S.addNotes(t(
    `Walk one key through the job. Input: each line is city, month, label (1 = more than 50 mm in the next 24 h) and rain that hour in mm. Map turns each line into key city|month and value (hours, heavy hours, rain sum, rain max). Combine adds the ${rows.length} records into one inside the same map task. Shuffle sends every record of ${key} from every map task to one reducer, grouped and sorted by key. Reduce adds the parts and only then computes rate, mean and max` + (fin ? ` — ${key}: ${n(Number(fn))} June hours in 2019–2022, ${fh} heavy, rate ${pct(Number(frate), 1)}, mean ${f(Number(fmean), 2)} mm/h, max ${fmax} mm/h.` : "."),
    `Theo một khóa đi qua job. Input: mỗi dòng là thành phố, tháng, nhãn (1 = mưa trên 50 mm trong 24 h tới) và mưa giờ đó (mm). Map biến mỗi dòng thành khóa thành phố|tháng và giá trị (số giờ, giờ mưa lớn, tổng mưa, mưa max). Combine cộng ${rows.length} bản ghi thành 1 ngay trong cùng map task. Shuffle gửi mọi bản ghi ${key} từ mọi map task về một reducer, gom và sắp theo khóa. Reduce cộng các phần rồi mới tính tỷ lệ, trung bình, max` + (fin ? ` — ${key}: ${n(Number(fn))} giờ tháng 6 giai đoạn 2019–2022, ${fh} giờ mưa lớn, tỷ lệ ${pct(Number(frate), 1)}, trung bình ${f(Number(fmean), 2)} mm/giờ, max ${fmax} mm/giờ.` : ".")));
},

// Spark SQL — queries
(IC) => {
  const S = slide("CONTENT", SEC(), t("Spark SQL: three analytical queries and what they return", "Spark SQL: ba truy vấn phân tích và kết quả"));
  const RN = { Bac: t("North", "Bắc Bộ"), BacTrungBo: t("North Central coast", "Bắc Trung Bộ"), NamTrungBo: t("South Central coast", "Nam Trung Bộ"),
    TayNguyen: t("Central Highlands", "Tây Nguyên"), Nam: t("South", "Nam Bộ") };
  const q1 = (D.eda_region_month || []).slice().sort((a, b) => b.heavy_pct - a.heavy_pct)[0];
  const q2 = (D.eda_province_year || []).slice().sort((a, b) => b.heavy_days - a.heavy_days)[0];
  const qs = [
    ["Q1", t("How often does heavy rain follow, per region and month?", "Mưa lớn xảy ra thường xuyên thế nào theo vùng và tháng?"),
      "SELECT region, month, avg(label) … GROUP BY region, month",
      q1 ? t(`top row: ${RN[q1.region] || q1.region}, month ${q1.month} → ${f(q1.heavy_pct, 1)}% of hours`, `dòng cao nhất: ${RN[q1.region] || q1.region}, tháng ${q1.month} → ${f(q1.heavy_pct, 1)}% số giờ`) : "–",
      t("group-by → one shuffle", "group-by → một lần shuffle")],
    ["Q2", t("How many heavy-rain days per province per year, and at what elevation?", "Mỗi tỉnh mỗi năm có bao nhiêu ngày mưa lớn, ở độ cao nào?"),
      "daily rain ⋈ locations ⋈ geography … GROUP BY province, year",
      q2 ? t(`top row: ${q2.new_province} ${q2.year} → ${q2.heavy_days} days > 50 mm (max ${f(q2.max_day_mm, 0)} mm/day)`,
        `dòng cao nhất: ${q2.new_province} ${q2.year} → ${q2.heavy_days} ngày > 50 mm (max ${f(q2.max_day_mm, 0)} mm/ngày)`) : "–",
      t("two joins with small tables → broadcast", "hai phép join với bảng nhỏ → broadcast")],
    ["Q3", t("What were the 5 wettest 24-hour periods at each city?", "5 khoảng 24 giờ mưa nhiều nhất ở mỗi thành phố là khi nào?"),
      "row_number() OVER (PARTITION BY location ORDER BY rain DESC)",
      t("5 rows per city with time and amount", "5 dòng mỗi thành phố, kèm thời điểm và lượng mưa"),
      t("window → shuffle by city, then sort", "window → shuffle theo thành phố, rồi sắp xếp")],
  ];
  const big = [
    q1 ? [`${f(q1.heavy_pct, 1)}%`, t(`of hours · ${RN[q1.region] || q1.region}, month ${q1.month}`, `số giờ · ${RN[q1.region] || q1.region}, tháng ${q1.month}`)] : ["–", ""],
    q2 ? [t(`${q2.heavy_days} days`, `${q2.heavy_days} ngày`), t(`> 50 mm · ${q2.new_province} ${q2.year}`, `> 50 mm · ${q2.new_province} ${q2.year}`)] : ["–", ""],
    [t("Top 5", "Top 5"), t("wettest 24 h per city, with time", "24 h mưa nhiều nhất mỗi TP, kèm thời điểm")],
  ];
  const ops = [["GROUP BY", t("1 shuffle", "1 lần shuffle")], ["JOIN × 2", t("broadcast small tables", "broadcast bảng nhỏ")], ["WINDOW", t("shuffle by city, sort", "shuffle theo TP, sắp xếp")]];
  qs.forEach(([id, q], i) => {
    const y = 1.4 + i * 1.65, h = 1.25, col = [C.accent1, C.accent5, C.accent2][i];
    S.addText(id, { shape: "ellipse", x: MX, y: y + 0.25, w: 0.75, h: 0.75, fill: { color: col }, color: C.background1,
      fontSize: 18, bold: true, align: "center", valign: "middle", margin: 0 });
    S.addText(q, { x: 1.55, y, w: 4.3, h, fontSize: 15, bold: true, color: C.text2, margin: 0, valign: "middle", isTextBox: true });
    arrow(S, C, 5.9, y + h / 2, 6.35, y + h / 2, { color: C.accent2, width: 1.75 });
    S.addShape("roundRect", { x: 6.4, y: y + 0.15, w: 2.7, h: h - 0.3, rectRadius: 0.08, fill: { color: C.background1 }, line: { color: col, width: 1.5 } });
    S.addText([{ text: ops[i][0], options: { bold: true, fontSize: 16, color: col, fontFace: "Consolas", breakLine: true } }, { text: ops[i][1], options: { fontSize: 11.5, color: C.text1 } }],
      { x: 6.45, y: y + 0.15, w: 2.6, h: h - 0.3, align: "center", valign: "middle", margin: 0, isTextBox: true });
    arrow(S, C, 9.15, y + h / 2, 9.6, y + h / 2, { color: C.accent2, width: 1.75 });
    S.addText([{ text: big[i][0], options: { bold: true, fontSize: 30, color: C.accent2, breakLine: true } }, { text: big[i][1], options: { fontSize: 12, color: C.text1 } }],
      { x: 9.7, y, w: W - MX - 9.7, h, margin: 0, valign: "middle", isTextBox: true });
  });
  source(S, C, t("Pilot data (12 cities).", "Dữ liệu pilot (12 thành phố)."));
  S.addNotes(qs.map(([id, q, sql, res, plan]) => `${id}: ${q} — ${sql} — ${res}. ${t("Execution", "Cách chạy")}: ${plan}.`).join("\n"));
},

// Spark SQL — reading the plan
(IC) => {
  const S = slide("CONTENT", SEC(), t("Reading Spark's execution plan for Q2 (bottom to top)", "Đọc execution plan của Q2 (từ dưới lên)"));
  const steps = [
    [t("Scan gold (Parquet)", "Đọc bảng gold (Parquet)"), t("reads only the needed columns", "chỉ đọc các cột cần dùng"), "local"],
    [t("Partial sum per city-day", "Cộng sơ bộ theo thành phố-ngày"), t("each task adds its own rows first", "mỗi task tự cộng phần của mình trước"), "local"],
    [t("Exchange (shuffle)", "Exchange (shuffle)"), t("rows of the same city-day move to one task — the costly network step", "các dòng cùng thành phố-ngày được chuyển về một task — bước tốn mạng"), "shuffle"],
    [t("Final daily rain", "Mưa ngày hoàn chỉnh"), t("one row per city-day", "một dòng cho mỗi thành phố-ngày"), "local"],
    [t("Broadcast join × 2", "Broadcast join × 2"), t("the small city and geography tables are copied to every task; the big table does not move", "bảng thành phố và địa lý nhỏ được chép sang mọi task; bảng lớn không phải di chuyển"), "broadcast"],
    [t("Group by province, year", "Gom theo tỉnh, năm"), t("partial → Exchange → final, then sort for ORDER BY", "cộng sơ bộ → Exchange → cộng cuối, rồi sắp xếp cho ORDER BY"), "shuffle"],
  ];
  const COL = { local: "accent1", shuffle: "accent2", broadcast: "accent3" };
  steps.slice().reverse().forEach(([h, d, kind], i) => {
    const y = 1.3 + i * 0.83, num = steps.length - i;
    S.addText(String(num), { shape: "ellipse", x: MX, y: y + 0.12, w: 0.45, h: 0.45, fill: { color: C[COL[kind]] }, color: C.background1,
      fontSize: 13, bold: true, align: "center", valign: "middle", margin: 0, isTextBox: true });
    S.addText([{ text: h, options: { bold: true, fontSize: 14, color: C[COL[kind]], breakLine: true } }, { text: d, options: { fontSize: 12 } }],
      { x: MX + 0.6, y, w: 6.6, h: 0.72, color: C.text1, margin: 0, valign: "middle", isTextBox: true });
    if (i < steps.length - 1) arrow(S, C, MX + 0.225, y + 0.83 + 0.1, MX + 0.225, y + 0.6, { width: 1.25, color: C.accent4 });
  });
  [[t("Local work", "Xử lý tại chỗ"), t("inside one task, no network", "trong một task, không qua mạng"), "accent1"],
   [t("Shuffle (Exchange)", "Shuffle (Exchange)"), t("data moves between machines — what we try to minimise", "dữ liệu chuyển giữa các máy — thứ cần giảm thiểu"), "accent2"],
   [t("Broadcast", "Broadcast"), t("a small table copied everywhere instead of shuffling the big one", "chép bảng nhỏ đi khắp nơi thay vì shuffle bảng lớn"), "accent3"]].forEach(([h, d, col], i) => {
    const y = 1.3 + i * 0.85;
    S.addShape("roundRect", { x: 7.95, y, w: 4.78, h: 0.75, rectRadius: 0.06, fill: { color: C.background2 }, line: { color: C[col], width: 1.5 } });
    S.addText([{ text: h, options: { bold: true, color: C[col], fontSize: 13, breakLine: true } }, { text: d, options: { fontSize: 11.5 } }],
      { x: 8.05, y: y + 0.03, w: 4.6, h: 0.7, color: C.text1, margin: 0, valign: "middle", isTextBox: true });
  });
  card(S, C, 7.95, 3.95, 4.78, 2.4, { title: t("Performance decisions", "Quyết định hiệu năng"), titleSize: 14, body: bullets([
    t("Broadcast the small tables so the large daily table is never shuffled for the join", "Broadcast bảng nhỏ để bảng mưa ngày lớn không phải shuffle khi join"),
    t("Partial sums before each shuffle: fewer rows cross the network", "Cộng sơ bộ trước mỗi lần shuffle: ít dòng qua mạng hơn"),
    t("Folders by year + columnar Parquet: read less from disk", "Thư mục theo năm + Parquet theo cột: đọc ít dữ liệu hơn"),
    t("Number of shuffle partitions to be measured in E6", "Số partition khi shuffle sẽ đo ở E6"),
  ], 12) });
  source(S, C, t("Simplified from the real plan: reports/spark_sql/q2_heavy_days_province_year_plan.txt (explain mode = formatted)", "Rút gọn từ plan thật: reports/spark_sql/q2_heavy_days_province_year_plan.txt (explain mode = formatted)"));
},

// model choice
(IC) => {
  const S = slide("CONTENT", SEC(), t("ML: choosing the model", "ML: chọn mô hình"));
  table(S, C, [[t("Candidate", "Ứng viên"), t("How it works", "Cách hoạt động"), t("Strength", "Ưu điểm"), t("Weakness", "Nhược điểm"), t("Role", "Vai trò")],
    ["Persistence", t("Last 24 h > 50 mm → predict yes", "24 h qua > 50 mm → đoán có"), t("Trivial", "Rất đơn giản"), t("Misses new events", "Bỏ sót đợt mưa mới"), "Baseline"],
    ["Climatology", t("Heavy-rain frequency per (point, month)", "Tần suất mưa lớn theo (điểm, tháng)"), t("Knows season and place", "Biết mùa và nơi"), t("Ignores today's weather", "Bỏ qua thời tiết hôm nay"), "Baseline"],
    ["Logistic regression", t("Weighted sum + sigmoid", "Tổng có trọng số + sigmoid"), t("Fast, interpretable", "Nhanh, dễ giải thích"), t("Linear only", "Chỉ tuyến tính"), t("Not kept", "Không giữ")],
    [{ text: "Random Forest ★", options: { bold: true, color: C.accent2 } }, t("Bagged decision trees, averaged votes", "Nhiều cây quyết định, lấy trung bình phiếu"),
      t("Non-linear interactions (“high 72 h rain AND falling pressure”), robust, trains in parallel in Spark ML, feature importance",
        "Học tương tác phi tuyến (“mưa 72 h cao VÀ áp suất giảm”), ổn định, train song song trên Spark ML, có feature importance"),
      t("Heavier than LR; probabilities not calibrated", "Nặng hơn LR; xác suất chưa hiệu chỉnh"), { text: t("Main model", "Mô hình chính"), options: { bold: true } }],
    ["Gradient-boosted trees", t("Sequential trees fix errors", "Cây sau sửa lỗi cây trước"), t("Often slightly more accurate", "Thường chính xác hơn chút"), t("Sequential, easier to overfit", "Train tuần tự, dễ overfit"), t("Future work", "Hướng mở rộng")],
    ["LSTM", t("Recurrent network on sequences", "Mạng hồi quy trên chuỗi"), t("Long sequences", "Chuỗi dài"), t("GPU, opaque, beyond 3 weeks", "Cần GPU, khó giải thích, quá 3 tuần"), t("Out of scope", "Ngoài phạm vi")],
  ], { x: MX, y: 1.35, w: W - 2 * MX, colW: [2.1, 2.9, 3.3, 2.43, 1.4], fontSize: 12 });
  source(S, C, t("We do not claim RF is the most accurate model possible — only that it beats the baselines and fits Spark ML.",
    "Nhóm không khẳng định RF là mô hình chính xác nhất — chỉ là nó hơn baseline và phù hợp Spark ML."));
},

// training protocol
(IC) => {
  const man = D.ablation.manifest;
  const S = slide("CONTENT", SEC(), t("ML: training and evaluation protocol", "ML: quy trình train và đánh giá"));
  const y0 = man.years[0], sx = (y) => MX + ((y - y0) / (2026 - y0)) * (W - 2 * MX);
  [["Train", y0, 2023, "accent1"], ["Validation", 2023, 2024, "accent5"], ["Test", 2024, 2026, "accent2"]].forEach(([k, a, b, col]) =>
    S.addText(a === b - 1 ? `${k}  ${a}` : `${k}  ${a}–${b - 1}`, { shape: "rect", x: sx(a), y: 1.45, w: sx(b) - sx(a) - 0.04, h: 0.55,
      fill: { color: C[col] }, color: C.background1, fontSize: 15, bold: true, align: "center", valign: "middle", margin: 0, isTextBox: true }));
  S.addText(t(`Rows whose label window crosses a split boundary are purged. Pilot data starts in ${y0}; the official run trains on 2005–2022.`,
    `Dòng có cửa sổ nhãn lấn qua ranh giới split bị loại. Dữ liệu pilot bắt đầu từ ${y0}; bản chính thức train trên 2005–2022.`),
    { x: MX, y: 2.1, w: W - 2 * MX, h: 0.45, fontSize: 13, italic: true, color: C.text1, margin: 0, isTextBox: true });
  [[t("Imbalance", "Mất cân bằng"), t(`weightCol: positives × ${f(man.class_weight_positive, 1)} (= N0/N1 on train)`, `weightCol: lớp dương × ${f(man.class_weight_positive, 1)} (= N0/N1 trên train)`)],
   [t("Hyper-parameters", "Siêu tham số"), t(`${man.model.numTrees} trees, depth ${man.model.maxDepth}, minInstances ${man.model.minInstancesPerNode}, seed ${man.seed} — identical for M1/M2/M3`,
     `${man.model.numTrees} cây, độ sâu ${man.model.maxDepth}, minInstances ${man.model.minInstancesPerNode}, seed ${man.seed} — giống nhau cho M1/M2/M3`)],
   [t("Threshold", "Ngưỡng"), t("Max F1 on validation 2023, frozen before touching test", "F1 cao nhất trên validation 2023, khóa lại trước khi chạm test")],
   [t("Metrics", "Thước đo"), t("PR-AUC (primary), recall, precision, F1, confusion matrix, ROC-AUC", "PR-AUC (chính), recall, precision, F1, confusion matrix, ROC-AUC")],
   [t("Uncertainty", "Độ bất định"), t(`Block bootstrap by day (${man.bootstrap.n_resamples} resamples), paired → 95% CI of differences`,
     `Bootstrap theo khối ngày (${man.bootstrap.n_resamples} lần), ghép cặp → khoảng tin cậy 95% của chênh lệch`)],
   [t("Breakdowns", "Chia nhóm"), t("By region and by region-specific rainy season", "Theo vùng và theo mùa mưa của từng vùng")]].forEach(([k, v], i) => {
    const y = 2.75 + i * 0.6;
    S.addText(k, { x: MX, y, w: 2.4, h: 0.5, fontSize: 14, bold: true, color: C.text2, margin: 0, valign: "middle", isTextBox: true });
    S.addText(v, { x: MX + 2.5, y, w: W - 2 * MX - 2.5, h: 0.5, fontSize: 14, color: C.text1, margin: 0, valign: "middle", isTextBox: true });
  });
  source(S, C, t("Code: src/experiments/ablation.py, metrics.py · manifest: reports/ablation_manifest.json", "Code: src/experiments/ablation.py, metrics.py · manifest: reports/ablation_manifest.json"));
},

// streaming
(IC) => {
  const yg = D.yagi || [], mc = D.mongo ? D.mongo.counts : {};
  const S = slide("CONTENT", SEC(), t("Structured Streaming: replaying Typhoon Yagi (Sep 2024)", "Structured Streaming: phát lại bão Yagi (09/2024)"));
  [[t("Replay producer", "Bộ phát lại"), t("silver 01–10/09/2024 → 1 file per hour, write .tmp then rename", "silver 01–10/09/2024 → 1 file mỗi giờ, ghi .tmp rồi đổi tên")],
   ["stream_in/", t("JSON file source, fixed schema, maxFilesPerTrigger 6", "file source JSON, schema cố định, maxFilesPerTrigger 6")],
   ["foreachBatch", t("dedup (cell, hour) → append to 96 h buffer → point_features() (same code as batch)", "lọc trùng (ô, giờ) → nối vào bộ đệm 96 h → point_features() (cùng code batch)")],
   [t("RF model (frozen)", "Model RF (cố định)"), t(`${SEL} selected by the ablation + its 2023 threshold; no refit on 2024`, `${SEL} được chọn từ ablation + ngưỡng 2023; không train lại trên 2024`)],
   [t("MongoDB upsert", "Upsert MongoDB"), t("_id = point|hour|model|replay → idempotent", "_id = điểm|giờ|model|replay → ghi lại không trùng")]].forEach(([a, b], i) => {
    const y = 1.35 + i * 0.98;
    S.addShape("roundRect", { x: MX, y, w: 6.3, h: 0.82, rectRadius: 0.08, fill: { color: C.background2 }, line: { color: C.accent1, width: 1 } });
    S.addText([{ text: a, options: { bold: true, fontSize: 14, breakLine: true } }, { text: b, options: { fontSize: 12 } }],
      { x: MX + 0.15, y: y + 0.04, w: 6.0, h: 0.74, color: C.text1, margin: 0, valign: "middle", isTextBox: true });
    if (i < 4) arrow(S, C, MX + 3.15, y + 0.82, MX + 3.15, y + 0.98, { width: 1.5 });
  });
  table(S, C, [[t("Evidence", "Bằng chứng"), t("Value", "Giá trị")],
    [t("Files replayed", "File đã phát"), n(D.stream.files)], [t("Micro-batches committed", "Micro-batch đã commit"), n(D.stream.checkpoint_commits)],
    [t("Warm-up before first prediction", "Làm nóng trước dự báo đầu"), t("72 h (01–03/09)", "72 h (01–03/09)")],
    [t("Predictions in MongoDB", "Dự báo trong MongoDB"), n(mc.predictions || 0)],
    [t("Re-run with same checkpoint", "Chạy lại cùng checkpoint"), t("0 files reprocessed", "0 file xử lý lại")],
    [t("Back-test rows with known label", "Dòng hậu kiểm có nhãn"), n(yg.length)]], { x: 7.3, y: 1.35, w: 5.43, colW: [3.4, 2.03], fontSize: 13 });
  card(S, C, 7.3, 4.75, 5.43, 1.55, { title: t("Why no watermark / Kafka", "Vì sao không watermark / Kafka"), titleSize: 14, body:
    t("No stateful aggregation inside Spark: the rolling state is the history buffer keyed by (cell, hour). A file source gives the same contract as a topic without another service.",
      "Không có phép gộp có trạng thái trong Spark: trạng thái là bộ đệm lịch sử theo (ô, giờ). File source cho cùng cam kết như topic mà không cần thêm dịch vụ."), bodySize: 12 });
  source(S, C, t("Code: src/streaming/ · test: tests/test_stream_parity.py", "Code: src/streaming/ · test: tests/test_stream_parity.py"));
},

// MongoDB
(IC) => {
  const mg = D.mongo || {};
  const S = slide("CONTENT", SEC(), t("Serving: MongoDB data model and read-back", "Phục vụ: mô hình dữ liệu MongoDB và đọc lại"));
  code(S, MX, 1.35, 5.6, 3.3, JSON.stringify(Object.fromEntries(Object.entries(mg.sample_prediction || {})
    .filter(([k]) => ["_id", "location_id", "new_province_code", "prediction_time", "heavy_rain_probability", "decision_threshold",
      "is_heavy_rain_predicted", "model_version", "run_type"].includes(k))
    .map(([k, v]) => [k, typeof v === "number" ? Number(v.toFixed(3)) : v])), null, 1), 10.5);
  S.addText(t("Real document read back after the Yagi replay", "Document thật đọc lại sau khi replay Yagi"), { x: MX, y: 4.7, w: 5.6, h: 0.3,
    fontSize: 11, italic: true, color: C.accent4, margin: 0, isTextBox: true });
  const st = (q) => (mg[q] || {}).plan_stages ? mg[q].plan_stages.join(" ← ") : "–";
  table(S, C, [[t("User query", "Truy vấn"), t("Collection · index", "Collection · index"), "Plan"],
    [t("Provinces on alert at hour t, by risk", "Tỉnh đang cảnh báo tại giờ t, theo rủi ro"), "province_risk · (prediction_time, score↓)", st("q1_alerts_at_time")],
    [t("Probability series for one point", "Chuỗi xác suất của một điểm"), "predictions · (location_id, prediction_time)", st("q2_location_series")],
    [t("Alert hours per province", "Số giờ cảnh báo theo tỉnh"), "province_risk · $group", "aggregation"]],
    { x: 6.5, y: 1.35, w: 6.23, colW: [2.3, 2.6, 1.33], fontSize: 12 });
  card(S, C, 6.5, 3.6, 6.23, 2.7, { title: t("Why MongoDB (and not only Parquet)", "Vì sao MongoDB (không chỉ Parquet)"), titleSize: 15, body: bullets([
    t("Parquet is for scans; users need key lookups in milliseconds", "Parquet để quét dữ liệu; người dùng cần tra theo khóa trong mili giây"),
    t("One self-contained document per (point, hour): no joins at read time", "Mỗi (điểm, giờ) là một document đủ thông tin: không cần join khi đọc"),
    t("Upsert by deterministic _id → Spark retries never duplicate", "Upsert theo _id cố định → Spark chạy lại không sinh bản trùng"),
    t("CAP: a single-primary replica set is CP — w: majority writes survive failover; reads wait ~10 s during an election", "CAP: replica set một primary là CP — ghi w: majority không mất khi failover; đọc chờ ~10 s lúc bầu primary"),
  ], 12.5) });
  source(S, C, t("Code: src/serving/query_mongo.py · evidence: reports/mongo/read_back.json", "Code: src/serving/query_mongo.py · bằng chứng: reports/mongo/read_back.json"));
},

// deployment
(IC) => {
  const S = slide("CONTENT", SEC(), t("Cluster and cloud deployment (design)", "Triển khai trên cụm và cloud (thiết kế)"));
  pill(S, C, W - MX - 1.15, 0.55, "proposed");
  [[t("Cluster manager", "Quản lý cụm"), t("Spark on YARN (course cluster) or Kubernetes; managed: Dataproc / EMR", "Spark trên YARN (cụm lab) hoặc Kubernetes; dịch vụ quản lý: Dataproc / EMR")],
   [t("Driver / executors", "Driver / executor"), t("Driver in cluster mode; executors 4 cores × 8 GB; dynamic allocation for batch, 2 fixed executors for streaming",
     "Driver ở cluster mode; executor 4 core × 8 GB; cấp phát động cho batch, 2 executor cố định cho streaming")],
   [t("Storage path", "Lưu trữ"), t("HDFS or object store (s3a://) for the lake; checkpoints on HDFS; MongoDB 3-node replica set", "HDFS hoặc object store (s3a://) cho lake; checkpoint trên HDFS; MongoDB replica set 3 node")],
   [t("Scaling", "Mở rộng"), t("More cells/years = more partitions and tasks, same code; partition by year, repartition by point for windows", "Thêm ô/năm = thêm partition và task, cùng code; chia theo năm, repartition theo điểm cho window")],
   [t("Fault tolerance", "Chịu lỗi"), t("Lineage recomputation, HDFS replication 3, streaming checkpoint + idempotent upsert, Mongo w: majority", "Tính lại theo lineage, HDFS replication 3, checkpoint streaming + upsert không trùng, Mongo w: majority")],
   [t("Observability", "Giám sát"), t("Spark UI / History Server, YARN UI, job counters, collect_log + quality reports", "Spark UI / History Server, YARN UI, counter của job, collect_log + báo cáo chất lượng")],
   [t("Security", "Bảo mật"), t("No credentials in repo; Kerberos/IAM on the cluster; Mongo auth + TLS; data is CC BY 4.0", "Không để credential trong repo; Kerberos/IAM trên cụm; Mongo auth + TLS; dữ liệu CC BY 4.0")],
   [t("Cost / performance", "Chi phí / hiệu năng"), t("Batch on spot workers (cheap, restartable); streaming on on-demand nodes (must stay up)", "Batch chạy trên máy spot (rẻ, chạy lại được); streaming trên máy on-demand (phải chạy liên tục)")]]
    .forEach(([k, v], i) => {
      const y = 1.35 + i * 0.63;
      S.addShape("roundRect", { x: MX, y, w: W - 2 * MX, h: 0.55, rectRadius: 0.06, fill: { color: i % 2 ? C.background1 : C.background2 }, line: { color: C.background2 } });
      S.addText(k, { x: MX + 0.15, y, w: 2.9, h: 0.55, fontSize: 14, bold: true, color: C.text2, margin: 0, valign: "middle", isTextBox: true });
      S.addText(v, { x: MX + 3.1, y, w: W - 2 * MX - 3.25, h: 0.55, fontSize: 13, color: C.text1, margin: 0, valign: "middle", isTextBox: true });
    });
},
];
