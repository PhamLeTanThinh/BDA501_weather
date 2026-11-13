// Các slide từ Structured Streaming trở đi, vẽ lại: luồng / hình / chữ lớn, chi tiết đưa xuống notes cho người trình bày.
const fs = require("fs"), path = require("path");
const K = require("./common");
const { pres, C, D, A, HEX, W, MX, t, f, pct, n, slide, source, pill, arrow, CT, SEL } = K;
const O = JSON.parse(fs.readFileSync(path.join(K.ROOT, "reports", "official", "deck_official.json"), "utf8"));
const AB = O.ablation, R = (m) => AB.results.find((r) => r.model === m && r.split === "test"), DL = (c) => AB.deltas.find((d) => d.comparison === c);
const SA = () => t("Architecture & pipeline", "Kiến trúc & pipeline"), SE = () => t("Experiments & results", "Thực nghiệm & kết quả");
const mil = (x) => t(`${f(x / 1e6, 1)} M`, `${f(x / 1e6, 1).replace(".", ",")} triệu`);
// kết quả lượt streaming (Kafka, M4): đo lường + parity, và dữ liệu cho slide Yagi
const rd = (f) => { const q = path.join(K.ROOT, "reports", "streaming", f); return fs.existsSync(q) ? JSON.parse(fs.readFileSync(q, "utf8")) : null; };
const SR = rd("stream_report.json"), SD = rd("stream_deck.json");
// thí nghiệm hiệu năng có kiểm soát (perf_official): summary.csv + environment.json
const PF = (() => {
  const d = path.join(K.ROOT, "reports", "official", "performance");
  if (!fs.existsSync(path.join(d, "summary.csv"))) return null;
  const [h, ...r] = fs.readFileSync(path.join(d, "summary.csv"), "utf8").trim().split(/\r?\n/);
  const k = h.split(",");
  const sum = r.map((l) => Object.fromEntries(l.split(",").map((v, i) => [k[i], isNaN(+v) || v === "" ? v : +v])));
  sum.forEach((x) => { x.setting = String(x.setting); });
  return { sum, env: JSON.parse(fs.readFileSync(path.join(d, "environment.json"), "utf8")) };
})();
// so sánh ba thuật toán (Logistic regression, Random Forest, GBT) trên cùng dữ liệu M4
const MC = (() => {
  const d = path.join(K.ROOT, "reports", "official", "model"), csv = (f) => fs.readFileSync(path.join(d, f), "utf8").trim().split(/\r?\n/);
  if (!fs.existsSync(path.join(d, "model_compare_manifest.json"))) return null;
  const rows = (f) => { const [h, ...r] = csv(f); const k = h.split(","); return r.map((l) => Object.fromEntries(l.split(",").map((v, i) => [k[i], isNaN(+v) || v === "" ? v : +v]))); };
  return { man: JSON.parse(fs.readFileSync(path.join(d, "model_compare_manifest.json"), "utf8")), res: rows("model_compare_results.csv"), del: rows("model_compare_deltas.csv") };
})();
const PILOT = () => t(`Pilot demo: 12 cities, model ${SEL}`, `Demo pilot: 12 thành phố, model ${SEL}`);

// Một dòng chữ to: tiêu đề câu hỏi / bài toán của slide.
function lead(S, text, y = 1.2) {
  S.addText(text, { x: MX, y, w: W - 2 * MX, h: 0.55, fontSize: 20, bold: true, color: C.text2, margin: 0, isTextBox: true, fit: "shrink" });
}
// Khối số lớn + chú thích ngắn.
function big(S, x, y, w, value, caption, color = C.accent2, size = 36) {
  S.addText(value, { x, y, w, h: size / 55, fontSize: size, bold: true, color, margin: 0, valign: "bottom", isTextBox: true, fit: "shrink" });
  S.addText(caption, { x, y: y + size / 55 + 0.05, w, h: 0.55, fontSize: 13, color: C.text1, margin: 0, valign: "top", isTextBox: true });
}
// Luồng ngang: phần tử { can: true } là kho dữ liệu (trụ), còn lại là bước xử lý; num = số liệu thật dưới khung.
function flow(S, y, nodes, hb = 1.35) {
  const gap = 0.3, cw = 1.55, nc = nodes.filter((d) => d.can).length, nb = nodes.length - nc;
  const bw = (W - 2 * MX - nc * cw - gap * (nodes.length - 1)) / nb;
  let x = MX, k = 0;
  const xs = [];
  nodes.forEach((d, i) => {
    const w = d.can ? cw : bw;
    if (d.can) {
      S.addText([{ text: d.head, options: { bold: true, fontSize: 13, breakLine: true } }, { text: d.sub || "", options: { fontSize: 10 } }],
        { shape: "can", x, y: y - 0.1, w, h: hb + 0.2, fill: { color: C.accent1 }, line: { color: C.accent1 }, color: C.background1,
          align: "center", valign: "bottom", margin: [0, 0.05, 0.12, 0.05], isTextBox: true });
    } else {
      k += 1;
      S.addShape("roundRect", { x, y, w, h: hb, rectRadius: 0.1, fill: { color: C.background1 }, line: { color: C.text2, width: 1.25 } });
      S.addText([{ text: `${k}. ${d.head}`, options: { bold: true, fontSize: 14, color: C.text2, breakLine: true } }, { text: d.sub || "", options: { fontSize: 11, color: C.text1 } }],
        { x: x + 0.1, y: y + 0.06, w: w - 0.2, h: hb - 0.12, align: "center", valign: "middle", margin: 0, isTextBox: true });
    }
    if (d.num) S.addText(d.num, { x: x - 0.15, y: y + hb + 0.15, w: w + 0.3, h: 0.35, fontSize: 12.5, bold: true, color: C.accent2, align: "center", margin: 0, isTextBox: true });
    xs.push([x, w]);
    x += w + gap;
  });
  for (let i = 0; i < xs.length - 1; i++) arrow(S, C, xs[i][0] + xs[i][1] + 0.03, y + hb / 2, xs[i + 1][0] - 0.03, y + hb / 2, { color: C.accent2, width: 1.75 });
  return xs;
}
const note = (S, en, vi) => S.addNotes(t(en, vi));

module.exports = {
// ---------------- Streaming: kết quả chạy thử với dữ liệu rác và tắt job giữa chừng
streaming: (IC) => {
  const S = slide("CONTENT", SA(), t("Structured Streaming: dirty data and a crash, handled", "Structured Streaming: dữ liệu bẩn và sự cố giữa chừng"));
  lead(S, t("Replay Typhoon Yagi through Kafka with injected junk, kill Spark mid-run, then check every number",
    "Phát lại bão Yagi qua Kafka kèm dữ liệu rác, tắt Spark giữa chừng, rồi kiểm tra từng con số"));
  if (!SR) return;
  const J = Object.fromEntries(SR.junk.map((r) => [r.case, r]));
  const rows = [
    [t("Duplicates", "Bản ghi trùng"), J["duplicate (immediate + later)"], t("watermark + dedup", "watermark + khử trùng")],
    [t("Late ≤ 6 h, out of order", "Trễ ≤ 6 giờ, sai thứ tự"), J["late within 6 h + out of order"], t("accepted, recomputed", "nhận, tính lại")],
    [t("Late > 6 h", "Trễ > 6 giờ"), J["late beyond 6 h"], t("dropped by watermark", "watermark bỏ")],
    [t("Broken records", "Bản ghi hỏng"), { injected: ["malformed JSON", "missing field", "out-of-range value", "unknown cell"].reduce((a, k) => a + J[k].injected, 0),
      handled: ["malformed JSON", "missing field", "out-of-range value", "unknown cell"].reduce((a, k) => a + J[k].handled, 0) }, t("quarantined", "cách ly")],
    [t("Missing hours (5 cells)", "Thiếu giờ (5 ô)"), J["missing hours (never sent)"], t("no forecast, never 0 mm", "không dự báo, không coi là 0")],
  ];
  const x0 = MX, cw = [3.0, 1.35, 1.35, 2.3], y0 = 1.95;
  [t("Case", "Tình huống"), t("Injected", "Chèn vào"), t("Spark", "Spark"), t("Handling", "Cách xử lý")].forEach((h, j) =>
    S.addText(h, { x: x0 + cw.slice(0, j).reduce((a, b) => a + b, 0) + (j === 3 ? 0.25 : 0), y: y0, w: cw[j], h: 0.35, fontSize: 12, bold: true, color: C.accent4,
      align: j === 1 || j === 2 ? "right" : "left", margin: 0, isTextBox: true }));
  rows.forEach(([k, r, how], i) => {
    const y = y0 + 0.42 + i * 0.5;
    S.addShape("line", { x: x0, y: y - 0.04, w: cw.reduce((a, b) => a + b, 0), h: 0, line: { color: "DCE5E8", width: 0.75 } });
    [[k, C.text2, true], [n(r.injected), C.text1, false], [r.handled == null ? "–" : n(r.handled), C.accent3, true], [how, C.text1, false]].forEach(([v, col, b], j) =>
      S.addText(v, { x: x0 + cw.slice(0, j).reduce((a, c) => a + c, 0) + (j === 3 ? 0.25 : 0), y, w: cw[j], h: 0.42, fontSize: 13.5, bold: b, color: col,
        align: j === 1 || j === 2 ? "right" : "left", valign: "middle", margin: 0, isTextBox: true }));
  });
  const k = SR.kafka, pr = SR.parity_stream_vs_batch_same_rows;
  const bx = 8.75, bw = W - MX - bx;
  big(S, bx, 1.9, bw, `${n(k.read_by_forecast_query)} = ${n(k.messages_in_topic)}`,
    t("Kafka messages read = in topic, even after killing Spark mid-run: nothing lost, nothing read twice", "bản tin đã đọc = có trong topic, kể cả khi tắt Spark giữa chừng: không mất, không đọc lại"), C.accent3, 26);
  big(S, bx, 3.35, bw, n(pr.cell_hours_both_scored),
    t(`cell-hours where stream = batch on the same rows · ${pr.alert_mismatch} alert differences`, `ô-giờ stream = batch trên cùng dữ liệu · ${pr.alert_mismatch} khác biệt cảnh báo`), C.accent1, 30);
  big(S, bx, 4.8, bw, String(SR.mongo.duplicate_cell_hours),
    t("duplicate forecasts in MongoDB after the restart (upsert by cell|hour)", "dự báo trùng trong MongoDB sau khi chạy lại (upsert theo ô|giờ)"), C.accent3, 30);
  source(S, C, t(`Silver 01–10/09/2024 · ${n(k.messages_in_topic)} Kafka messages · model M4 · reports/streaming/stream_report.md`,
    `Silver 01–10/09/2024 · ${n(k.messages_in_topic)} bản tin Kafka · model M4 · reports/streaming/stream_report.md`));
  const rw = SR.rows;
  note(S, `Producer replays silver hour by hour into Kafka and injects junk without any flag in the message, so Spark must detect it. Duplicates (sent again at once or a few hours later) are removed by withWatermark(6 h) + dropDuplicatesWithinWatermark on (cell, hour). Messages up to 6 hours late or out of order are accepted and the affected hours are recomputed; messages more than 6 hours late are dropped by the watermark, so that hour counts as missing. Malformed JSON, missing fields, impossible values and unknown cells are quarantined in MongoDB. Five cells lose six hours each: their features become empty and no forecast is made, rather than treating the gap as 0 mm. Totals: ${n(rw.sent)} sent, ${n(rw.valid_into_forecast)} clean rows into the forecast, ${n(rw.dropped_total)} removed (${n(rw.rejected_records)} broken, ${n(rw.dropped_duplicates)} duplicates, ${n(rw.dropped_late)} too late). Spark was killed with SIGKILL mid-run and restarted from its checkpoint while the producer kept sending; both queries still read exactly the ${n(k.messages_in_topic)} messages in the topic. Parity: recomputing in batch on exactly the rows the stream accepted gives the same probabilities for all ${n(pr.cell_hours_both_scored)} cell-hours (stored to 4 decimals) and no alert difference. Two counts are not exact by design: Spark's watermark is the max event time of earlier micro-batches minus 6 h, so during bursts (warm-up, the 20-hour burst) some messages more than 6 h late arrive before the watermark has moved and are still accepted — Spark guarantees to keep data less than 6 h late, not to drop everything older. The late-but-accepted count is estimated from Kafka arrival order, whose timestamps have millisecond resolution, so it is approximate; dropped and quarantined counts are exact.`,
    `Producer phát lại silver từng giờ vào Kafka và chèn rác mà không đánh dấu, nên Spark phải tự phát hiện. Bản trùng (gửi lại ngay hoặc sau vài giờ) bị bỏ bằng withWatermark(6 giờ) + dropDuplicatesWithinWatermark theo (ô, giờ). Bản tin trễ tới 6 giờ hoặc sai thứ tự được nhận và các giờ bị ảnh hưởng được tính lại; trễ quá 6 giờ bị watermark bỏ nên giờ đó coi như thiếu. JSON hỏng, thiếu trường, giá trị vô lý và ô không tồn tại bị cách ly vào MongoDB. Năm ô mất mỗi ô sáu giờ: feature của các ô đó để trống và không dự báo, chứ không coi chỗ thiếu là 0 mm. Tổng: gửi ${n(rw.sent)}, ${n(rw.valid_into_forecast)} dòng sạch vào dự báo, loại ${n(rw.dropped_total)} (${n(rw.rejected_records)} hỏng, ${n(rw.dropped_duplicates)} trùng, ${n(rw.dropped_late)} quá hạn). Spark bị tắt bằng SIGKILL giữa chừng rồi chạy lại từ checkpoint trong lúc producer vẫn phát; cả hai truy vấn vẫn đọc đúng ${n(k.messages_in_topic)} bản tin có trong topic. Parity: tính lại bằng batch trên đúng các dòng stream đã nhận cho cùng xác suất ở cả ${n(pr.cell_hours_both_scored)} ô-giờ (lưu 4 chữ số) và không khác cảnh báo nào. Hai con số không khớp tuyệt đối là do bản chất: watermark của Spark = giờ lớn nhất của các micro-batch trước trừ 6 giờ, nên lúc dữ liệu dồn (làm nóng, đợt dồn 20 giờ) một số bản tin trễ hơn 6 giờ đến trước khi watermark kịp tăng và vẫn được nhận — Spark chỉ cam kết giữ dữ liệu trễ dưới 6 giờ, không cam kết bỏ hết dữ liệu trễ hơn. Số bản tin trễ được nhận là ước lượng theo thứ tự đến trong Kafka (timestamp tính theo mili-giây) nên gần đúng; số bị bỏ và bị cách ly thì chính xác.`);
},

// ---------------- MongoDB
mongo: (IC) => {
  const S = slide("CONTENT", SA(), t("Serving: MongoDB data model and the live web page", "Phục vụ: mô hình dữ liệu MongoDB và trang web trực tiếp"));
  lead(S, t("One document per cell and hour, written by upsert, read by the web page every 2 seconds",
    "Mỗi ô, mỗi giờ là một document, ghi bằng upsert, trang web đọc lại mỗi 2 giây"));
  const sp = (SD && SD.sample_doc) || {};
  const x0 = MX, y0 = 2.0, dw = 5.2;
  S.addShape("roundRect", { x: x0, y: y0, w: dw, h: 3.9, rectRadius: 0.1, fill: { color: C.background2 }, line: { color: C.accent1, width: 1.5 } });
  S.addText(t("predictions · one document", "predictions · một document"), { x: x0 + 0.25, y: y0 + 0.15, w: dw - 0.5, h: 0.35, fontSize: 12, bold: true, color: C.accent1, margin: 0, isTextBox: true });
  [["_id", sp._id || "–"], [t("province", "tỉnh"), sp.province || "–"], [t("probability", "xác suất"), sp.p == null ? "–" : f(sp.p, 3)],
   [t("alert", "cảnh báo"), sp.alert ? t("YES", "CÓ") : t("no", "không")], [t("rain next 24 h", "mưa 24 h tới"), sp.observed_24h_mm == null ? "–" : `${f(sp.observed_24h_mm, 1)} mm`],
   [t("revisions", "lần tính"), String(sp.revisions || 1)]].forEach(([k, v], i) => {
    const y = y0 + 0.6 + i * 0.52;
    S.addText(k, { x: x0 + 0.25, y, w: 1.6, h: 0.45, fontSize: 13, color: C.text1, margin: 0, valign: "middle", isTextBox: true });
    S.addText(v, { x: x0 + 1.9, y, w: dw - 2.1, h: 0.45, fontSize: i === 0 ? 13 : 17, bold: true, fontFace: "Consolas", color: i === 3 && sp.alert ? C.accent2 : C.text2, margin: 0, valign: "middle", isTextBox: true, fit: "shrink" });
  });
  [[t("Map: every cell at the latest hour", "Bản đồ: mọi ô ở giờ mới nhất"), "predictions (ts)"],
   [t("Chart: one cell over time", "Biểu đồ: một ô theo thời gian"), "predictions (cell, ts)"],
   [t("Top provinces at hour t", "Tỉnh rủi ro cao ở giờ t"), "province_risk (ts)"],
   [t("Quarantined records", "Bản ghi bị cách ly"), "stream_rejects (capped)"]].forEach(([q, idx], i) => {
    const y = 2.0 + i * 0.98, x = 6.0;
    S.addText(q, { x, y, w: 3.1, h: 0.75, fontSize: 14, bold: true, color: C.text2, margin: 0, valign: "middle", isTextBox: true });
    arrow(S, C, x + 3.15, y + 0.37, x + 3.5, y + 0.37, { color: C.accent2, width: 1.75 });
    S.addText(idx, { shape: "roundRect", rectRadius: 0.08, x: x + 3.55, y: y + 0.1, w: W - MX - x - 3.55, h: 0.55, fill: { color: C.background1 }, line: { color: C.accent1, width: 1.25 },
      fontFace: "Consolas", fontSize: 12, color: C.accent1, bold: true, align: "center", valign: "middle", margin: 0 });
  });
  source(S, C, t("Web: FastAPI + Leaflet map at http://localhost:8000 · document shown: Hai Phong cell, 06/09/2024 19:00, from the stream run",
    "Web: FastAPI + bản đồ Leaflet tại http://localhost:8000 · document trên slide: ô Hải Phòng, 19:00 06/09/2024, từ lượt chạy stream"));
  note(S, "Why MongoDB and not only Parquet: Parquet is built for scans, the web page needs lookups by key every two seconds. Each (cell, hour) is one self-contained document, so no join is needed at read time. Upsert by a deterministic _id = cell|hour means a replayed micro-batch overwrites instead of duplicating; when late data arrives the same document is recomputed and its revisions counter goes up. The web page reads predictions (latest hour for the map, one cell for the chart), province_risk and stream_rejects, plus per-batch counters in batch_stats, reject_stats and progress, summed by batch id so a replayed batch is never counted twice.",
    "Vì sao MongoDB mà không chỉ Parquet: Parquet để quét dữ liệu, trang web cần tra theo khóa mỗi hai giây. Mỗi (ô, giờ) là một document đủ thông tin nên không cần join khi đọc. Upsert theo _id cố định = ô|giờ nên batch chạy lại sẽ ghi đè chứ không nhân đôi; khi dữ liệu trễ tới, cùng document được tính lại và bộ đếm revisions tăng. Trang web đọc predictions (giờ mới nhất cho bản đồ, một ô cho biểu đồ), province_risk và stream_rejects, cùng bộ đếm theo batch trong batch_stats, reject_stats và progress, cộng theo batch id nên batch chạy lại không bao giờ bị đếm hai lần.");
},

// ---------------- Triển khai cụm (thiết kế)
deploy: (IC) => {
  const S = slide("CONTENT", SA(), t("Cluster and cloud deployment (design)", "Triển khai trên cụm và cloud (thiết kế)"));
  pill(S, C, W - MX - 1.15, 0.55, "proposed");
  // khung cụm Spark
  const cx = MX, cy = 1.3, cw = 8.1, ch = 3.3;
  S.addShape("roundRect", { x: cx, y: cy, w: cw, h: ch, rectRadius: 0.1, fill: { color: C.background2 }, line: { color: C.text2, width: 1.25 } });
  S.addText(t("Spark cluster · YARN or Kubernetes (managed: Dataproc / EMR)", "Cụm Spark · YARN hoặc Kubernetes (dịch vụ: Dataproc / EMR)"),
    { x: cx + 0.2, y: cy + 0.1, w: cw - 0.4, h: 0.35, fontSize: 13, bold: true, color: C.text2, margin: 0, isTextBox: true });
  S.addText([{ text: "Driver", options: { bold: true, fontSize: 15, breakLine: true } }, { text: t("cluster mode", "cluster mode"), options: { fontSize: 11 } }],
    { shape: "roundRect", rectRadius: 0.08, x: cx + 0.25, y: cy + 1.1, w: 1.5, h: 1.3, fill: { color: C.text2 }, color: C.background1, align: "center", valign: "middle", margin: 0 });
  const exec = (x, y, col) => S.addText(t("exec\n4 c · 8 GB", "exec\n4 c · 8 GB"), { shape: "roundRect", rectRadius: 0.06, x, y, w: 1.0, h: 0.75, fill: { color: col },
    color: C.background1, fontSize: 10, bold: true, align: "center", valign: "middle", margin: 0 });
  S.addText(t("Batch · spot nodes · dynamic allocation", "Batch · máy spot · cấp phát động"), { x: cx + 2.1, y: cy + 0.55, w: 5.8, h: 0.3, fontSize: 12, bold: true, color: C.accent1, margin: 0, isTextBox: true });
  for (let i = 0; i < 5; i++) exec(cx + 2.1 + i * 1.15, cy + 0.9, C.accent1);
  S.addText(t("Streaming · on-demand nodes · 2 fixed executors", "Streaming · máy on-demand · 2 executor cố định"), { x: cx + 2.1, y: cy + 1.85, w: 5.8, h: 0.3, fontSize: 12, bold: true, color: C.accent2, margin: 0, isTextBox: true });
  for (let i = 0; i < 2; i++) exec(cx + 2.1 + i * 1.15, cy + 2.2, C.accent2);
  arrow(S, C, cx + 1.78, cy + 1.6, cx + 2.05, cy + 1.27, { color: C.text2, width: 1.25 });
  arrow(S, C, cx + 1.78, cy + 1.95, cx + 2.05, cy + 2.55, { color: C.text2, width: 1.25 });
  // kho lưu trữ bên dưới
  const can = (x, w, head, sub) => S.addText([{ text: head, options: { bold: true, fontSize: 13, breakLine: true } }, { text: sub, options: { fontSize: 10 } }],
    { shape: "can", x, y: 5.15, w, h: 1.15, fill: { color: C.accent1 }, line: { color: C.accent1 }, color: C.background1, align: "center", valign: "bottom", margin: [0, 0.05, 0.1, 0.05] });
  can(cx + 0.3, 2.3, t("Lake", "Lake"), "HDFS / s3a://");
  can(cx + 2.9, 2.3, "Checkpoints", t("on HDFS", "trên HDFS"));
  can(cx + 5.5, 2.3, "MongoDB", t("replica set × 3", "replica set × 3"));
  [cx + 1.45, cx + 4.05, cx + 6.65].forEach((x) => arrow(S, C, x, cy + ch + 0.05, x, 5.1, { color: C.accent2, width: 1.5 }));
  // bốn nguyên tắc, mỗi cái một dòng ngắn
  [[t("Scale", "Mở rộng"), t("more cells or years = more tasks, same code", "thêm ô hoặc năm = thêm task, cùng code")],
   [t("Survive failures", "Chịu lỗi"), t("lineage · HDFS × 3 · checkpoint · w: majority", "lineage · HDFS × 3 · checkpoint · w: majority")],
   [t("Watch", "Giám sát"), "Spark UI · History Server · YARN UI",],
   [t("Secure", "Bảo mật"), t("no keys in repo · Kerberos / IAM · TLS", "không để khóa trong repo · Kerberos / IAM · TLS")]].forEach(([h, d], i) => {
    const y = 1.35 + i * 1.22, x = 9.1;
    S.addText([{ text: h, options: { bold: true, fontSize: 16, color: C.text2, breakLine: true } }, { text: d, options: { fontSize: 12, color: C.text1 } }],
      { x, y, w: W - MX - x, h: 1.05, margin: 0, valign: "middle", isTextBox: true });
  });
  note(S, "Design, not yet run. Driver in cluster mode; executors 4 cores × 8 GB. Batch uses dynamic allocation on spot workers: cheap, and a lost worker is simply recomputed from lineage. Streaming keeps 2 fixed executors on on-demand nodes because it must stay up. The lake lives on HDFS or an object store (s3a://), streaming checkpoints on HDFS, MongoDB as a 3-node replica set. Scaling: partition by year, repartition by point for window features. Observability: Spark UI / History Server, YARN UI, job counters and our quality reports. Security: no credentials in the repo, Kerberos or IAM on the cluster, Mongo auth + TLS; ERA5 data is CC BY 4.0.",
    "Đây là thiết kế, chưa chạy. Driver ở cluster mode; executor 4 core × 8 GB. Batch cấp phát động trên máy spot: rẻ, mất máy thì tính lại theo lineage. Streaming giữ 2 executor cố định trên máy on-demand vì phải chạy liên tục. Lake đặt trên HDFS hoặc object store (s3a://), checkpoint streaming trên HDFS, MongoDB là replica set 3 node. Mở rộng: chia theo năm, repartition theo điểm cho feature cửa sổ. Giám sát: Spark UI / History Server, YARN UI, counter của job và báo cáo chất lượng. Bảo mật: không để credential trong repo, Kerberos hoặc IAM trên cụm, Mongo auth + TLS; dữ liệu ERA5 theo giấy phép CC BY 4.0.");
},

// ---------------- Chọn mô hình
modelChoice: (IC) => {
  const S = slide("CONTENT", t("Model building", "Xây dựng mô hình"), t("ML: choosing the model", "ML: chọn mô hình"));
  lead(S, t("Three algorithms trained on the same data; the one with the best validation PR-AUC is kept",
    "Train ba thuật toán trên cùng dữ liệu; giữ thuật toán có PR-AUC validation cao nhất"));
  const mc = (name) => MC && MC.res.find((r) => r.model === name && r.split === "test");
  const tag = (name) => (mc(name) ? t(`trained · test ${f(mc(name).pr_auc)}`, `đã train · test ${f(mc(name).pr_auc)}`) : t("trained", "đã train"));
  const best = MC ? MC.man.best : "Random Forest";
  const M = [["Persistence", t("baseline", "baseline"), t("rained > 50 mm in the last 24 h → yes", "24 h qua > 50 mm → có"), false],
    ["Climatology", t("baseline", "baseline"), t("how often per place and month", "tần suất theo nơi và tháng"), false],
    [t("Logistic reg.", "Hồi quy logistic"), tag("Logistic regression"), t("linear, fast", "tuyến tính, nhanh"), best === "Logistic regression"],
    ["Random Forest", best === "Random Forest" ? t("★ chosen", "★ được chọn") : tag("Random Forest"), t("many trees, averaged votes", "nhiều cây, lấy trung bình phiếu"), best === "Random Forest"],
    [t("Boosted trees", "Boosted trees"), tag("Gradient-boosted trees"), t("trees fix each other's errors", "cây sau sửa lỗi cây trước"), best === "Gradient-boosted trees"],
    ["LSTM", t("out of scope", "ngoài phạm vi"), t("needs GPU, hard to explain", "cần GPU, khó giải thích"), false]];
  const ay = 2.35, gap = 0.2, w = (W - 2 * MX - 5 * gap) / 6;
  arrow(S, C, MX, ay + 1.65, W - MX, ay + 1.65, { color: C.accent4, width: 1.5 });
  S.addText(t("simpler", "đơn giản"), { x: MX, y: ay + 1.72, w: 2, h: 0.3, fontSize: 11, italic: true, color: C.accent4, margin: 0, isTextBox: true });
  S.addText(t("more complex", "phức tạp hơn"), { x: W - MX - 2, y: ay + 1.72, w: 2, h: 0.3, fontSize: 11, italic: true, color: C.accent4, align: "right", margin: 0, isTextBox: true });
  M.forEach(([name, role, how, hl], i) => {
    const x = MX + i * (w + gap), y = hl ? ay - 0.25 : ay, trained = i >= 2 && i <= 4;
    const col = hl ? C.accent2 : trained ? C.accent1 : C.accent4;
    S.addShape("roundRect", { x, y, w, h: hl ? 1.7 : 1.45, rectRadius: 0.1, fill: { color: hl ? C.accent2 : C.background1 }, line: { color: col, width: hl ? 2 : 1.25 } });
    S.addText([{ text: name, options: { bold: true, fontSize: 15, color: hl ? C.background1 : C.text2, breakLine: true } },
      { text: role, options: { bold: true, fontSize: 12, color: hl ? C.background1 : col, breakLine: true } },
      { text: how, options: { fontSize: 11, color: hl ? C.background1 : C.text1 } }],
      { x: x + 0.08, y, w: w - 0.16, h: hl ? 1.7 : 1.45, align: "center", valign: "middle", margin: 0, isTextBox: true });
  });
  [[t("Learns interactions", "Học tương tác"), t("“lots of rain in 72 h AND pressure falling”", "“mưa 72 h nhiều VÀ áp suất đang giảm”")],
   [t("Parallel in Spark ML", "Song song trên Spark ML"), t("each tree trains independently", "mỗi cây train độc lập")],
   [t("Explains itself", "Giải thích được"), t("feature importance per variable", "mức quan trọng của từng biến")]].forEach(([h, d], i) => {
    const x = MX + i * ((W - 2 * MX) / 3);
    S.addText([{ text: h, options: { bold: true, fontSize: 16, color: C.accent2, breakLine: true } }, { text: d, options: { fontSize: 12.5, color: C.text1 } }],
      { x, y: 4.95, w: (W - 2 * MX) / 3 - 0.2, h: 0.85, margin: 0, valign: "middle", isTextBox: true });
  });
  note(S, "Persistence and climatology are the baselines every model must beat. Logistic regression, Random Forest and gradient-boosted trees are all trained on the same M4 features, the same training sample and the same class weight, and compared on validation 2023 (next slide). Random Forest is kept: best validation PR-AUC, trains in parallel in Spark ML and gives feature importance. LSTM needs a GPU, is hard to explain and does not fit the project time.",
    "Persistence và climatology là baseline mà mọi model phải vượt. Hồi quy logistic, Random Forest và gradient-boosted trees đều được train trên cùng feature M4, cùng mẫu train và cùng trọng số lớp, rồi so trên validation 2023 (slide sau). Giữ Random Forest: PR-AUC validation cao nhất, train song song trên Spark ML và có feature importance. LSTM cần GPU, khó giải thích và không vừa thời gian dự án.");
},

// ---------------- So sánh ba thuật toán (cùng dữ liệu M4)
modelCompare: (IC) => {
  if (!MC) return;
  const S = slide("CONTENT", t("Model building", "Xây dựng mô hình"), t("ML: three algorithms on the same data", "ML: ba thuật toán trên cùng dữ liệu"));
  const names = ["Logistic regression", "Random Forest", "Gradient-boosted trees"];
  const lbl = { "Logistic regression": t("Logistic regression", "Hồi quy logistic"), "Random Forest": "Random Forest", "Gradient-boosted trees": t("Gradient-boosted trees", "Gradient-boosted trees") };
  const r = (m, sp) => MC.res.find((x) => x.model === m && x.split === sp);
  const best = MC.man.best;
  S.addText(t("PR-AUC (higher is better) · chosen on validation 2023, then reported on test 2024–2025", "PR-AUC (càng cao càng tốt) · chọn trên validation 2023, rồi báo cáo trên test 2024–2025"),
    { x: MX, y: 1.25, w: 8.2, h: 0.4, fontSize: 14, bold: true, color: C.text2, margin: 0, isTextBox: true });
  const bx = MX + 2.55, sc = 5.1 / 0.45;
  names.forEach((m, i) => {
    const y = 1.85 + i * 1.3, hl = m === best;
    S.addText([{ text: lbl[m], options: { bold: true, fontSize: 14, color: hl ? C.accent2 : C.text2, breakLine: true } },
      { text: r(m, "test").train_seconds ? t(`train ${f(r(m, "test").train_seconds, 0)} s`, `train ${f(r(m, "test").train_seconds, 0)} s`) : t("100 trees", "100 cây"), options: { fontSize: 11, color: C.text1 } }],
      { x: MX, y, w: 2.45, h: 0.95, margin: 0, valign: "middle", isTextBox: true });
    [["validation", 0, C.accent5], ["test", 0.48, hl ? C.accent2 : C.accent1]].forEach(([sp, dy, col]) => {
      const v = r(m, sp).pr_auc;
      S.addShape("rect", { x: bx, y: y + dy + 0.05, w: v * sc, h: 0.36, fill: { color: col }, line: { color: col } });
      S.addText(`${f(v)}  ${sp === "validation" ? "val" : "test"}`, { x: bx + v * sc + 0.08, y: y + dy, w: 1.4, h: 0.46, fontSize: 12.5, bold: true, color: col, valign: "middle", margin: 0, isTextBox: true });
    });
  });
  const dl = (c) => MC.del.find((d) => d.comparison === c);
  const a = dl("Random Forest - Logistic regression"), b = dl("Gradient-boosted trees - Random Forest");
  const sg = (x) => `${x >= 0 ? "+" : "−"}${f(Math.abs(x), 3)}`;
  big(S, 9.1, 1.3, W - MX - 9.1, best === "Random Forest" ? "Random Forest" : lbl[best], t(`kept: best validation PR-AUC (${f(r(best, "validation").pr_auc)})`, `được giữ: PR-AUC validation cao nhất (${f(r(best, "validation").pr_auc)})`), C.accent2, 30);
  big(S, 9.1, 2.95, W - MX - 9.1, sg(a.bootstrap_mean), t(`RF vs logistic on test · CI ${sg(a.ci_lo)} to ${sg(a.ci_hi)}: a real gain`, `RF so với logistic trên test · KTC ${sg(a.ci_lo)} đến ${sg(a.ci_hi)}: hơn thật`), C.accent3, 32);
  big(S, 9.1, 4.55, W - MX - 9.1, sg(b.bootstrap_mean), t(`boosted trees vs RF · CI ${sg(b.ci_lo)} to ${sg(b.ci_hi)}: no real difference`, `boosted trees so với RF · KTC ${sg(b.ci_lo)} đến ${sg(b.ci_hi)}: không khác biệt thật`), C.accent4, 32);
  source(S, C, t(`Same M4 features, same training sample (${n(MC.man.train_rows)} rows, weight ×${f(MC.man.class_weight_positive, 2)}), same validation and test · reports/official/model/model_compare_*.csv`,
    `Cùng feature M4, cùng mẫu train (${n(MC.man.train_rows)} dòng, trọng số ×${f(MC.man.class_weight_positive, 2)}), cùng validation và test · reports/official/model/model_compare_*.csv`));
  const tr = names.map((m) => `${m}: validation ${f(r(m, "validation").pr_auc)}, test ${f(r(m, "test").pr_auc)}, test recall ${pct(r(m, "test").recall)}, precision ${pct(r(m, "test").precision)}`).join("; ");
  note(S, `All three algorithms use the 33 M4 features, the same training sample and class weight, and the same split. The model is chosen only on validation 2023, so the test years stay untouched. ${tr}. Random Forest beats logistic regression by ${sg(a.bootstrap_mean)} PR-AUC on test with a 95% CI entirely above zero (block bootstrap by day). Gradient-boosted trees are ${sg(b.bootstrap_mean)} versus Random Forest with a CI that crosses zero, so the two tree models are statistically tied; Random Forest is kept because it is best on validation and trains in parallel. Settings: logistic regression standardized, L2 1e-4, 100 iterations; Random Forest 100 trees, depth 12; boosted trees 100 rounds, depth 6, step 0.1.`,
    `Cả ba thuật toán dùng 33 feature M4, cùng mẫu train và trọng số lớp, cùng cách chia dữ liệu. Model chỉ được chọn trên validation 2023, nên các năm test không bị đụng tới. ${tr}. Random Forest hơn hồi quy logistic ${sg(a.bootstrap_mean)} PR-AUC trên test, khoảng tin cậy 95% nằm hẳn trên 0 (bootstrap theo khối ngày). Gradient-boosted trees chênh ${sg(b.bootstrap_mean)} so với Random Forest với khoảng tin cậy cắt qua 0, nên hai model cây ngang nhau về mặt thống kê; giữ Random Forest vì tốt nhất trên validation và train song song. Tham số: logistic chuẩn hóa feature, L2 1e-4, 100 vòng lặp; Random Forest 100 cây, độ sâu 12; boosted trees 100 vòng, độ sâu 6, bước 0,1.`);
},

// ---------------- Quy trình train / đánh giá (bản chính thức)
protocol: (IC) => {
  const man = AB.manifest, co = Object.fromEntries(man.cohort.map((c) => [c.split, c])), ts = man.train_sample;
  const S = slide("CONTENT", t("Model building", "Xây dựng mô hình"), t("ML: training and evaluation protocol", "ML: quy trình train và đánh giá"));
  const sx = (y) => MX + ((y - 2015) / 11) * (W - 2 * MX);
  [["Train", 2015, 2023, C.accent1, co.train], ["Validation", 2023, 2024, C.accent5, co.validation], ["Test", 2024, 2026, C.accent2, co.test]].forEach(([k, a, b, col, c]) => {
    S.addText(a === b - 1 ? `${k === "Validation" ? "Val" : k} ${a}` : `${k}  ${a}–${b - 1}`, { shape: "rect", x: sx(a), y: 1.3, w: sx(b) - sx(a) - 0.04, h: 0.55,
      fill: { color: col }, color: C.background1, fontSize: 15, bold: true, align: "center", valign: "middle", margin: 0 });
    S.addText(k === "Validation" ? `${mil(c.rows)} · ${pct(c.positives / c.rows, 2)}` : t(`${mil(c.rows)} rows · ${pct(c.positives / c.rows, 2)} heavy`, `${mil(c.rows)} dòng · ${pct(c.positives / c.rows, 2)} mưa lớn`),
      { x: sx(a) - 0.1, y: 1.9, w: sx(b) - sx(a) + 0.16, h: 0.3, fontSize: 11, color: C.text1, align: "center", margin: 0, isTextBox: true });
  });
  flow(S, 2.85, [
    { head: t("Sample train", "Lấy mẫu train"), sub: t(`all heavy-rain rows + ${pct(man.train_sample.neg_frac)} of the others`, `mọi dòng mưa lớn + ${pct(man.train_sample.neg_frac)} dòng còn lại`), num: t(`${mil(ts.rows)} rows · weight ×${f(ts.class_weight_positive, 2)}`, `${mil(ts.rows)} dòng · trọng số ×${f(ts.class_weight_positive, 2).replace(".", ",")}`) },
    { head: t("Train 3 models", "Train 3 model"), sub: t("logistic · Random Forest · boosted trees, same data", "logistic · Random Forest · boosted trees, cùng dữ liệu"), num: t(`${man.cells} cells`, `${man.cells} ô`) },
    { head: t("Pick model + threshold", "Chọn model + ngưỡng"), sub: t("best PR-AUC, then best F1, on validation 2023", "PR-AUC rồi F1 cao nhất trên validation 2023"), num: t(`M4: ${f(man.thresholds_from_validation.M4, 2)}`, `M4: ${f(man.thresholds_from_validation.M4, 2)}`) },
    { head: t("Test once", "Test một lần"), sub: t("PR-AUC on 2024–2025 + bootstrap by day", "PR-AUC trên 2024–2025 + bootstrap theo ngày"), num: t(`${man.bootstrap.n_resamples} resamples → 95% CI`, `${man.bootstrap.n_resamples} lần → khoảng tin cậy 95%`) },
  ], 1.45);
  S.addText([{ text: t("Rule: ", "Nguyên tắc: "), options: { bold: true, color: C.accent1 } },
    { text: t("test years are never seen while training or choosing the threshold.", "các năm test không được nhìn khi train hay khi chọn ngưỡng.") }],
    { x: MX, y: 5.2, w: W - 2 * MX, h: 0.45, fontSize: 15, color: C.text1, margin: 0, isTextBox: true });
  note(S, `Rows whose 24-hour label window crosses a split boundary are dropped. Heavy rain is about 1% of rows, so training keeps every positive and ${pct(ts.neg_frac)} of the negatives (${n(ts.rows)} rows) and weights positives ×${f(ts.class_weight_positive, 2)} to rebalance. Same hyper-parameters for every feature set (${man.model.numTrees} trees, depth ${man.model.maxDepth}, minInstances ${man.model.minInstancesPerNode}, subsampling ${man.model.subsamplingRate}). Metrics: PR-AUC is primary, plus precision, recall, F1, confusion matrix and ROC-AUC. Uncertainty: block bootstrap by whole days, ${man.bootstrap.n_resamples} resamples, paired between models → 95% CI of the difference. Results are also broken down by region.`,
    `Dòng có cửa sổ nhãn 24 giờ lấn qua ranh giới split bị loại. Mưa lớn chỉ khoảng 1% số dòng, nên khi train giữ mọi dòng dương và ${pct(ts.neg_frac)} dòng âm (${n(ts.rows)} dòng), nhân trọng số dòng dương ×${f(ts.class_weight_positive, 2)} để cân bằng lại. Mọi bộ feature dùng cùng siêu tham số (${man.model.numTrees} cây, độ sâu ${man.model.maxDepth}, minInstances ${man.model.minInstancesPerNode}, subsampling ${man.model.subsamplingRate}). Thước đo: PR-AUC là chính, kèm precision, recall, F1, confusion matrix và ROC-AUC. Độ bất định: bootstrap theo khối ngày, ${man.bootstrap.n_resamples} lần, ghép cặp giữa các model → khoảng tin cậy 95% của chênh lệch. Kết quả còn được chia theo vùng.`);
},

// ---------------- E1: kết quả chính thức trên test
e1: (IC) => {
  const m4 = R("M4"), per = R("persistence_rain24h"), d = DL("M4 - M1"), prev = m4.positives / m4.n;
  const S = slide("CONTENT", SE(), t("E1 · The final model M4 clearly beats the baselines", "E1 · Model cuối M4 vượt rõ các baseline"));
  S.addText(t("PR-AUC on test 2024–2025 (higher is better)", "PR-AUC trên test 2024–2025 (càng cao càng tốt)"), { x: MX, y: 1.3, w: 7.4, h: 0.35, fontSize: 14, bold: true, color: C.text2, margin: 0, isTextBox: true });
  const bars = [[t("random guess", "đoán ngẫu nhiên"), prev, C.accent4], ["persistence", per.pr_auc, C.accent4],
    [t("M1 local weather", "M1 thời tiết tại chỗ"), R("M1").pr_auc, C.accent1], [t("M2 + terrain", "M2 + địa hình"), R("M2").pr_auc, C.accent1],
    [t("M3 + neighbours", "M3 + ô lân cận"), R("M3").pr_auc, C.accent1], [t("M4 + atmosphere", "M4 + khí quyển"), m4.pr_auc, C.accent2]];
  const bx = MX + 2.3, bwMax = 4.4, sc = bwMax / 0.5;
  bars.forEach(([name, v, col], i) => {
    const y = 1.85 + i * 0.72;
    S.addText(name, { x: MX, y, w: 2.2, h: 0.5, fontSize: 13, bold: i === 5, color: C.text1, align: "right", valign: "middle", margin: 0, isTextBox: true });
    S.addShape("rect", { x: bx, y: y + 0.05, w: Math.max(0.04, v * sc), h: 0.4, fill: { color: col }, line: { color: col } });
    S.addText(f(v), { x: bx + Math.max(0.04, v * sc) + 0.1, y, w: 0.9, h: 0.5, fontSize: 14, bold: true, color: col, valign: "middle", margin: 0, isTextBox: true });
  });
  big(S, 8.4, 1.4, 4.3, f(m4.pr_auc), t(`PR-AUC of M4 · 95% CI ${f(m4.pr_auc_ci_lo, 2)}–${f(m4.pr_auc_ci_hi, 2)} · ${f(m4.pr_auc / per.pr_auc, 1)}× persistence`,
    `PR-AUC của M4 · KTC 95% ${f(m4.pr_auc_ci_lo, 2)}–${f(m4.pr_auc_ci_hi, 2)} · gấp ${f(m4.pr_auc / per.pr_auc, 1)} lần persistence`), C.accent2, 48);
  big(S, 8.4, 3.05, 4.3, `+${f(d.delta_pr_auc, 3)}`, t(`atmosphere over M1 · CI ${f(d.ci_lo, 3)} to ${f(d.ci_hi, 3)}, above 0`, `khí quyển so với M1 · KTC ${f(d.ci_lo, 3)} đến ${f(d.ci_hi, 3)}, trên 0`), C.accent3, 36);
  big(S, 8.4, 4.45, 4.3, `${pct(m4.recall)} · ${pct(m4.precision)}`, t("recall (target ≥ 60%) · precision (target ≥ 30%)", "recall (mục tiêu ≥ 60%) · precision (mục tiêu ≥ 30%)"), C.text2, 36);
  source(S, C, t(`Official run: ${AB.manifest.cells} cells, ${mil(m4.n)} test cell-hours`, `Bản chính thức: ${AB.manifest.cells} ô, ${mil(m4.n)} ô-giờ test`));
  note(S, `Test 2024–2025: ${n(m4.n)} cell-hours, ${n(m4.positives)} heavy (${pct(prev, 2)}), so a random model scores PR-AUC ≈ ${f(prev, 3)}. M4 threshold ${f(m4.threshold, 3)} chosen on 2023: TP ${n(m4.tp)}, FP ${n(m4.fp)}, FN ${n(m4.fn)}; F1 ${f(m4.f1, 3)}, ROC-AUC ${f(m4.roc_auc, 3)}. Terrain (M2) and neighbours (M3) change PR-AUC by less than 0.002 and their CIs cross zero, so they are dropped. Note: this keep-or-drop rule was applied on the test years; using validation 2023 alone gives the same choice (M4 0.210 vs 0.171–0.176 for M1–M3), so the selection does not depend on test data. Recall ${pct(m4.recall)} is still below the 60% target; precision ${pct(m4.precision)} meets the 30% target.`,
    `Test 2024–2025: ${n(m4.n)} ô-giờ, ${n(m4.positives)} giờ mưa lớn (${pct(prev, 2)}), nên đoán ngẫu nhiên được PR-AUC ≈ ${f(prev, 3)}. Ngưỡng M4 ${f(m4.threshold, 3)} chọn trên 2023: TP ${n(m4.tp)}, FP ${n(m4.fp)}, FN ${n(m4.fn)}; F1 ${f(m4.f1, 3)}, ROC-AUC ${f(m4.roc_auc, 3)}. Địa hình (M2) và ô lân cận (M3) đổi PR-AUC chưa tới 0,002 và khoảng tin cậy cắt qua 0, nên bỏ. Lưu ý: quy tắc giữ/bỏ này áp trên các năm test; nếu chỉ dùng validation 2023 vẫn chọn M4 (0,210 so với 0,171–0,176 của M1–M3), nên lựa chọn không phụ thuộc dữ liệu test. Recall ${pct(m4.recall)} vẫn dưới mục tiêu 60%; precision ${pct(m4.precision)} đạt mục tiêu 30%.`);
},

// ---------------- E7: biểu đồ Yagi (lượt stream M4)
yagiChart: (IC) => {
  if (!SD) return;
  const S = slide("CONTENT", SE(), t("E7 · Typhoon Yagi replay: what the stream predicted", "E7 · Phát lại bão Yagi: streaming đã dự báo gì"));
  const cells = Object.keys(SD.cells), times = [...new Set(cells.flatMap((c) => SD.series[c].map((r) => r.ts)))].sort();
  const lab = times.map((x) => `${x.slice(8, 10)}/09 ${x.slice(11, 13)}h`);
  const ser = (c, k) => times.map((x) => { const r = SD.series[c].find((y) => y.ts === x); return r && r[k] != null ? r[k] : null; });
  const thr = (SD.sample_doc && SD.sample_doc.threshold) || R("M4").threshold;
  const nm = (c) => t(SD.cells[c] === "Hải Phòng" ? "Hai Phong" : "Quang Ninh", SD.cells[c]);
  S.addChart(pres.ChartType.line, [...cells.map((c) => ({ name: nm(c), labels: lab, values: ser(c, "p") })),
    { name: t("threshold", "ngưỡng"), labels: lab, values: times.map(() => thr) }],
    Object.assign(CT(), { x: MX, y: 1.3, w: 7.6, h: 2.55, chartColors: [HEX.accent1, HEX.accent5, HEX.accent2], lineSize: 2, lineDataSymbol: "none",
      showLegend: true, legendPos: "r", showTitle: true, title: t("M4 probability of > 50 mm in the next 24 h", "Xác suất M4 mưa > 50 mm trong 24 h tới"),
      valAxisMinVal: 0, valAxisMaxVal: 1, catAxisLabelFrequency: 24, valAxisLabelFormatCode: "0.0" }));
  S.addChart(pres.ChartType.line, [...cells.map((c) => ({ name: nm(c), labels: lab, values: ser(c, "observed_24h_mm") })),
    { name: "50 mm", labels: lab, values: times.map(() => 50) }],
    Object.assign(CT(), { x: MX, y: 3.95, w: 7.6, h: 2.45, chartColors: [HEX.accent1, HEX.accent5, HEX.accent6], lineSize: 2, lineDataSymbol: "none",
      showLegend: true, legendPos: "r", showTitle: true, title: t("ERA5 rain in the following 24 h (mm) — ground truth", "Mưa ERA5 trong 24 h sau đó (mm) — giá trị thật"),
      valAxisMinVal: 0, catAxisLabelFrequency: 24 }));
  const rows = cells.flatMap((c) => SD.series[c]).filter((r) => r.label === 0 || r.label === 1);
  const tp = rows.filter((r) => r.alert && r.label === 1).length, fp = rows.filter((r) => r.alert && r.label === 0).length,
    fn = rows.filter((r) => !r.alert && r.label === 1).length;
  const cf = SD.confusion_all_cells || {};
  big(S, 8.5, 1.35, 4.2, `${tp} / ${tp + fp}`, t("alerts in these two cells were followed by > 50 mm", "cảnh báo ở hai ô này có mưa > 50 mm thật"), C.accent3, 40);
  big(S, 8.5, 2.85, 4.2, `${tp} / ${tp + fn}`, t("heavy-rain hours caught in these two cells", "giờ mưa lớn bắt được ở hai ô này"), C.accent2, 40);
  big(S, 8.5, 4.35, 4.2, `${pct((cf.tp || 0) / Math.max(1, (cf.tp || 0) + (cf.fn || 0)))} · ${pct((cf.tp || 0) / Math.max(1, (cf.tp || 0) + (cf.fp || 0)))}`,
    t("recall · precision over all cells, 04–09/09", "recall · precision trên mọi ô, 04–09/09"), C.text2, 34);
  source(S, C, t("Stream run: Kafka replay of silver 01–10/09/2024, model M4, threshold from 2023 · truth filled in once t + 24 h has passed",
    "Lượt stream: phát lại silver 01–10/09/2024 qua Kafka, model M4, ngưỡng chọn trên 2023 · mưa thật ghi vào sau khi qua t + 24 h"));
  note(S, `Two cells: Hai Phong (20.75°N 106.75°E) and Quang Ninh (21.00°N 107.00°E). In these two cells: TP ${tp}, FP ${fp}, FN ${fn}. Over every cell-hour from 04/09 whose truth is known: TP ${n(cf.tp || 0)}, FP ${n(cf.fp || 0)}, FN ${n(cf.fn || 0)}, TN ${n(cf.tn || 0)}. This is one week inside the 2024–2025 test period, so the numbers are not a replacement for the full test evaluation (E1), only a check that the live lane behaves like the batch model.`,
    `Hai ô: Hải Phòng (20,75°N 106,75°E) và Quảng Ninh (21,00°N 107,00°E). Ở hai ô này: TP ${tp}, FP ${fp}, FN ${fn}. Trên mọi ô-giờ từ 04/09 đã biết mưa thật: TP ${n(cf.tp || 0)}, FP ${n(cf.fp || 0)}, FN ${n(cf.fn || 0)}, TN ${n(cf.tn || 0)}. Đây chỉ là một tuần trong giai đoạn test 2024–2025, nên không thay cho đánh giá đầy đủ (E1), chỉ để kiểm tra làn trực tiếp hoạt động như model batch.`);
},

// ---------------- E7: mẫu dữ liệu, vẽ thành cột xác suất theo thời gian
yagiSample: (IC) => {
  if (!SD) return;
  const hp = SD.series["g20.75_106.75"] || [], thr = (SD.sample_doc && SD.sample_doc.threshold) || R("M4").threshold;
  const yr = hp.filter((r) => /0[5-9] (00|12):00$/.test(r.ts) && r.ts >= "2024-09-05 00:00" && r.ts <= "2024-09-09 00:00");
  const S = slide("CONTENT", SE(), t("E7 data sample: stream predictions vs what happened", "Dữ liệu mẫu E7: dự báo của stream so với thực tế"));
  lead(S, t(`Hai Phong every 12 h: bar = M4 probability, line = alert threshold ${f(thr, 3)}`, `Hải Phòng mỗi 12 h: cột = xác suất M4, vạch = ngưỡng cảnh báo ${f(thr, 3)}`));
  const x0 = MX + 1.9, cw = (W - MX - x0) / Math.max(1, yr.length), base = 4.35, hmax = 2.3;
  [[t("probability", "xác suất"), 3.0], [t("time", "thời điểm"), base + 0.08], [t("rain next 24 h", "mưa 24 h tới"), base + 0.5], [t("result", "kết quả"), base + 1.0]].forEach(([k, y]) =>
    S.addText(k, { x: MX, y, w: 1.8, h: 0.4, fontSize: 12, bold: true, color: C.text2, margin: 0, valign: "middle", isTextBox: true }));
  S.addShape("line", { x: x0, y: base, w: W - MX - x0, h: 0, line: { color: C.accent4, width: 1 } });
  yr.forEach((r, i) => {
    const a = !!r.alert, tr = r.label === 1, known = r.label === 0 || r.label === 1;
    const o = !known ? "–" : a && tr ? "TP" : a && !tr ? "FP" : !a && tr ? "FN" : "TN", oc = { TP: C.accent3, FP: C.accent6, FN: C.accent6, TN: C.accent4, "–": C.accent4 }[o];
    const p = Number(r.p), x = x0 + i * cw, bw = cw * 0.55, h = Math.max(0.03, p * hmax);
    S.addShape("rect", { x: x + (cw - bw) / 2, y: base - h, w: bw, h, fill: { color: a ? C.accent2 : C.accent1 }, line: { color: a ? C.accent2 : C.accent1 } });
    S.addText(f(p, 3), { x, y: base - h - 0.32, w: cw, h: 0.3, fontSize: 11, bold: true, color: C.text1, align: "center", margin: 0, isTextBox: true });
    S.addText(`${r.ts.slice(8, 10)}/09 ${r.ts.slice(11, 13)}h`, { x, y: base + 0.08, w: cw, h: 0.4, fontSize: 11, color: C.text1, align: "center", valign: "middle", margin: 0, isTextBox: true });
    S.addText(r.observed_24h_mm == null ? "–" : `${f(r.observed_24h_mm, 0)} mm`, { x, y: base + 0.5, w: cw, h: 0.4, fontSize: 13, bold: tr, color: tr ? C.accent5 : C.text1, align: "center", valign: "middle", margin: 0, isTextBox: true });
    S.addText(o, { shape: "roundRect", rectRadius: 0.06, x: x + cw * 0.15, y: base + 1.02, w: cw * 0.7, h: 0.38, fill: { color: oc }, color: C.background1,
      fontSize: 12, bold: true, align: "center", valign: "middle", margin: 0 });
  });
  const ty = base - thr * hmax;
  S.addShape("line", { x: x0, y: ty, w: W - MX - x0, h: 0, line: { color: C.accent6, width: 1.5, dashType: "dash" } });
  S.addText([{ text: "TP ", options: { bold: true, color: C.accent3 } }, { text: t("alert and heavy rain   ", "báo và có mưa lớn   ") },
    { text: "FP / FN ", options: { bold: true, color: C.accent6 } }, { text: t("false alarm / missed   ", "báo nhầm / bỏ sót   ") },
    { text: "TN ", options: { bold: true, color: C.accent4 } }, { text: t("no alert, no heavy rain", "không báo, không mưa lớn") }],
    { x: MX, y: 6.0, w: W - 2 * MX, h: 0.35, fontSize: 12.5, color: C.text1, margin: 0, isTextBox: true });
  source(S, C, t("Each bar is one MongoDB document from the stream run (cell 20.75°N 106.75°E), truth added after t + 24 h",
    "Mỗi cột là một document MongoDB của lượt stream (ô 20,75°N 106,75°E), mưa thật ghi thêm sau t + 24 h"));
  note(S, `Each column is the stream's forecast for the Hai Phong cell every 12 hours, read back from MongoDB, with the observed rain of the next 24 hours added once the stream had passed t + 24 h. Model M4, threshold ${f(thr, 3)} chosen on validation 2023 and frozen.`,
    `Mỗi cột là dự báo của stream cho ô Hải Phòng cách nhau 12 giờ, đọc lại từ MongoDB, kèm mưa thật 24 giờ sau đó được ghi vào khi stream đã qua t + 24 h. Model M4, ngưỡng ${f(thr, 3)} chọn trên validation 2023 và khóa lại.`);
},

// ---------------- Kiểm chứng
checks: (IC) => {
  const mv = (D.mapreduce || {}).validation || {}, sv = O.silver, no = O.e0.noaa;
  const S = slide("CONTENT", SE(), t("How we know the outputs are correct", "Làm sao biết kết quả là đúng"));
  const items = [
    ["0", t("duplicate (cell, hour) keys", "khóa (ô, giờ) trùng"), t(`bronze ${mil(sv.bronze_rows)} = silver ${mil(sv.silver_rows)} rows`, `bronze ${mil(sv.bronze_rows)} = silver ${mil(sv.silver_rows)} dòng`)],
    [f(no.median_ratio, 2), t("ERA5 / rain-gauge totals", "tổng mưa ERA5 / trạm đo"), t(`${no.stations.length} NOAA gauges, ${no.years}`, `${no.stations.length} trạm NOAA, ${no.years}`)],
    [`${mv.mismatches ?? "–"} / ${mv.keys_spark ?? "–"}`, t("MapReduce vs Spark SQL differences", "khác biệt MapReduce và Spark SQL"), t("same climatology table", "cùng bảng climatology")],
    [t("no leak", "không rò rỉ"), t("future data cannot change features", "dữ liệu tương lai không đổi được feature"), t("unit tests change the future and compare", "unit test đổi tương lai rồi so sánh")],
    [SR ? n(SR.parity_stream_vs_batch_same_rows.cell_hours_both_scored) : "–", t("cell-hours: stream = batch", "ô-giờ: stream = batch"), t("same rows, same features, 0 alert differences", "cùng dữ liệu, cùng feature, 0 khác cảnh báo")],
    ["Δ = 0", t("for two identical models", "với hai model giống nhau"), t("PR-AUC checked on hand-computed cases", "PR-AUC kiểm bằng ví dụ tính tay")],
    [t("same rows", "cùng dòng"), t("baselines and models", "baseline và model"), t("same test rows, same threshold rule", "cùng dòng test, cùng cách chọn ngưỡng")],
    [SR ? `${n(SR.kafka.read_by_forecast_query)}` : "–", t("Kafka messages read = in topic", "bản tin Kafka đọc = có trong topic"), t("Spark killed mid-run: 0 lost, 0 duplicate forecasts", "tắt Spark giữa chừng: 0 mất, 0 dự báo trùng")],
  ];
  const gw = (W - 2 * MX - 3 * 0.25) / 4;
  items.forEach(([v, h, d], i) => {
    const x = MX + (i % 4) * (gw + 0.25), y = 1.35 + Math.floor(i / 4) * 2.55;
    S.addShape("roundRect", { x, y, w: gw, h: 2.3, rectRadius: 0.1, fill: { color: C.background2 }, line: { color: C.background2 } });
    S.addText(v, { x: x + 0.15, y: y + 0.15, w: gw - 0.3, h: 0.8, fontSize: 32, bold: true, color: C.accent3, margin: 0, valign: "middle", isTextBox: true, fit: "shrink" });
    S.addText([{ text: h, options: { bold: true, fontSize: 14, color: C.text2, breakLine: true } }, { text: d, options: { fontSize: 11.5, color: C.text1 } }],
      { x: x + 0.15, y: y + 1.0, w: gw - 0.3, h: 1.2, margin: 0, valign: "top", isTextBox: true });
  });
  note(S, "All tests run with pytest inside the Spark container. Row counts: bronze and silver match, and gold has no duplicate (cell, hour) keys. Independent truth: ERA5 rain compared with NOAA rain gauges. MapReduce and Spark SQL compute the same climatology table with zero differences. Leakage: unit tests modify future rows and assert past features do not change; the label window is exact and rows across split boundaries are purged. Stream = batch: recomputing in batch on exactly the rows the stream accepted gives the same forecasts. Metrics: average precision is checked on hand-computed examples, and identical models give a difference of exactly 0. Baselines use the same rows and the same threshold method. Kafka: Spark was killed mid-run and restarted; it still read exactly the messages in the topic and MongoDB has no duplicate forecast.",
    "Mọi test chạy bằng pytest trong container Spark. Số dòng: bronze và silver khớp, gold không có khóa (ô, giờ) trùng. Đối chiếu độc lập: so mưa ERA5 với trạm đo mưa NOAA. MapReduce và Spark SQL tính ra cùng bảng climatology, không khác biệt. Rò rỉ: unit test sửa dữ liệu tương lai và kiểm tra feature quá khứ không đổi; cửa sổ nhãn chính xác, dòng nằm ở ranh giới split bị loại. Stream = batch: tính lại bằng batch trên đúng các dòng stream đã nhận cho cùng dự báo. Thước đo: average precision kiểm bằng ví dụ tính tay, hai model giống nhau cho chênh lệch đúng bằng 0. Baseline dùng cùng dòng và cùng cách chọn ngưỡng. Kafka: tắt Spark giữa chừng rồi chạy lại vẫn đọc đúng số bản tin có trong topic, MongoDB không có dự báo trùng.");
},

// ---------------- E6: thí nghiệm hiệu năng (dự kiến)
perf: (IC) => {
  const S = slide("CONTENT", SE(), t("E6 · Spark performance experiment (measured)", "E6 · Thí nghiệm hiệu năng Spark (đã đo)"));
  if (!PF) return;
  const e1 = PF.sum.filter((r) => r.experiment === "E1_shuffle_partitions"), e2 = PF.sum.filter((r) => r.experiment === "E2_join_strategy");
  const best1 = e1.reduce((a, b) => (b.mean_s < a.mean_s ? b : a));
  lead(S, t("Change one factor at a time on the official data, measure, explain", "Đổi một yếu tố mỗi lần trên dữ liệu chính thức, đo, rồi giải thích"));
  // thí nghiệm 1: cột thời gian theo số partition
  const half = (W - 2 * MX - 0.5) / 2, base = 4.55, hmax = 2.0, mx1 = Math.max(...e1.map((r) => r.max_s));
  S.addText([{ text: t("1 · shuffle partitions", "1 · số shuffle partition"), options: { bold: true, color: C.accent1, fontSize: 15, breakLine: true } },
    { text: t(`M4 feature build, ${PF.env.E1_input.split("=")[1].trim()}, AQE off`, `dựng feature M4, ${PF.env.E1_input.split("=")[1].trim().replace("rows", "dòng")}, tắt AQE`), options: { fontSize: 11, color: C.text1 } }],
    { x: MX, y: 1.85, w: half, h: 0.6, margin: 0, isTextBox: true });
  const bw1 = half / e1.length;
  e1.forEach((r, i) => {
    const x = MX + i * bw1, h = (r.mean_s / mx1) * hmax, hl = r === best1, col = hl ? C.accent2 : C.accent1;
    S.addShape("rect", { x: x + bw1 * 0.2, y: base - h, w: bw1 * 0.6, h, fill: { color: col }, line: { color: col } });
    S.addText(`${f(r.mean_s, 1)} s`, { x, y: base - h - 0.3, w: bw1, h: 0.28, fontSize: 12, bold: true, color: col, align: "center", margin: 0, isTextBox: true });
    S.addText([{ text: `${r.setting}`, options: { bold: true, breakLine: true } }, { text: t(`${r.tasks} tasks`, `${r.tasks} task`), options: { fontSize: 10 } }],
      { x, y: base + 0.05, w: bw1, h: 0.45, fontSize: 12, color: C.text1, align: "center", margin: 0, isTextBox: true });
  });
  // thí nghiệm 2: broadcast vs sort-merge
  const x2 = MX + half + 0.5, mx2 = Math.max(...e2.map((r) => r.max_s)), bw2 = half / 2;
  S.addText([{ text: t("2 · join strategy", "2 · cách join"), options: { bold: true, color: C.accent5, fontSize: 15, breakLine: true } },
    { text: t(`heavy-rain cell-days per province × year on gold, ${PF.env.E2_input.split("=")[1].trim()}`, `số ô-ngày mưa lớn theo tỉnh × năm trên gold, ${PF.env.E2_input.split("=")[1].trim().replace("rows", "dòng")}`), options: { fontSize: 11, color: C.text1 } }],
    { x: x2, y: 1.85, w: half, h: 0.6, margin: 0, isTextBox: true });
  e2.forEach((r, i) => {
    const x = x2 + i * bw2, h = (r.mean_s / mx2) * hmax, col = i === 0 ? C.accent5 : C.accent4;
    S.addShape("rect", { x: x + bw2 * 0.25, y: base - h, w: bw2 * 0.5, h, fill: { color: col }, line: { color: col } });
    S.addText(`${f(r.mean_s, 1)} s`, { x, y: base - h - 0.3, w: bw2, h: 0.28, fontSize: 12, bold: true, color: col, align: "center", margin: 0, isTextBox: true });
    S.addText([{ text: r.setting === "broadcast" ? "BroadcastHashJoin" : "SortMergeJoin", options: { bold: true, breakLine: true } },
      { text: t(`shuffle ${f(r.shuffle_write_mb, 0)} MB`, `shuffle ${f(r.shuffle_write_mb, 0)} MB`), options: { fontSize: 10 } }],
      { x, y: base + 0.05, w: bw2, h: 0.45, fontSize: 12, color: C.text1, align: "center", margin: 0, isTextBox: true });
  });
  const b = e2.find((r) => r.setting === "broadcast"), s = e2.find((r) => r.setting === "sortmerge");
  S.addText([{ text: t("Reading: ", "Đọc kết quả: "), options: { bold: true, color: C.accent2 } },
    { text: t(`fastest at ${best1.setting} partitions: on 3 cores, more partitions only add task overhead, the ~${f(e1[0].shuffle_write_mb, 0)} MB shuffle stays the same. Broadcast halves the shuffle (${f(b.shuffle_write_mb, 0)} vs ${f(s.shuffle_write_mb, 0)} MB) but is not faster (${f(b.mean_s, 1)} vs ${f(s.mean_s, 1)} s): the join comes after aggregation to ~2 M cell-days, so reading 47.5 M rows dominates.`,
      `nhanh nhất ở ${best1.setting} partition: máy 3 lõi nên thêm partition chỉ tăng chi phí task, shuffle ~${f(e1[0].shuffle_write_mb, 0)} MB không đổi. Broadcast giảm một nửa shuffle (${f(b.shuffle_write_mb, 0)} so với ${f(s.shuffle_write_mb, 0)} MB) nhưng không nhanh hơn (${f(b.mean_s, 1)} so với ${f(s.mean_s, 1)} s): join diễn ra sau khi đã gom còn ~2 triệu ô-ngày, nên đọc 47,5 triệu dòng mới là phần tốn thời gian.`) }],
    { x: MX, y: 5.25, w: W - 2 * MX, h: 0.75, fontSize: 13, color: C.text1, margin: 0, valign: "top", isTextBox: true });
  source(S, C, t(`Spark ${PF.env.spark} local[3], driver ${PF.env.driver_memory}, one laptop · mean of ${PF.env.repeats} · reports/official/performance/`,
    `Spark ${PF.env.spark} local[3], driver ${PF.env.driver_memory}, một laptop · trung bình ${PF.env.repeats} · reports/official/performance/`));
  const rows = PF.sum.map((r) => `${r.experiment} ${r.setting}: mean ${f(r.mean_s, 1)} s (min ${f(r.min_s, 1)}, max ${f(r.max_s, 1)}), tasks ${r.tasks}, shuffle write ${f(r.shuffle_write_mb, 0)} MB`).join("; ");
  note(S, `Controlled experiment on the official data, one factor at a time, AQE off so Spark does not merge partitions by itself, 1 warm-up run then timed runs, noop sink so only computation is timed. ${rows}. Limits: one laptop with 3 cores, so absolute times are noisy (other processes, disk cache) and cannot prove cluster scaling; on a cluster the best partition count grows with the number of executor cores and the data size. Shuffle volume is the same for every partition count — only how it is split changes. Broadcast helps while the small side fits in executor memory (10 MB threshold here; the cell table is a few KB).`,
    `Thí nghiệm có kiểm soát trên dữ liệu chính thức, mỗi lần đổi một yếu tố, tắt AQE để Spark không tự gộp partition, chạy làm nóng 1 lần rồi đo, ghi ra sink noop để chỉ đo phần tính toán. ${rows}. Giới hạn: một laptop 3 lõi nên thời gian tuyệt đối còn nhiễu (tiến trình khác, cache đĩa) và không chứng minh được khả năng mở rộng trên cụm; trên cụm, số partition tốt nhất tăng theo số lõi executor và kích thước dữ liệu. Lượng shuffle như nhau ở mọi số partition — chỉ cách chia thay đổi. Broadcast có lợi khi bảng nhỏ vừa bộ nhớ executor (ngưỡng 10 MB; bảng ô chỉ vài KB).`);
},

// ---------------- Session 9: tổng kết mức sẵn sàng kỹ thuật
readiness: (IC) => {
  const S = slide("CONTENT", SE(), t("Session 9 Technical Readiness Summary", "Session 9: tổng kết mức sẵn sàng kỹ thuật"));
  const m4 = R("M4"), per = R("persistence_rain24h"), d4 = DL("M4 - M1");
  const e1 = PF ? PF.sum.filter((r) => r.experiment === "E1_shuffle_partitions") : [], e2 = PF ? PF.sum.filter((r) => r.experiment === "E2_join_strategy") : [];
  const best1 = e1.length ? e1.reduce((a, b) => (b.mean_s < a.mean_s ? b : a)) : null;
  const bb = e2.find((r) => r.setting === "broadcast"), ss = e2.find((r) => r.setting === "sortmerge");
  const rows = [
    [t("1 · Architecture truth", "1 · Kiến trúc thật"), t("every component tagged [I] / [I*] pilot / [P] design; MapReduce + Spark SQL marked pilot; daily ETL and cluster marked design; streaming labelled a replay (hindcast)",
      "mọi thành phần gắn [I] / [I*] pilot / [P] thiết kế; MapReduce + Spark SQL ghi là pilot; ETL hằng ngày và cụm ghi là thiết kế; streaming ghi rõ là phát lại (hindcast)")],
    [t("2 · Performance", "2 · Hiệu năng"), PF ? t(`shuffle partitions 4/16/64/200 → fastest ${best1.setting} (${f(best1.mean_s, 1)} s) on 3 cores; broadcast join halves shuffle but is not faster (${f(bb.mean_s, 1)} vs ${f(ss.mean_s, 1)} s, 47.5 M rows)`,
      `shuffle partition 4/16/64/200 → nhanh nhất ${best1.setting} (${f(best1.mean_s, 1)} s) trên 3 lõi; broadcast join giảm nửa shuffle nhưng không nhanh hơn (${f(bb.mean_s, 1)} so với ${f(ss.mean_s, 1)} s, 47,5 triệu dòng)`) : "–"],
    [t("3 · Validation", "3 · Kiểm chứng"), t(`data: 0 duplicate keys, bronze → silver reconciled (−493×24 h outside 2015–2025); ML: M4 PR-AUC ${f(m4.pr_auc)} vs ${f(per.pr_auc)} persistence, atmosphere +${f(d4.delta_pr_auc, 3)} (CI > 0)`,
      `dữ liệu: 0 khóa trùng, bronze → silver khớp (−493×24 giờ ngoài 2015–2025); ML: M4 PR-AUC ${f(m4.pr_auc)} so với ${f(per.pr_auc)} persistence, khí quyển +${f(d4.delta_pr_auc, 3)} (KTC > 0)`)],
    [t("4 · Operations", "4 · Vận hành"), SR ? t(`Kafka replay, not live (ERA5 ~5 days late); Spark killed mid-run: ${n(SR.kafka.read_by_forecast_query)} / ${n(SR.kafka.messages_in_topic)} read, 0 duplicate forecasts; README reruns from raw CDS; cluster = design`,
      `Kafka phát lại, không phải trực tiếp (ERA5 trễ ~5 ngày); tắt Spark giữa chừng: đọc ${n(SR.kafka.read_by_forecast_query)} / ${n(SR.kafka.messages_in_topic)}, 0 dự báo trùng; README chạy lại từ CDS gốc; cụm = thiết kế`) : "–"],
    [t("5 · Limitation", "5 · Giới hạn"), t(`recall ${pct(m4.recall)} at the F1 threshold → decision support for forecasters, not a stand-alone warning; recall-first threshold gives 74% recall at 19% precision`,
      `recall ${pct(m4.recall)} ở ngưỡng F1 → hỗ trợ dự báo viên, chưa phải hệ thống cảnh báo độc lập; ngưỡng ưu tiên recall cho recall 74%, precision 19%`)],
  ];
  rows.forEach(([h, d], i) => {
    const y = 1.3 + i * 0.98;
    S.addShape("roundRect", { x: MX, y, w: W - 2 * MX, h: 0.85, rectRadius: 0.08, fill: { color: i % 2 ? C.background1 : C.background2 }, line: { color: C.background2 } });
    S.addText(h, { x: MX + 0.2, y, w: 2.6, h: 0.85, fontSize: 15, bold: true, color: C.text2, valign: "middle", margin: 0, isTextBox: true });
    S.addText(d, { x: MX + 2.9, y, w: W - 2 * MX - 3.1, h: 0.85, fontSize: 13, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
  });
  source(S, C, t("Evidence: evidence/session9/ (A architecture · B benchmark · C query plan · D data · E model · F serving/stream · G deployment · H Card E · I readiness)",
    "Bằng chứng: evidence/session9/ (A kiến trúc · B hiệu năng · C query plan · D dữ liệu · E model · F phục vụ/stream · G triển khai · H Card E · I sẵn sàng)"));
  note(S, "One line per required item of the Session 9 handoff. Every number comes from a measured run on the official data, except the cluster deployment, which is a design. Details and file paths for each line are in evidence/session9/.",
    "Mỗi dòng ứng với một mục bắt buộc của Session 9. Mọi con số đến từ lượt chạy đo thật trên dữ liệu chính thức, trừ phần triển khai cụm là thiết kế. Chi tiết và đường dẫn file của từng dòng nằm trong evidence/session9/.");
},

// ---------------- Giới hạn → bước tiếp theo
limits: (IC) => {
  const m4 = R("M4"), no = O.e0.noaa;
  const S = slide("CONTENT", SE(), t("Limitations and next steps", "Giới hạn và bước tiếp theo"));
  S.addText(t("Limitation", "Giới hạn"), { x: MX, y: 1.25, w: 5.6, h: 0.4, fontSize: 16, bold: true, color: C.accent6, margin: 0, isTextBox: true });
  S.addText(t("Next step", "Bước tiếp theo"), { x: 7.0, y: 1.25, w: 5.7, h: 0.4, fontSize: 16, bold: true, color: C.accent1, margin: 0, isTextBox: true });
  [[t(`Recall ${pct(m4.recall)}, target 60%`, `Recall ${pct(m4.recall)}, mục tiêu 60%`), t("pick the threshold for recall; calibrate probabilities", "chọn ngưỡng ưu tiên recall; hiệu chỉnh xác suất")],
   [t("Rain after landfall is missed", "Bỏ sót mưa sau khi bão vào bờ"), t("add upwind / 100–300 km context", "thêm thông tin đầu gió / 100–300 km")],
   [t(`Label = ERA5 on a 25 km cell (hits ${pct(no.hit / no.heavy_days)} of gauge heavy days)`, `Nhãn = ERA5 trên ô 25 km (trúng ${pct(no.hit / no.heavy_days)} ngày mưa lớn tại trạm)`), t("check against more rain gauges", "đối chiếu thêm nhiều trạm đo mưa")],
   [t("Kafka has one broker; Spark runs locally", "Kafka chỉ một broker; Spark chạy local"), t("3 brokers with replication; Spark on YARN", "3 broker có nhân bản; Spark trên YARN")],
   [t("ERA5 arrives ~5 days late", "ERA5 trễ khoảng 5 ngày"), t("Forecast API producer for a live stream", "producer từ Forecast API cho stream thật")],
   [t("Spark in local mode; silver/gold not on HDFS", "Spark chạy local; silver/gold chưa lên HDFS"), t("run on YARN, lake on HDFS; repeat E6 on the cluster", "chạy trên YARN, lake trên HDFS; đo lại E6 trên cụm")]].forEach(([a, b], i) => {
    const y = 1.8 + i * 0.75;
    S.addShape("roundRect", { x: MX, y, w: 5.6, h: 0.6, rectRadius: 0.08, fill: { color: "F8E9EB" }, line: { color: "F8E9EB" } });
    S.addText(a, { x: MX + 0.2, y, w: 5.3, h: 0.6, fontSize: 13.5, bold: true, color: C.text2, valign: "middle", margin: 0, isTextBox: true });
    arrow(S, C, MX + 5.7, y + 0.3, 6.9, y + 0.3, { color: C.accent2, width: 1.75 });
    S.addShape("roundRect", { x: 7.0, y, w: W - MX - 7.0, h: 0.6, rectRadius: 0.08, fill: { color: C.background2 }, line: { color: C.background2 } });
    S.addText(b, { x: 7.2, y, w: W - MX - 7.4, h: 0.6, fontSize: 13.5, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
  });
  note(S, `The model now finds about ${pct(m4.recall)} of heavy-rain hours with ${pct(m4.precision)} precision; to reach 60% recall the threshold can be chosen for recall instead of F1, and probabilities calibrated. The Yagi replay shows that rain after landfall is missed, so context from farther away or upwind is the next feature idea. The label is ERA5 on a 25 km cell, which reaches 50 mm on only ${pct(no.hit / no.heavy_days)} of rain-gauge heavy-rain days. The streaming lane runs the official M4 through a single Kafka broker; production needs several brokers with replication. ERA5 is published about 5 days late, so live use needs a Forecast API producer and re-validation. Spark ran in local mode and silver/gold are on local disk; the next step is YARN with the lake on HDFS, where the E6 performance experiment will run.`,
    `Model hiện bắt được khoảng ${pct(m4.recall)} số giờ mưa lớn với precision ${pct(m4.precision)}; để đạt recall 60% có thể chọn ngưỡng theo recall thay vì F1 và hiệu chỉnh xác suất. Phát lại Yagi cho thấy mưa sau khi bão vào bờ bị bỏ sót, nên hướng feature tiếp là thông tin từ xa hơn hoặc phía đầu gió. Nhãn là ERA5 trên ô 25 km, chỉ đạt 50 mm ở ${pct(no.hit / no.heavy_days)} số ngày mưa lớn tại trạm đo. Làn streaming chạy M4 chính thức qua một broker Kafka; vận hành thật cần nhiều broker có nhân bản. ERA5 công bố trễ khoảng 5 ngày nên chạy thật cần producer từ Forecast API và kiểm chứng lại. Spark chạy local, silver/gold nằm trên ổ cục bộ; bước tiếp là chạy trên YARN với lake trên HDFS, và chạy thí nghiệm hiệu năng E6 ở đó.`);
},

// ---------------- Kết luận + phân công
conclusions: (IC) => {
  const m4 = R("M4"), per = R("persistence_rain24h"), d4 = DL("M4 - M1"), d2 = DL("M2 - M1"), d3 = DL("M3 - M1");
  const S = slide("CONTENT", SE(), t("Conclusions and team contributions", "Kết luận và phân công"));
  // chuỗi pipeline
  const chain = ["CDS ERA5", "raw · bronze (HDFS)", "silver · gold", "Spark ML", "Kafka", "Streaming", "MongoDB · web"];
  const gap = 0.3, cw = (W - 2 * MX - gap * (chain.length - 1)) / chain.length;
  chain.forEach((c, i) => {
    const x = MX + i * (cw + gap);
    S.addText(c, { shape: "roundRect", rectRadius: 0.08, x, y: 1.3, w: cw, h: 0.5, fill: { color: C.background2 }, line: { color: C.accent1, width: 1 },
      fontSize: 12, bold: true, color: C.text2, align: "center", valign: "middle", margin: 0 });
    if (i < chain.length - 1) arrow(S, C, x + cw + 0.02, 1.55, x + cw + gap - 0.02, 1.55, { color: C.accent2, width: 1.5 });
  });
  S.addText(t("end to end on 493 cells × 11 years, plus MapReduce and Spark SQL on the same lake", "chạy trọn trên 493 ô × 11 năm, kèm MapReduce và Spark SQL trên cùng lake"),
    { x: MX, y: 1.85, w: W - 2 * MX, h: 0.3, fontSize: 12, italic: true, color: C.text1, margin: 0, isTextBox: true });
  const cw3 = (W - 2 * MX) / 3;
  big(S, MX, 2.35, cw3 - 0.2, f(m4.pr_auc), t(`PR-AUC of the final model M4, vs ${f(per.pr_auc)} for persistence`, `PR-AUC của model cuối M4, so với ${f(per.pr_auc)} của persistence`), C.accent2, 44);
  big(S, MX + cw3, 2.35, cw3 - 0.2, `+${f(d4.delta_pr_auc, 3)}`, t("atmosphere helps: 95% CI above 0", "khí quyển có ích: KTC 95% trên 0"), C.accent3, 44);
  big(S, MX + 2 * cw3, 2.35, cw3 - 0.2, `≈ 0`, t(`terrain (+${f(d2.delta_pr_auc, 3)}) and neighbours (+${f(d3.delta_pr_auc, 3)}) add nothing`, `địa hình (+${f(d2.delta_pr_auc, 3)}) và ô lân cận (+${f(d3.delta_pr_auc, 3)}) không thêm gì`), C.accent4, 44);
  S.addText([{ text: t("Decision: ", "Quyết định: "), options: { bold: true, color: C.accent2 } },
    { text: t("the final model is M4 = local weather + atmosphere (33 features).", "model cuối là M4 = thời tiết tại chỗ + khí quyển (33 feature).") }],
    { x: MX, y: 3.85, w: W - 2 * MX, h: 0.45, fontSize: 17, bold: true, color: C.text2, margin: 0, isTextBox: true });
  // phân công
  [["V", t("Mr. Viet", "Mr. Việt"), t("Batch layer", "Batch layer"), t("ingestion, 63 → 34 mapping, bronze → gold, MapReduce, Spark SQL", "thu thập, ánh xạ 63 → 34, bronze → gold, MapReduce, Spark SQL")],
   ["L", t("Mr. Luan", "Mr. Luân"), t("ML & evaluation", "ML & đánh giá"), t("EDA, NOAA check, features, RF + ablation, performance plan", "EDA, đối chiếu NOAA, feature, RF + ablation, kế hoạch hiệu năng")],
   ["T", t("Mr. Thinh", "Mr. Thịnh"), t("Streaming & serving", "Streaming & phục vụ"), t("Kafka producer + junk tests, Structured Streaming, MongoDB, web dashboard", "producer Kafka + kiểm thử dữ liệu rác, Structured Streaming, MongoDB, web dashboard")]].forEach(([ini, who, role, what], i) => {
    const x = MX + i * cw3, y = 4.55;
    S.addText(ini, { shape: "ellipse", x, y: y + 0.1, w: 0.8, h: 0.8, fill: { color: "EAF3F4" }, line: { color: "EAF3F4" }, color: C.text2,
      fontSize: 24, bold: true, align: "center", valign: "middle", margin: 0 });
    S.addText([{ text: `${who} — ${role}`, options: { bold: true, fontSize: 14, color: C.text2, breakLine: true } }, { text: what, options: { fontSize: 11.5, color: C.text1 } }],
      { x: x + 0.95, y, w: cw3 - 1.1, h: 1.0, margin: 0, valign: "middle", isTextBox: true });
  });
  S.addText(t("Questions?", "Câu hỏi?"), { x: MX, y: 5.95, w: 5, h: 0.45, fontSize: 22, bold: true, color: C.accent1, margin: 0, isTextBox: true });
  note(S, `The pipeline runs end to end: Copernicus CDS → raw and bronze (also on HDFS) → silver → gold → Spark ML → Structured Streaming → MongoDB, with MapReduce and Spark SQL on the same lake. On the held-out years 2024–2025 the final model M4 reaches PR-AUC ${f(m4.pr_auc)} against ${f(per.pr_auc)} for persistence. The ablation shows atmosphere features add ${f(d4.delta_pr_auc, 3)} PR-AUC with a 95% CI of ${f(d4.ci_lo, 3)} to ${f(d4.ci_hi, 3)}, while terrain and neighbouring cells add almost nothing and their CIs cross zero.`,
    `Pipeline chạy trọn: Copernicus CDS → raw và bronze (có cả trên HDFS) → silver → gold → Spark ML → Structured Streaming → MongoDB, kèm MapReduce và Spark SQL trên cùng lake. Trên các năm giữ riêng 2024–2025, model cuối M4 đạt PR-AUC ${f(m4.pr_auc)} so với ${f(per.pr_auc)} của persistence. Ablation cho thấy feature khí quyển thêm ${f(d4.delta_pr_auc, 3)} PR-AUC với khoảng tin cậy 95% từ ${f(d4.ci_lo, 3)} đến ${f(d4.ci_hi, 3)}, còn địa hình và ô lân cận gần như không thêm gì và khoảng tin cậy cắt qua 0.`);
},
};
