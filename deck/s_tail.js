// Các slide từ Structured Streaming trở đi, vẽ lại: luồng / hình / chữ lớn, chi tiết đưa xuống notes cho người trình bày.
const fs = require("fs"), path = require("path");
const K = require("./common");
const { pres, C, D, A, HEX, W, MX, t, f, pct, n, slide, source, pill, arrow, CT, SEL } = K;
const O = JSON.parse(fs.readFileSync(path.join(K.ROOT, "reports", "official", "deck_official.json"), "utf8"));
const AB = O.ablation, R = (m) => AB.results.find((r) => r.model === m && r.split === "test"), DL = (c) => AB.deltas.find((d) => d.comparison === c);
const SA = () => t("Architecture & pipeline", "Kiến trúc & pipeline"), SE = () => t("Experiments & results", "Thực nghiệm & kết quả");
const mil = (x) => t(`${f(x / 1e6, 1)} M`, `${f(x / 1e6, 1).replace(".", ",")} triệu`);
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
// ---------------- Streaming
streaming: (IC) => {
  const yg = D.yagi || [], mc = D.mongo ? D.mongo.counts : {};
  const S = slide("CONTENT", SA(), t("Structured Streaming: replaying Typhoon Yagi (Sep 2024)", "Structured Streaming: phát lại bão Yagi (09/2024)"));
  lead(S, t("Replay Typhoon Yagi hour by hour as if it were live, and predict heavy rain 24 h ahead",
    "Phát lại bão Yagi từng giờ như dữ liệu trực tiếp, và dự báo mưa lớn 24 h tới"));
  flow(S, 2.35, [
    { can: true, head: "silver/", sub: "01–10/09/2024" },
    { head: t("Replay producer", "Bộ phát lại"), sub: t("1 JSON file per hour", "1 file JSON mỗi giờ"), num: t(`${n(D.stream.files)} files`, `${n(D.stream.files)} file`) },
    { head: t("Read stream", "Đọc stream"), sub: t("file source, 6 files per trigger", "file source, 6 file mỗi lượt"), num: t(`${n(D.stream.checkpoint_commits)} micro-batches`, `${n(D.stream.checkpoint_commits)} micro-batch`) },
    { head: t("Build features", "Tạo feature"), sub: t("96 h buffer, same code as batch", "bộ đệm 96 h, cùng code batch"), num: t("72 h warm-up", "làm nóng 72 h") },
    { head: t("Predict", "Dự báo"), sub: t(`RF ${SEL} + 2023 threshold, frozen`, `RF ${SEL} + ngưỡng 2023, cố định`), num: t("no refit on 2024", "không train lại") },
    { can: true, head: "MongoDB", sub: t("upsert", "upsert"), num: t(`${n(mc.predictions || 0)} predictions`, `${n(mc.predictions || 0)} dự báo`) },
  ]);
  big(S, MX, 4.6, 3.6, "0", t("files processed twice when the job is restarted (checkpoint)", "file bị xử lý lại khi chạy lại job (nhờ checkpoint)"), C.accent3, 44);
  big(S, MX + 4.2, 4.6, 3.6, n(yg.length), t("predictions checked against the real rain once t + 24 h has passed", "dự báo được đối chiếu với mưa thật sau khi qua t + 24 h"), C.accent2, 44);
  big(S, MX + 8.4, 4.6, W - 2 * MX - 8.4, t("final model", "model cuối"), t("the official model plugs into this same lane", "model chính thức gắn vào đúng làn này"), C.text2, 30);
  source(S, C, PILOT());
  note(S, "The producer writes each hour to a .tmp file and renames it, so Spark never reads half a file. Spark reads the folder as a stream, at most 6 files per trigger. In foreachBatch it removes duplicate (cell, hour) rows, appends to a 96-hour history buffer and builds features with the same function as the batch job (a parity test checks they are equal). The frozen RF and its 2023 threshold predict; results are upserted with _id = point|hour|model|replay, so a retry never creates duplicates. No watermark or Kafka: there is no stateful aggregation inside Spark, the rolling state is the history buffer keyed by (cell, hour), and a file source gives the same delivery contract as a topic without running another service.",
    "Bộ phát lại ghi mỗi giờ ra file .tmp rồi đổi tên, nên Spark không bao giờ đọc nửa file. Spark đọc thư mục như một stream, tối đa 6 file mỗi lượt. Trong foreachBatch: lọc trùng (ô, giờ), nối vào bộ đệm lịch sử 96 giờ, tạo feature bằng đúng hàm của batch (có parity test kiểm tra bằng nhau). Model RF và ngưỡng 2023 giữ cố định để dự báo; kết quả upsert với _id = điểm|giờ|model|replay nên chạy lại không sinh bản trùng. Không dùng watermark hay Kafka: không có phép gộp có trạng thái trong Spark, trạng thái là bộ đệm lịch sử theo (ô, giờ), và file source cho cùng cam kết như một topic mà không cần chạy thêm dịch vụ.");
},

// ---------------- MongoDB
mongo: (IC) => {
  const mg = D.mongo || {}, sp = mg.sample_prediction || {};
  const S = slide("CONTENT", SA(), t("Serving: MongoDB data model and read-back", "Phục vụ: mô hình dữ liệu MongoDB và đọc lại"));
  lead(S, t("One document per city and hour, so a user's question is a single indexed lookup",
    "Mỗi thành phố, mỗi giờ là một document, nên câu hỏi của người dùng chỉ là một lần tra theo index"));
  // hình một document: các trường chính, chữ to dễ đọc
  const x0 = MX, y0 = 2.0, dw = 5.2;
  S.addShape("roundRect", { x: x0, y: y0, w: dw, h: 3.9, rectRadius: 0.1, fill: { color: C.background2 }, line: { color: C.accent1, width: 1.5 } });
  S.addText(t("predictions · one document", "predictions · một document"), { x: x0 + 0.25, y: y0 + 0.15, w: dw - 0.5, h: 0.35, fontSize: 12, bold: true, color: C.accent1, margin: 0, isTextBox: true });
  const ptime = String(sp.prediction_time || "").slice(0, 16).replace("T", " ");
  [[t("city", "thành phố"), `${sp.location_id || "–"} · ${sp.new_province_code || ""}`],
   [t("time", "thời điểm"), ptime],
   [t("probability", "xác suất"), f(sp.heavy_rain_probability || 0, 2)],
   [t("threshold", "ngưỡng"), f(sp.decision_threshold || 0, 3)],
   [t("alert", "cảnh báo"), sp.is_heavy_rain_predicted ? t("YES", "CÓ") : t("no", "không")],
   ["model", `${SEL} · ${sp.run_type || "replay"}`]].forEach(([k, v], i) => {
    const y = y0 + 0.6 + i * 0.52;
    S.addText(k, { x: x0 + 0.25, y, w: 1.6, h: 0.45, fontSize: 13, color: C.text1, margin: 0, valign: "middle", isTextBox: true });
    S.addText(v, { x: x0 + 1.9, y, w: dw - 2.1, h: 0.45, fontSize: 17, bold: true, fontFace: "Consolas", color: i === 4 && sp.is_heavy_rain_predicted ? C.accent2 : C.text2, margin: 0, valign: "middle", isTextBox: true });
  });
  // ba câu hỏi → index → kết quả
  const st = (q) => ((mg[q] || {}).plan_stages || []).includes("IXSCAN") ? t("index scan ✓", "dùng index ✓") : "–";
  [[t("Which provinces are on alert at hour t?", "Tỉnh nào đang cảnh báo tại giờ t?"), "(time, score ↓)", st("q1_alerts_at_time")],
   [t("Probability over time for one city", "Xác suất theo thời gian của một thành phố"), "(city, time)", st("q2_location_series")],
   [t("Alert hours per province", "Số giờ cảnh báo theo tỉnh"), "$group", t("aggregation", "aggregation")]].forEach(([q, idx, res], i) => {
    const y = 2.05 + i * 1.3, x = 6.0;
    S.addText(q, { x, y, w: 2.85, h: 1.0, fontSize: 14, bold: true, color: C.text2, margin: 0, valign: "middle", isTextBox: true });
    arrow(S, C, x + 2.95, y + 0.5, x + 3.3, y + 0.5, { color: C.accent2, width: 1.75 });
    S.addText(idx, { shape: "roundRect", rectRadius: 0.08, x: x + 3.35, y: y + 0.2, w: 1.75, h: 0.6, fill: { color: C.background1 }, line: { color: C.accent1, width: 1.25 },
      fontFace: "Consolas", fontSize: 12, color: C.accent1, bold: true, align: "center", valign: "middle", margin: 0 });
    arrow(S, C, x + 5.15, y + 0.5, x + 5.5, y + 0.5, { color: C.accent2, width: 1.75 });
    S.addText(res, { x: x + 5.55, y, w: W - MX - x - 5.55, h: 1.0, fontSize: 13, bold: true, color: C.accent3, margin: 0, valign: "middle", isTextBox: true });
  });
  source(S, C, t(`Real document read back after the Yagi replay · ${PILOT()}`, `Document thật đọc lại sau khi phát lại bão Yagi · ${PILOT()}`));
  note(S, "Why MongoDB and not only Parquet: Parquet is built for scans, users need lookups by key in milliseconds. Each (city, hour) is one self-contained document, so no join is needed at read time. Upsert by a deterministic _id means Spark retries never create duplicates. CAP: a replica set with one primary is CP — writes with w: majority survive a failover, reads wait about 10 s while a new primary is elected.",
    "Vì sao MongoDB mà không chỉ Parquet: Parquet để quét dữ liệu, người dùng cần tra theo khóa trong mili giây. Mỗi (thành phố, giờ) là một document đủ thông tin nên không cần join khi đọc. Upsert theo _id cố định nên Spark chạy lại không sinh bản trùng. CAP: replica set một primary là CP — ghi với w: majority không mất khi failover, đọc phải chờ khoảng 10 giây trong lúc bầu primary mới.");
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
  lead(S, t("Random Forest: the simplest model that learns interactions and scales on Spark",
    "Random Forest: mô hình đơn giản nhất học được tương tác và chạy tốt trên Spark"));
  const M = [["Persistence", t("baseline", "baseline"), t("rained > 50 mm in the last 24 h → yes", "24 h qua > 50 mm → có"), C.accent4],
    ["Climatology", t("baseline", "baseline"), t("how often per place and month", "tần suất theo nơi và tháng"), C.accent4],
    [t("Logistic reg.", "Hồi quy logistic"), t("not kept", "không giữ"), t("linear only", "chỉ tuyến tính"), C.accent4],
    ["Random Forest", t("★ main model", "★ mô hình chính"), t("many trees, averaged votes", "nhiều cây, lấy trung bình phiếu"), C.accent2],
    [t("Boosted trees", "Boosted trees"), t("future work", "hướng mở rộng"), t("sequential, overfits easily", "train tuần tự, dễ overfit"), C.accent4],
    ["LSTM", t("out of scope", "ngoài phạm vi"), t("needs GPU, hard to explain", "cần GPU, khó giải thích"), C.accent4]];
  const ay = 2.35, gap = 0.2, w = (W - 2 * MX - 5 * gap) / 6;
  arrow(S, C, MX, ay + 1.65, W - MX, ay + 1.65, { color: C.accent4, width: 1.5 });
  S.addText(t("simpler", "đơn giản"), { x: MX, y: ay + 1.72, w: 2, h: 0.3, fontSize: 11, italic: true, color: C.accent4, margin: 0, isTextBox: true });
  S.addText(t("more complex", "phức tạp hơn"), { x: W - MX - 2, y: ay + 1.72, w: 2, h: 0.3, fontSize: 11, italic: true, color: C.accent4, align: "right", margin: 0, isTextBox: true });
  M.forEach(([name, role, how, col], i) => {
    const x = MX + i * (w + gap), hl = i === 3, y = hl ? ay - 0.25 : ay;
    S.addShape("roundRect", { x, y, w, h: hl ? 1.7 : 1.45, rectRadius: 0.1, fill: { color: hl ? C.accent2 : C.background1 }, line: { color: col, width: hl ? 2 : 1.25 } });
    S.addText([{ text: name, options: { bold: true, fontSize: 15, color: hl ? C.background1 : C.text2, breakLine: true } },
      { text: role, options: { bold: true, fontSize: 12, color: hl ? C.background1 : col, breakLine: true } },
      { text: how, options: { fontSize: 11, color: hl ? C.background1 : C.text1 } }],
      { x: x + 0.08, y, w: w - 0.16, h: hl ? 1.7 : 1.45, align: "center", valign: "middle", margin: 0, isTextBox: true });
  });
  // vì sao RF
  [[t("Learns interactions", "Học tương tác"), t("“lots of rain in 72 h AND pressure falling”", "“mưa 72 h nhiều VÀ áp suất đang giảm”")],
   [t("Parallel in Spark ML", "Song song trên Spark ML"), t("each tree trains independently", "mỗi cây train độc lập")],
   [t("Explains itself", "Giải thích được"), t("feature importance per variable", "mức quan trọng của từng biến")]].forEach(([h, d], i) => {
    const x = MX + i * ((W - 2 * MX) / 3);
    S.addText([{ text: h, options: { bold: true, fontSize: 16, color: C.accent2, breakLine: true } }, { text: d, options: { fontSize: 12.5, color: C.text1 } }],
      { x, y: 4.95, w: (W - 2 * MX) / 3 - 0.2, h: 0.85, margin: 0, valign: "middle", isTextBox: true });
  });
  note(S, "Persistence and climatology are the baselines every model must beat. Logistic regression is fast and interpretable but only linear. Random Forest learns non-linear interactions, is robust, trains in parallel in Spark ML and gives feature importance; its weakness is that it is heavier than LR and its probabilities are not calibrated. Gradient-boosted trees are often slightly more accurate but train sequentially and overfit more easily — future work. LSTM needs a GPU, is hard to explain and does not fit a 3-week project. We do not claim RF is the most accurate possible model, only that it beats the baselines and fits Spark ML.",
    "Persistence và climatology là baseline mà mọi model phải vượt. Hồi quy logistic nhanh, dễ giải thích nhưng chỉ tuyến tính. Random Forest học được tương tác phi tuyến, ổn định, train song song trên Spark ML và có feature importance; nhược điểm là nặng hơn LR và xác suất chưa được hiệu chỉnh. Gradient-boosted trees thường chính xác hơn chút nhưng train tuần tự và dễ overfit — để làm sau. LSTM cần GPU, khó giải thích và không vừa dự án 3 tuần. Nhóm không khẳng định RF là chính xác nhất, chỉ là nó vượt baseline và phù hợp Spark ML.");
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
    { head: t("Train RF", "Train RF"), sub: t(`${man.model.numTrees} trees, depth ${man.model.maxDepth}, same for M1–M4`, `${man.model.numTrees} cây, độ sâu ${man.model.maxDepth}, giống nhau cho M1–M4`), num: t(`${man.cells} cells`, `${man.cells} ô`) },
    { head: t("Pick threshold", "Chọn ngưỡng"), sub: t("best F1 on validation 2023, then frozen", "F1 cao nhất trên validation 2023, rồi khóa lại"), num: t(`M4: ${f(man.thresholds_from_validation.M4, 2)}`, `M4: ${f(man.thresholds_from_validation.M4, 2)}`) },
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
  note(S, `Test 2024–2025: ${n(m4.n)} cell-hours, ${n(m4.positives)} heavy (${pct(prev, 2)}), so a random model scores PR-AUC ≈ ${f(prev, 3)}. M4 threshold ${f(m4.threshold, 3)} chosen on 2023: TP ${n(m4.tp)}, FP ${n(m4.fp)}, FN ${n(m4.fn)}; F1 ${f(m4.f1, 3)}, ROC-AUC ${f(m4.roc_auc, 3)}. Terrain (M2) and neighbours (M3) change PR-AUC by less than 0.002 and their CIs cross zero, so they are dropped. Recall ${pct(m4.recall)} is still below the 60% target; precision ${pct(m4.precision)} meets the 30% target.`,
    `Test 2024–2025: ${n(m4.n)} ô-giờ, ${n(m4.positives)} giờ mưa lớn (${pct(prev, 2)}), nên đoán ngẫu nhiên được PR-AUC ≈ ${f(prev, 3)}. Ngưỡng M4 ${f(m4.threshold, 3)} chọn trên 2023: TP ${n(m4.tp)}, FP ${n(m4.fp)}, FN ${n(m4.fn)}; F1 ${f(m4.f1, 3)}, ROC-AUC ${f(m4.roc_auc, 3)}. Địa hình (M2) và ô lân cận (M3) đổi PR-AUC chưa tới 0,002 và khoảng tin cậy cắt qua 0, nên bỏ. Recall ${pct(m4.recall)} vẫn dưới mục tiêu 60%; precision ${pct(m4.precision)} đạt mục tiêu 30%.`);
},

// ---------------- E7: biểu đồ Yagi
yagiChart: (IC) => {
  const yg = D.yagi || [];
  if (!yg.length) return;
  const S = slide("CONTENT", SE(), t("E7 · Typhoon Yagi replay: what the stream predicted", "E7 · Phát lại bão Yagi: streaming đã dự báo gì"));
  const locs = [...new Set(yg.map((r) => r.location_id))], times = [...new Set(yg.map((r) => r.ts))].sort();
  const lab = times.map((x) => x.slice(8, 10) + "/09 " + x.slice(11, 13) + "h");
  const ser = (loc, k) => times.map((x) => { const r = yg.find((y) => y.location_id === loc && y.ts === x); return r ? r[k] : null; });
  const thr = A.manifest.thresholds_from_validation[SEL];
  S.addChart(pres.ChartType.line, [...locs.map((l) => ({ name: `${l} P ${SEL}`, labels: lab, values: ser(l, "heavy_rain_probability") })),
    { name: t("threshold", "ngưỡng"), labels: lab, values: times.map(() => thr) }],
    Object.assign(CT(), { x: MX, y: 1.3, w: 7.6, h: 2.55, chartColors: [HEX.accent1, HEX.accent5, HEX.accent2], lineSize: 2, lineDataSymbol: "none",
      showLegend: true, legendPos: "r", showTitle: true, title: t("Predicted probability of > 50 mm in the next 24 h", "Xác suất dự báo mưa > 50 mm trong 24 h tới"),
      valAxisMinVal: 0, valAxisMaxVal: 1, catAxisLabelFrequency: 24, valAxisLabelFormatCode: "0.0" }));
  S.addChart(pres.ChartType.line, [...locs.map((l) => ({ name: t(`${l} observed`, `${l} thực tế`), labels: lab, values: ser(l, "rain_next_24h") })),
    { name: "50 mm", labels: lab, values: times.map(() => 50) }],
    Object.assign(CT(), { x: MX, y: 3.95, w: 7.6, h: 2.45, chartColors: [HEX.accent1, HEX.accent5, HEX.accent6], lineSize: 2, lineDataSymbol: "none",
      showLegend: true, legendPos: "r", showTitle: true, title: t("ERA5 rain in the following 24 h (mm) — ground truth", "Mưa ERA5 trong 24 h sau đó (mm) — giá trị thật"),
      valAxisMinVal: 0, catAxisLabelFrequency: 24 }));
  const tp = yg.filter((r) => r.label === 1 && r.is_heavy_rain_predicted).length, fp = yg.filter((r) => r.label === 0 && r.is_heavy_rain_predicted).length,
    fn = yg.filter((r) => r.label === 1 && !r.is_heavy_rain_predicted).length;
  big(S, 8.5, 1.35, 4.2, `${tp} / ${tp + fp}`, t("alerts were right: when it warned, it was raining heavily", "cảnh báo đúng: hễ báo là có mưa lớn thật"), C.accent3, 40);
  big(S, 8.5, 2.85, 4.2, t("6–7 Sep ✓", "6–7/9 ✓"), t("caught the landfall peak with high probability", "bắt được đỉnh mưa khi bão vào bờ, xác suất cao"), C.accent1, 34);
  big(S, 8.5, 4.25, 4.2, t("8–9 Sep ✗", "8–9/9 ✗"), t(`missed most rain after landfall (${fn} of ${tp + fn} heavy hours missed)`, `bỏ sót phần lớn mưa sau bão (sót ${fn}/${tp + fn} giờ mưa lớn)`), C.accent6, 34);
  source(S, C, PILOT());
  note(S, `${n(yg.length)} hourly predictions for Hai Phong and Quang Ninh whose label became known after t + 24 h. TP ${tp}, FP ${fp}, FN ${fn}. The model fires strongly on 6–7 September when the typhoon makes landfall. After landfall the local features see a weakening system, so the continuing rain on 8–9 September is mostly missed. Next step: features from 100–300 km away or upwind, rather than adjacent cells.`,
    `${n(yg.length)} dự báo theo giờ cho Hải Phòng và Quảng Ninh, nhãn có được sau t + 24 h. TP ${tp}, FP ${fp}, FN ${fn}. Model báo mạnh vào 6–7/9 khi bão đổ bộ. Sau đó feature tại chỗ chỉ thấy hệ thống đang suy yếu nên phần lớn mưa kéo dài 8–9/9 bị bỏ sót. Hướng tiếp: feature từ vùng cách 100–300 km hoặc phía đầu gió thay vì ô sát bên.`);
},

// ---------------- E7: mẫu dữ liệu, vẽ thành cột xác suất theo thời gian
yagiSample: (IC) => {
  const yr = D.samples.yagi_rows || [], thr = A.manifest.thresholds_from_validation[SEL];
  const S = slide("CONTENT", SE(), t("E7 data sample: stream predictions vs what happened", "Dữ liệu mẫu E7: dự báo của stream so với thực tế"));
  lead(S, t(`Hai Phong every 12 h: bar = predicted probability, line = alert threshold ${f(thr, 3)}`, `Hải Phòng mỗi 12 h: cột = xác suất dự báo, vạch = ngưỡng cảnh báo ${f(thr, 3)}`));
  const x0 = MX + 1.9, cw = (W - MX - x0) / Math.max(1, yr.length), base = 4.35, hmax = 2.3;
  [[t("probability", "xác suất"), 3.0], [t("time", "thời điểm"), base + 0.08], [t("rain next 24 h", "mưa 24 h tới"), base + 0.5], [t("result", "kết quả"), base + 1.0]].forEach(([k, y]) =>
    S.addText(k, { x: MX, y, w: 1.8, h: 0.4, fontSize: 12, bold: true, color: C.text2, margin: 0, valign: "middle", isTextBox: true }));
  S.addShape("line", { x: x0, y: base, w: W - MX - x0, h: 0, line: { color: C.accent4, width: 1 } });
  yr.forEach((r, i) => {
    const a = r.is_heavy_rain_predicted === true || r.is_heavy_rain_predicted === "True", tr = r.label === 1;
    const o = a && tr ? "TP" : a && !tr ? "FP" : !a && tr ? "FN" : "TN", oc = { TP: C.accent3, FP: C.accent6, FN: C.accent6, TN: C.accent4 }[o];
    const p = Number(r.heavy_rain_probability), x = x0 + i * cw, bw = cw * 0.55, h = Math.max(0.03, p * hmax);
    S.addShape("rect", { x: x + (cw - bw) / 2, y: base - h, w: bw, h, fill: { color: a ? C.accent2 : C.accent1 }, line: { color: a ? C.accent2 : C.accent1 } });
    S.addText(f(p, 3), { x, y: base - h - 0.32, w: cw, h: 0.3, fontSize: 11, bold: true, color: C.text1, align: "center", margin: 0, isTextBox: true });
    S.addText(String(r.ts).slice(8, 10) + "/09 " + String(r.ts).slice(11, 13) + "h", { x, y: base + 0.08, w: cw, h: 0.4, fontSize: 11, color: C.text1, align: "center", valign: "middle", margin: 0, isTextBox: true });
    S.addText(`${f(Number(r.rain_next_24h), 0)} mm`, { x, y: base + 0.5, w: cw, h: 0.4, fontSize: 13, bold: tr, color: tr ? C.accent5 : C.text1, align: "center", valign: "middle", margin: 0, isTextBox: true });
    S.addText(o, { shape: "roundRect", rectRadius: 0.06, x: x + cw * 0.15, y: base + 1.02, w: cw * 0.7, h: 0.38, fill: { color: oc }, color: C.background1,
      fontSize: 12, bold: true, align: "center", valign: "middle", margin: 0 });
  });
  const ty = base - thr * hmax;
  S.addShape("line", { x: x0, y: ty, w: W - MX - x0, h: 0, line: { color: C.accent6, width: 1.5, dashType: "dash" } });
  S.addText([{ text: "TP ", options: { bold: true, color: C.accent3 } }, { text: t("alert and heavy rain   ", "báo và có mưa lớn   ") },
    { text: "FN ", options: { bold: true, color: C.accent6 } }, { text: t("heavy rain but no alert   ", "có mưa lớn nhưng không báo   ") },
    { text: "TN ", options: { bold: true, color: C.accent4 } }, { text: t("no alert, no heavy rain", "không báo, không mưa lớn") }],
    { x: MX, y: 6.0, w: W - 2 * MX, h: 0.35, fontSize: 12.5, color: C.text1, margin: 0, isTextBox: true });
  source(S, C, PILOT());
  note(S, `Each column is one MongoDB document for Hai Phong, every 12 hours, with the truth computed only after the replay passed t + 24 h. Model ${SEL}, threshold ${f(thr, 3)} frozen from 2023. Alerts fire on the landfall days (TP); rain that continues after landfall, or that starts while the probability is still rising, is missed (FN).`,
    `Mỗi cột là một document MongoDB của Hải Phòng, cách 12 giờ, giá trị thật chỉ tính sau khi replay đã qua t + 24 h. Model ${SEL}, ngưỡng ${f(thr, 3)} khóa từ 2023. Cảnh báo bật đúng những ngày bão đổ bộ (TP); mưa kéo dài sau đó, hoặc bắt đầu khi xác suất còn đang tăng, bị bỏ sót (FN).`);
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
    [t("train = serve", "train = serve"), t("stream features equal batch features", "feature stream bằng feature batch"), t("parity test on a 96 h buffer", "parity test trên bộ đệm 96 h")],
    ["Δ = 0", t("for two identical models", "với hai model giống nhau"), t("PR-AUC checked on hand-computed cases", "PR-AUC kiểm bằng ví dụ tính tay")],
    [t("same rows", "cùng dòng"), t("baselines and models", "baseline và model"), t("same test rows, same threshold rule", "cùng dòng test, cùng cách chọn ngưỡng")],
    ["0", t("extra documents on re-run", "document thêm khi chạy lại"), t("MongoDB reads use indexes", "MongoDB đọc bằng index")],
  ];
  const gw = (W - 2 * MX - 3 * 0.25) / 4;
  items.forEach(([v, h, d], i) => {
    const x = MX + (i % 4) * (gw + 0.25), y = 1.35 + Math.floor(i / 4) * 2.55;
    S.addShape("roundRect", { x, y, w: gw, h: 2.3, rectRadius: 0.1, fill: { color: C.background2 }, line: { color: C.background2 } });
    S.addText(v, { x: x + 0.15, y: y + 0.15, w: gw - 0.3, h: 0.8, fontSize: 32, bold: true, color: C.accent3, margin: 0, valign: "middle", isTextBox: true, fit: "shrink" });
    S.addText([{ text: h, options: { bold: true, fontSize: 14, color: C.text2, breakLine: true } }, { text: d, options: { fontSize: 11.5, color: C.text1 } }],
      { x: x + 0.15, y: y + 1.0, w: gw - 0.3, h: 1.2, margin: 0, valign: "top", isTextBox: true });
  });
  note(S, "All tests run with pytest inside the Spark container. Row counts: bronze and silver match, and gold has no duplicate (cell, hour) keys. Independent truth: ERA5 rain compared with NOAA rain gauges. MapReduce and Spark SQL compute the same climatology table with zero differences. Leakage: unit tests modify future rows and assert past features do not change; the label window is exact and rows across split boundaries are purged. Train = serve: features built in the stream from a 96 h buffer equal the batch features. Metrics: average precision is checked on hand-computed examples, and identical models give a difference of exactly 0. Baselines use the same rows and the same threshold method. Serving: MongoDB reads use indexes (IXSCAN) and a re-run adds no documents.",
    "Mọi test chạy bằng pytest trong container Spark. Số dòng: bronze và silver khớp, gold không có khóa (ô, giờ) trùng. Đối chiếu độc lập: so mưa ERA5 với trạm đo mưa NOAA. MapReduce và Spark SQL tính ra cùng bảng climatology, không khác biệt. Rò rỉ: unit test sửa dữ liệu tương lai và kiểm tra feature quá khứ không đổi; cửa sổ nhãn chính xác, dòng nằm ở ranh giới split bị loại. Train = serve: feature tạo trong stream từ bộ đệm 96 giờ bằng feature batch. Thước đo: average precision kiểm bằng ví dụ tính tay, hai model giống nhau cho chênh lệch đúng bằng 0. Baseline dùng cùng dòng và cùng cách chọn ngưỡng. Phục vụ: MongoDB đọc bằng index (IXSCAN), chạy lại không thêm document.");
},

// ---------------- E6: thí nghiệm hiệu năng (dự kiến)
perf: (IC) => {
  const S = slide("CONTENT", SE(), t("E6 · Spark performance experiment (planned)", "E6 · Thí nghiệm hiệu năng Spark (dự kiến)"));
  pill(S, C, W - MX - 1.15, 0.55, "proposed");
  lead(S, t("Two knobs, measured on the full official data so the timings reflect the real scale",
    "Hai nút chỉnh, đo trên toàn bộ dữ liệu chính thức để thời gian phản ánh đúng quy mô thật"));
  const chips = (x, y, vals, col) => vals.forEach((v, i) => S.addText(v, { shape: "roundRect", rectRadius: 0.1, x: x + i * 1.35, y, w: 1.2, h: 0.75,
    fill: { color: C.background1 }, line: { color: col, width: 1.75 }, fontSize: 20, bold: true, color: col, align: "center", valign: "middle", margin: 0 }));
  const half = (W - 2 * MX - 0.4) / 2;
  [[t("1 · Shuffle partitions", "1 · Số shuffle partition"), ["4", "16", "64", "200"], C.accent1, t("gold feature build: rolling windows + join", "dựng feature gold: cửa sổ trượt + join")],
   [t("2 · Join strategy", "2 · Cách join"), ["Broadcast", "SortMerge"], C.accent2, t("query Q2: daily rain ⋈ cities ⋈ geography", "truy vấn Q2: mưa ngày ⋈ thành phố ⋈ địa lý")]].forEach(([h, vals, col, job], i) => {
    const x = MX + i * (half + 0.4);
    S.addText(h, { x, y: 2.1, w: half, h: 0.45, fontSize: 18, bold: true, color: col, margin: 0, isTextBox: true });
    if (vals.length > 2) chips(x, 2.75, vals, col);
    else vals.forEach((v, k) => S.addText(v, { shape: "roundRect", rectRadius: 0.1, x: x + k * 2.75, y: 2.75, w: 2.5, h: 0.75,
      fill: { color: C.background1 }, line: { color: col, width: 1.75 }, fontSize: 20, bold: true, color: col, align: "center", valign: "middle", margin: 0 }));
    S.addText(job, { x, y: 3.7, w: half, h: 0.4, fontSize: 13, color: C.text1, margin: 0, isTextBox: true });
  });
  flow(S, 4.55, [
    { head: t("Warm up", "Làm nóng"), sub: t("1 run, not timed", "1 lần, không tính giờ") },
    { head: t("Time", "Đo"), sub: t("3 runs per setting, noop sink", "3 lần mỗi mức, sink noop") },
    { head: t("Report", "Báo cáo"), sub: t("time + plan + hardware + Spark version", "thời gian + plan + phần cứng + phiên bản Spark") },
  ], 1.05);
  note(S, "Factor 1 changes spark.sql.shuffle.partitions over 4, 16, 64 and 200 with adaptive query execution turned off, on the gold feature build (rolling windows plus the neighbour join). Factor 2 forces BroadcastHashJoin versus SortMergeJoin on query Q2. Each setting gets one warm-up and three timed runs writing to a noop sink. The script is ready; results will be reported with hardware, Spark version, row count, both query plans and the observed trend.",
    "Yếu tố 1 đổi spark.sql.shuffle.partitions qua 4, 16, 64 và 200, tắt adaptive query execution, trên bước dựng feature gold (cửa sổ trượt và join ô lân cận). Yếu tố 2 ép BroadcastHashJoin so với SortMergeJoin trên truy vấn Q2. Mỗi mức chạy 1 lần làm nóng và 3 lần đo, ghi ra sink noop. Script đã sẵn sàng; kết quả sẽ báo cáo kèm phần cứng, phiên bản Spark, số dòng, cả hai query plan và xu hướng quan sát được.");
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
   [t(`Streaming demo still uses pilot ${SEL}`, `Demo streaming vẫn dùng ${SEL} pilot`), t("plug the official M4 into the stream", "gắn M4 chính thức vào stream")],
   [t("ERA5 arrives ~5 days late", "ERA5 trễ khoảng 5 ngày"), t("Forecast API producer for a live stream", "producer từ Forecast API cho stream thật")],
   [t("Spark in local mode; silver/gold not on HDFS", "Spark chạy local; silver/gold chưa lên HDFS"), t("run on YARN, lake on HDFS; run E6", "chạy trên YARN, lake trên HDFS; chạy E6")]].forEach(([a, b], i) => {
    const y = 1.8 + i * 0.75;
    S.addShape("roundRect", { x: MX, y, w: 5.6, h: 0.6, rectRadius: 0.08, fill: { color: "F8E9EB" }, line: { color: "F8E9EB" } });
    S.addText(a, { x: MX + 0.2, y, w: 5.3, h: 0.6, fontSize: 13.5, bold: true, color: C.text2, valign: "middle", margin: 0, isTextBox: true });
    arrow(S, C, MX + 5.7, y + 0.3, 6.9, y + 0.3, { color: C.accent2, width: 1.75 });
    S.addShape("roundRect", { x: 7.0, y, w: W - MX - 7.0, h: 0.6, rectRadius: 0.08, fill: { color: C.background2 }, line: { color: C.background2 } });
    S.addText(b, { x: 7.2, y, w: W - MX - 7.4, h: 0.6, fontSize: 13.5, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
  });
  note(S, `The model now finds about ${pct(m4.recall)} of heavy-rain hours with ${pct(m4.precision)} precision; to reach 60% recall the threshold can be chosen for recall instead of F1, and probabilities calibrated. The Yagi replay shows that rain after landfall is missed, so context from farther away or upwind is the next feature idea. The label is ERA5 on a 25 km cell, which reaches 50 mm on only ${pct(no.hit / no.heavy_days)} of rain-gauge heavy-rain days. The streaming demo and MongoDB evidence still come from the pilot model; the official M4 goes into the same lane. ERA5 is published about 5 days late, so live use needs a Forecast API producer and re-validation. Spark ran in local mode and silver/gold are on local disk; the next step is YARN with the lake on HDFS, where the E6 performance experiment will run.`,
    `Model hiện bắt được khoảng ${pct(m4.recall)} số giờ mưa lớn với precision ${pct(m4.precision)}; để đạt recall 60% có thể chọn ngưỡng theo recall thay vì F1 và hiệu chỉnh xác suất. Phát lại Yagi cho thấy mưa sau khi bão vào bờ bị bỏ sót, nên hướng feature tiếp là thông tin từ xa hơn hoặc phía đầu gió. Nhãn là ERA5 trên ô 25 km, chỉ đạt 50 mm ở ${pct(no.hit / no.heavy_days)} số ngày mưa lớn tại trạm đo. Demo streaming và bằng chứng MongoDB vẫn từ model pilot; M4 chính thức gắn vào cùng làn này. ERA5 công bố trễ khoảng 5 ngày nên chạy thật cần producer từ Forecast API và kiểm chứng lại. Spark chạy local, silver/gold nằm trên ổ cục bộ; bước tiếp là chạy trên YARN với lake trên HDFS, và chạy thí nghiệm hiệu năng E6 ở đó.`);
},

// ---------------- Kết luận + phân công
conclusions: (IC) => {
  const m4 = R("M4"), per = R("persistence_rain24h"), d4 = DL("M4 - M1"), d2 = DL("M2 - M1"), d3 = DL("M3 - M1");
  const S = slide("CONTENT", SE(), t("Conclusions and team contributions", "Kết luận và phân công"));
  // chuỗi pipeline
  const chain = ["CDS ERA5", "raw · bronze (HDFS)", "silver", "gold", "Spark ML", "Streaming", "MongoDB"];
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
   ["T", t("Mr. Thinh", "Mr. Thịnh"), t("Streaming & serving", "Streaming & phục vụ"), t("replay producer, Structured Streaming, MongoDB model + read-back", "bộ phát lại, Structured Streaming, MongoDB + đọc lại")]].forEach(([ini, who, role, what], i) => {
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
