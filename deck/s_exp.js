// Section 3 — experiments & results.
const K = require("./common");
const { pres, C, D, A, HEX, W, H, MX, t, f, pct, n, slide, card, bullets, table, source, pill, iconCircle, CT, SEL, nLoc, test, delta, verdict } = K;
const SEC = () => t("Experiments & results", "Thực nghiệm & kết quả");
const P8 = D.ablation_pilot8;
const lolo = A.lolo_results ? { res: A.lolo_results, del: A.lolo_deltas, per: A.lolo_per_location } : null;
const st = (k) => ({ text: { done: t("Done", "Xong"), proposed: t("Planned", "Dự kiến") }[k], options: { bold: true, color: C[{ done: "accent3", proposed: "accent4" }[k]] } });
// tên tỉnh cũ của từng điểm pilot (mã như HGIA khó đọc trên slide)
const PNAME = Object.fromEntries(require("fs").readFileSync(require("path").join(K.ROOT, "data", "reference", "points.csv"), "utf8")
  .trim().split(/\r?\n/).slice(1).map((l) => l.split(",")).map((c) => [c[0], c[1]]));
const pname = (id) => PNAME[id] || id;
const dcol = (d) => (d.ci_lo > 0 ? C.accent3 : d.ci_hi < 0 ? C.accent6 : C.text1);

module.exports = [
(IC) => {
  const S = slide("SECTION_DARK", SEC(), t("Results, demo and validation", "Kết quả, demo và kiểm chứng"));
  S.addText(t("Performance plan, the Typhoon Yagi replay, how we checked correctness, limits and next steps",
    "Kế hoạch đo hiệu năng, demo phát lại bão Yagi, cách kiểm chứng, giới hạn và bước tiếp theo"), { placeholder: "body" });
},

// plan
(IC) => {
  const S = slide("CONTENT", SEC(), t("Experiment plan: trust the label, then choose the features", "Kế hoạch thực nghiệm: tin được nhãn, rồi chọn feature"));
  // cột cuối = kết luận của từng thí nghiệm (tin được / có giúp không), không phải "xong / chưa"
  const OFF = JSON.parse(require("fs").readFileSync(require("path").join(K.ROOT, "reports", "official", "deck_official.json"), "utf8"));
  const V = (text, kind) => ({ text, options: { bold: true, color: { yes: C.accent3, no: C.accent6, mixed: C.accent2, todo: C.accent4 }[kind] } });
  const gain = (d) => (d.ci_lo > 0 ? V(t(`Helps (Δ ${f(d.delta_pr_auc, 3)})`, `Có giúp (Δ ${f(d.delta_pr_auc, 3)})`), "yes")
    : d.ci_hi < 0 ? V(t(`Worse (Δ ${f(d.delta_pr_auc, 3)})`, `Kém đi (Δ ${f(d.delta_pr_auc, 3)})`), "no")
    : V(t(`No gain (Δ ${f(d.delta_pr_auc, 3)})`, `Không giúp (Δ ${f(d.delta_pr_auc, 3)})`), "no"));
  const m1 = test("M1"), per = test("persistence_rain24h"), d21 = delta("M2 - M1"), d31 = delta("M3 - M1");
  // E9 chạy trên dữ liệu chính thức (src.experiments.official_m4); chưa có file thì ghi "chưa chạy"
  const e9f = require("path").join(K.ROOT, "reports", "official", "model", "e9_manifest.json");
  const E9 = require("fs").existsSync(e9f) ? JSON.parse(require("fs").readFileSync(e9f, "utf8")) : null;
  const l21 = lolo ? lolo.del.find((d) => d.comparison === "M2 - M1") : null;
  const p8 = P8 ? P8.deltas.find((d) => d.comparison === "M2 - M1") : null;
  const OA = D.official_ablation, ores = (m) => OA.results.find((r) => r.model === m && r.split === "test"), odel = (c) => OA.deltas.find((d) => d.comparison === c);
  table(S, C, [["#", t("Question", "Câu hỏi"), t("What changes (all else fixed)", "Thay đổi gì (giữ nguyên phần còn lại)"), t("Data", "Dữ liệu"), t("Result", "Kết luận")],
    ["E0", t("Can we trust ERA5 as the label?", "ERA5 có dùng làm nhãn được không?"), t("ERA5 cell vs 14 NOAA gauges", "Ô ERA5 so với 14 trạm NOAA"), t("CDS 2015–2025", "CDS 2015–2025"),
      V(t(`Trusted as a ~26 km label (r ${f(D.noaa.median_corr_vn, 2)})`, `Tin được, hiểu là nhãn trên ô ~26 km (r ${f(D.noaa.median_corr_vn, 2)})`), "mixed")],
    ["E1", t("Does local weather predict heavy rain?", "Thời tiết tại chỗ có dự báo được mưa lớn?"), t("M1 vs persistence (rain of the last 24 h)", "M1 so với persistence (mưa 24 h qua)"), t("CDS, 483 cells", "CDS, 483 ô"),
      V(t(`Yes: PR-AUC ${f(ores("M1").pr_auc, 3)} vs ${f(ores("persistence_rain24h").pr_auc, 3)}`, `Có: PR-AUC ${f(ores("M1").pr_auc, 3)} so với ${f(ores("persistence_rain24h").pr_auc, 3)}`), "yes")],
    ["E2", t("H1: does terrain help?", "H1: địa hình có giúp?"), t("M2 = M1 + elevation, coast distance, slope", "M2 = M1 + độ cao, cách biển, độ dốc"), t("CDS, 483 cells", "CDS, 483 ô"), gain(odel("M2 - M1"))],
    ["E3", t("H2: do neighbouring cells help?", "H2: ô lân cận có giúp?"), t("M3 = M2 + 4 neighbour features", "M3 = M2 + 4 feature lân cận"), t("CDS, 483 cells", "CDS, 483 ô"), gain(odel("M3 - M1"))],
    ["E4", t("Was terrain variety the bottleneck?", "Có phải do thiếu đa dạng địa hình?"), t(`Add highland points: ${P8 ? P8.locations.length : 8} → ${nLoc}`, `Thêm điểm vùng cao: ${P8 ? P8.locations.length : 8} → ${nLoc}`), t("pilot", "pilot"),
      p8 ? V(t("No: same result with more points", "Không: thêm điểm vẫn vậy"), "no") : V(t("Not run", "Chưa chạy"), "todo")],
    ["E5", t("Does terrain help at unseen places?", "Địa hình có giúp ở nơi chưa thấy?"), t("Leave-location-out: train on the other points", "Giữ riêng từng điểm: train trên các điểm còn lại"), t("pilot", "pilot"),
      l21 ? gain(l21) : V(t("Not run", "Chưa chạy"), "todo")],
    ["E6", t("H3: does the atmosphere help?", "H3: khí quyển có giúp?"), t("M4 = M1 + 9 features from 6 CDS column variables", "M4 = M1 + 9 feature từ 6 biến cột khí quyển CDS"), t("CDS, 483 cells", "CDS, 483 ô"), gain(odel("M4 - M1"))],
  ], { x: MX, y: 1.35, w: W - 2 * MX, colW: [0.5, 3.15, 3.9, 1.65, 2.93], fontSize: 11.5 });
  source(S, C, t("Result colours: green = yes / helps, red = no gain or worse, orange = trusted with limits. Official data: 483 land cells, train 2015–2022, test 2024–2025. Pilot: 12 points, 2019–2025.",
    "Màu kết luận: xanh = có / có giúp, đỏ = không giúp hoặc kém đi, cam = tin được nhưng có giới hạn. Dữ liệu chính thức: 483 ô đất liền, train 2015–2022, test 2024–2025. Pilot: 12 điểm, 2019–2025."));
},

// E1
(IC) => {
  const m1 = test("M1"), per = test("persistence_rain24h"), cli = test("climatology_loc_month");
  const S = slide("CONTENT", SEC(), t("E1 · The local-weather model clearly beats the baselines", "E1 · Mô hình thời tiết tại chỗ vượt rõ các baseline"));
  if (D.pr_curves) {
    const lab = D.pr_curves.M1.map((p) => p.recall.toFixed(2));
    S.addChart(pres.ChartType.line, [{ name: t("M1 Random Forest", "M1 Random Forest"), labels: lab, values: D.pr_curves.M1.map((p) => p.precision) },
      { name: "Persistence", labels: lab, values: D.pr_curves.persistence_rain24h.map((p) => p.precision) }],
      Object.assign(CT(), { x: MX, y: 1.3, w: 7.3, h: 5.0, chartColors: [HEX.accent1, HEX.accent4], lineSize: 2.5, lineDataSymbol: "none",
        showLegend: true, legendPos: "b", showTitle: true, title: t("Precision–recall on test 2024–2025", "Precision–recall trên test 2024–2025"),
        valAxisMinVal: 0, valAxisMaxVal: 1, valAxisLabelFormatCode: "0.0", catAxisTitle: "Recall", showCatAxisTitle: true,
        valAxisTitle: "Precision", showValAxisTitle: true, catAxisLabelFrequency: 8 }));
  }
  const big = (x, y, v, l, col) => {
    S.addText(v, { x, y, w: 2.6, h: 0.8, fontSize: 36, bold: true, color: C[col], margin: 0, isTextBox: true });
    S.addText(l, { x, y: y + 0.8, w: 2.6, h: 0.5, fontSize: 12, color: C.text1, margin: 0, valign: "top", isTextBox: true });
  };
  big(8.2, 1.4, f(m1.pr_auc), "PR-AUC M1", "accent1");
  big(10.5, 1.4, f(per.pr_auc), t(`persistence (climatology ${f(cli.pr_auc)})`, `persistence (climatology ${f(cli.pr_auc)})`), "accent4");
  big(8.2, 2.85, pct(m1.recall), t("recall — target ≥ 60%", "recall — mục tiêu ≥ 60%"), m1.recall >= 0.6 ? "accent3" : "accent2");
  big(10.5, 2.85, pct(m1.precision), t("precision — target ≥ 30%", "precision — mục tiêu ≥ 30%"), m1.precision >= 0.3 ? "accent3" : "accent2");
  S.addText(t(`Test: ${n(m1.n)} point-hours, ${n(m1.positives)} positive (${pct(m1.prevalence, 1)}); a random model scores PR-AUC = ${f(m1.prevalence)}. Threshold ${f(m1.threshold)} from 2023. TP ${n(m1.tp)} · FP ${n(m1.fp)} · FN ${n(m1.fn)}.`,
    `Test: ${n(m1.n)} điểm-giờ, ${n(m1.positives)} dòng dương (${pct(m1.prevalence, 1)}); đoán ngẫu nhiên được PR-AUC = ${f(m1.prevalence)}. Ngưỡng ${f(m1.threshold)} chọn trên 2023. TP ${n(m1.tp)} · FP ${n(m1.fp)} · FN ${n(m1.fn)}.`),
    { x: 8.2, y: 4.4, w: 4.53, h: 1.9, fontSize: 13, color: C.text1, margin: 0, valign: "top", isTextBox: true });
  source(S, C, "reports/ablation_results.csv · reports/figures/_predictions_eval.parquet");
},

// E2–E5 decision
(IC) => {
  const OA = D.official_ablation;
  const S = slide("CONTENT", SEC(), t("Feature selection: only the atmosphere helps", "Chọn feature: chỉ khí quyển có giúp"));
  // chỉ kết quả chính thức: từ M1, thêm từng nhóm feature, nhóm nào giúp thì nói giúp
  const ores = (m) => OA.results.find((r) => r.model === m && r.split === "test");
  const dl = (c) => OA.deltas.find((d) => d.comparison === c);
  S.addText(t("Start from local weather, add one group of features at a time, keep only what clearly helps",
    "Bắt đầu từ thời tiết tại chỗ, thêm từng nhóm feature, chỉ giữ nhóm nào giúp rõ ràng"),
    { x: MX, y: 1.2, w: W - 2 * MX, h: 0.5, fontSize: 18, bold: true, color: C.text2, margin: 0, isTextBox: true });
  const rows = [
    ["M1", t("Local weather", "Thời tiết tại chỗ"), t("rain, humidity, pressure, wind, temperature, cloud", "mưa, độ ẩm, áp suất, gió, nhiệt độ, mây"), null],
    ["M2", t("+ Terrain", "+ Địa hình"), t("elevation, distance to coast, slope", "độ cao, khoảng cách tới biển, độ dốc"), dl("M2 - M1")],
    ["M3", t("+ Neighbouring cells", "+ Ô lân cận"), t("rain and wind in the cells around", "mưa và gió ở các ô xung quanh"), dl("M3 - M1")],
    ["M4", t("+ Atmosphere", "+ Khí quyển"), t("water vapour, moisture flow, CAPE, K-index", "hơi nước, dòng ẩm, CAPE, chỉ số K"), dl("M4 - M1")],
  ];
  const bx = 5.0, sc = 4.4 / 0.45, base = ores("M1").pr_auc;
  rows.forEach(([m, name, what, d], i) => {
    const y = 2.55 + i * 0.8, v = ores(m).pr_auc, good = d && d.ci_lo > 0, col = !d ? C.accent1 : good ? C.accent3 : C.accent4;
    S.addText([{ text: name, options: { bold: true, fontSize: 17, color: !d ? C.text2 : good ? C.accent3 : C.text2, breakLine: true } }, { text: what, options: { fontSize: 11.5, color: C.text1 } }],
      { x: MX, y, w: 4.25, h: 0.8, margin: 0, valign: "middle", isTextBox: true });
    S.addShape("rect", { x: bx, y: y + 0.2, w: v * sc, h: 0.4, fill: { color: col }, line: { color: col } });
    S.addText(f(v), { x: bx + v * sc + 0.1, y: y + 0.15, w: 0.8, h: 0.5, fontSize: 15, bold: true, color: col, margin: 0, valign: "middle", isTextBox: true });
    const verdict = !d ? t("starting point", "điểm xuất phát") : good ? t(`helps  +${f(d.delta_pr_auc, 3)}`, `có ích  +${f(d.delta_pr_auc, 3)}`) : t("no gain", "không giúp");
    S.addText(verdict, { shape: "roundRect", rectRadius: 0.08, x: 10.75, y: y + 0.15, w: W - MX - 10.75, h: 0.5,
      fill: { color: !d ? C.background2 : good ? C.accent3 : C.background2 }, line: { color: !d ? C.background2 : good ? C.accent3 : C.background2 },
      fontSize: 14, bold: true, color: good ? C.background1 : C.text2, align: "center", valign: "middle", margin: 0 });
  });
  S.addShape("line", { x: bx + base * sc, y: 2.6, w: 0, h: 3.15, line: { color: C.accent1, width: 1, dashType: "dash" } });
  // chú thích trục: đây là PR-AUC, càng cao càng tốt; vạch đứt = mức của M1
  const prev = ores("M1").positives / ores("M1").n;
  S.addText([{ text: t("PR-AUC on test 2024–2025", "PR-AUC trên test 2024–2025"), options: { bold: true, color: C.text2, breakLine: true } },
    { text: t(`higher is better · random guess ≈ ${f(prev, 3)}`, `càng cao càng tốt · đoán ngẫu nhiên ≈ ${f(prev, 3)}`), options: { color: C.text1, fontSize: 11 } }],
    { x: bx, y: 1.95, w: 5.6, h: 0.55, fontSize: 13, margin: 0, valign: "bottom", isTextBox: true });
  S.addText(t("dashed line = M1 level", "vạch đứt = mức của M1"), { x: bx + base * sc - 1.5, y: 5.75, w: 3.0, h: 0.3, fontSize: 11, italic: true, color: C.accent1, align: "center", margin: 0, isTextBox: true });
  S.addText([{ text: t("Decision: ", "Quyết định: "), options: { color: C.accent2 } },
    { text: t("final model M4 = local weather + atmosphere (33 features).", "model cuối M4 = thời tiết tại chỗ + khí quyển (33 feature).") }],
    { x: MX, y: 6.15, w: W - 2 * MX, h: 0.45, fontSize: 18, bold: true, color: C.text2, margin: 0, isTextBox: true });
  const d2 = dl("M2 - M1"), d3 = dl("M3 - M1"), d4 = dl("M4 - M1");
  const pd = (x, c) => { const d = x && x.find((e) => e.comparison === c); return d ? `${f(d.delta_pr_auc, 3)} (CI ${f(d.ci_lo, 3)} to ${f(d.ci_hi, 3)})` : "–"; };
  S.addNotes(t(`Official run, 483 cells, test 2024–2025, block bootstrap by day with 500 paired resamples. Terrain: +${f(d2.delta_pr_auc, 3)}, CI ${f(d2.ci_lo, 3)} to ${f(d2.ci_hi, 3)} — crosses zero, no real change. Neighbouring cells: +${f(d3.delta_pr_auc, 3)}, CI ${f(d3.ci_lo, 3)} to ${f(d3.ci_hi, 3)} — no real change. Atmosphere: +${f(d4.delta_pr_auc, 3)}, CI ${f(d4.ci_lo, 3)} to ${f(d4.ci_hi, 3)} — entirely above zero, the only group kept. The pilot (Open-Meteo points, 2019–2025) gave the same answer for terrain: 8 lowland points ${pd(P8 && P8.deltas, "M2 - M1")}, 12 points incl. 4 highland ${pd(A.deltas, "M2 - M1")}, and on unseen points (leave-one-point-out) terrain was worse ${pd(lolo && lolo.del, "M2 - M1")}.`,
    `Bản chính thức, 483 ô, test 2024–2025, bootstrap theo khối ngày 500 lần ghép cặp. Địa hình: +${f(d2.delta_pr_auc, 3)}, khoảng tin cậy ${f(d2.ci_lo, 3)} đến ${f(d2.ci_hi, 3)} — cắt qua 0, không khác biệt. Ô lân cận: +${f(d3.delta_pr_auc, 3)}, khoảng tin cậy ${f(d3.ci_lo, 3)} đến ${f(d3.ci_hi, 3)} — không khác biệt. Khí quyển: +${f(d4.delta_pr_auc, 3)}, khoảng tin cậy ${f(d4.ci_lo, 3)} đến ${f(d4.ci_hi, 3)} — nằm hẳn trên 0, là nhóm duy nhất được giữ. Pilot (điểm Open-Meteo, 2019–2025) cho cùng kết luận về địa hình: 8 điểm đồng bằng ${pd(P8 && P8.deltas, "M2 - M1")}, 12 điểm gồm 4 điểm vùng cao ${pd(A.deltas, "M2 - M1")}, và với điểm chưa thấy khi train (giữ riêng từng điểm) địa hình còn kém đi ${pd(lolo && lolo.del, "M2 - M1")}.`));
},

// sample: gold rows + predictions
(IC) => {
  const SM = D.samples, gr = SM.gold_rows;
  const S = slide("CONTENT", SEC(), t("Pilot sample: gold rows and what each model predicts", "Mẫu pilot: dòng gold và dự báo của từng mô hình"));
  const HD = (txt, fill) => ({ text: txt, options: { bold: true, color: C.background1, fill: { color: fill }, align: "center", valign: "middle", fontSize: 10.5 } });
  const G = (txt, span, fill) => ({ text: txt, options: { colspan: span, bold: true, color: C.background1, fill: { color: fill }, align: "center", fontSize: 11.5 } });
  const c1 = HEX.accent1, c2 = HEX.accent5, c3 = HEX.accent2, c0 = HEX.dk2;
  const body = gr.map((r) => {
    const hot = r.label === 1, fill = { color: hot ? "FDF1E7" : "FFFFFF" };
    const v = (x, d) => ({ text: f(x, d), options: { align: "center", fill, fontSize: 12, color: C.text1 } });
    return [{ text: r.ts, options: { fill, fontSize: 12, bold: true, color: C.text2 } }, v(r.rain_3h, 1), v(r.rain_24h, 1), v(r.rh_mean_6h, 0), v(r.pressure_change_24h, 1),
      v(r.elevation_m, 0), v(r.distance_to_coast_km, 1), v(r.neighbor_rain_sum_6h, 1), v(r.neighbor_pressure_change_24h, 1), v(r.rain_next_24h, 1),
      { text: hot ? t("1 · heavy", "1 · mưa lớn") : t("0", "0"), options: { bold: true, align: "center", fill, fontSize: 12, color: hot ? C.accent2 : C.text1 } }];
  });
  S.addTable([
    [G(t("Hai Phong, Typhoon Yagi 2024", "Hải Phòng, bão Yagi 2024"), 1, c0), G(t("M1 · local weather", "M1 · thời tiết tại chỗ"), 4, c1),
      G(t("M2 adds · terrain", "M2 thêm · địa hình"), 2, c2), G(t("M3 adds · neighbours", "M3 thêm · ô lân cận"), 2, c3), G(t("Label", "Nhãn"), 2, c0)],
    [HD(t("Hour", "Giờ"), c0), HD(t("Rain 3 h\n(mm)", "Mưa 3 h\n(mm)"), c1), HD(t("Rain 24 h\n(mm)", "Mưa 24 h\n(mm)"), c1), HD(t("Humidity 6 h\n(%)", "Độ ẩm 6 h\n(%)"), c1),
      HD(t("Pressure Δ 24 h\n(hPa)", "Δ áp suất 24 h\n(hPa)"), c1), HD(t("Elevation\n(m)", "Độ cao\n(m)"), c2), HD(t("To coast\n(km)", "Cách biển\n(km)"), c2),
      HD(t("Rain 6 h\n(mm)", "Mưa 6 h\n(mm)"), c3), HD(t("Pressure Δ 24 h\n(hPa)", "Δ áp suất 24 h\n(hPa)"), c3), HD(t("Next 24 h\n(mm)", "24 h tới\n(mm)"), c0), HD(t("> 50 mm?", "> 50 mm?"), c0)],
    ...body], { x: MX, y: 1.25, w: W - 2 * MX, colW: [1.25, 1.0, 1.0, 1.15, 1.25, 0.95, 0.95, 1.0, 1.25, 1.05, 1.28], rowH: [0.5, 0.5, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3],
    border: { type: "solid", pt: 0.5, color: "D5E2E4" }, margin: 0.05, valign: "middle" });
  S.addText(t("Same test hours, three models: predicted chance of heavy rain in the next 24 h",
    "Cùng các giờ trên tập test, ba mô hình: xác suất dự báo có mưa lớn trong 24 giờ tới"),
    { x: MX, y: 4.25, w: 7.6, h: 0.3, fontSize: 12.5, bold: true, color: C.text2, margin: 0, isTextBox: true });
  const pc = (v) => ({ text: f(v, 2), options: { align: "center" } });
  table(S, C, [[t("Point", "Điểm"), t("Hour (test)", "Giờ (test)"), t("Heavy rain happened?", "Có mưa lớn thật?"), "M1", t("M2 (+ terrain)", "M2 (+ địa hình)"), t("M3 (+ neighbours)", "M3 (+ ô lân cận)")],
    ...SM.predictions.slice(0, 4).map((r) => [pname(r.location_id), r.ts,  // 2 điểm × (1 giờ có mưa lớn, 1 giờ không) cho vừa slide
      { text: r.label === 1 ? t("yes", "có") : t("no", "không"), options: { bold: true, align: "center", color: r.label === 1 ? C.accent2 : C.text1 } },
      pc(r.M1), pc(r.M2), pc(r.M3)])], { x: MX, y: 4.6, w: 7.6, colW: [1.35, 1.75, 1.3, 0.9, 1.15, 1.15], rowH: 0.27, fontSize: 11, headSize: 10.5 });
  card(S, C, 8.5, 4.25, 4.23, 2.1, { title: t("How to read it", "Cách đọc"), titleSize: 14, body: bullets([
    t("Top: one row per hour; the label looks only at the next 24 h", "Bảng trên: mỗi dòng một giờ; nhãn chỉ nhìn 24 giờ tới"),
    t("Bottom: every model scores exactly the same rows, so they can be compared fairly", "Bảng dưới: mọi mô hình chấm đúng cùng các dòng, nên so sánh công bằng"),
    t("Terrain columns never change for a point (Hai Phong: 8 m)", "Cột địa hình không đổi theo giờ (Hải Phòng: 8 m)"),
  ], 12) });
  source(S, C, "gold/features_hourly (Parquet) · reports/figures/_predictions_eval.parquet");
},

// E5 per point
(IC) => {
  if (!lolo) return;
  const geo = Object.fromEntries(D.geography.map((g) => [g.location_id, g])), per = {};
  lolo.per.forEach((r) => { (per[r.location_id] = per[r.location_id] || { pos: r.positives })[r.model] = r.pr_auc; });
  const locs = Object.keys(per).sort((a, b) => geo[a].elevation_m - geo[b].elevation_m);
  const S = slide("CONTENT", SEC(), t("E5 · Does terrain help at a place the model has never seen?", "E5 · Địa hình có giúp ở nơi mô hình chưa từng thấy?"));
  S.addText(t("Each row: train on the other 11 points, then test 2024–2025 on this point only. Points sorted by elevation.",
    "Mỗi dòng: train trên 11 điểm còn lại, rồi test 2024–2025 riêng trên điểm này. Các điểm xếp theo độ cao."),
    { x: MX, y: 1.2, w: 8.3, h: 0.35, fontSize: 12.5, italic: true, color: C.text1, margin: 0, isTextBox: true });
  table(S, C, [[t("Point kept out", "Điểm giữ riêng"), t("Elevation (m)", "Độ cao (m)"), t("Heavy-rain hours (test)", "Giờ mưa lớn (test)"),
    t("PR-AUC M1", "PR-AUC M1"), t("PR-AUC M2 (+ terrain)", "PR-AUC M2 (+ địa hình)"), t("Change", "Thay đổi")],
    ...locs.map((l) => { const p = per[l], dd = p.M2 - p.M1, few = p.pos < 30;
      return [{ text: pname(l), options: { bold: true } }, { text: f(geo[l].elevation_m, 0), options: { align: "center" } },
        { text: few ? `${n(p.pos)} *` : n(p.pos), options: { align: "center" } }, { text: f(p.M1, 2), options: { align: "center" } },
        { text: f(p.M2, 2), options: { align: "center" } },
        { text: few ? "–" : Math.abs(dd) < 0.005 ? "0.00" : `${dd > 0 ? "+" : "−"}${f(Math.abs(dd), 2)}`, options: { bold: true, align: "center",
          color: few ? C.accent4 : dd > 0.005 ? C.accent3 : dd < -0.005 ? C.accent6 : C.text1 } }]; })],
    { x: MX, y: 1.65, w: 8.3, colW: [1.75, 1.15, 1.6, 1.15, 1.6, 1.05], fontSize: 11.5 });
  const dl = lolo.del.find((d) => d.comparison === "M2 - M1"), r1 = lolo.res.find((r) => r.model === "M1"), r2 = lolo.res.find((r) => r.model === "M2");
  const worse = locs.filter((l) => per[l].pos >= 30 && per[l].M2 - per[l].M1 < -0.005).length, valid = locs.filter((l) => per[l].pos >= 30).length;
  S.addText(t("Answer: no — terrain makes it worse", "Trả lời: không — địa hình làm kém đi"),
    { x: 9.3, y: 1.3, w: 3.43, h: 0.75, fontSize: 18, bold: true, color: C.accent6, margin: 0, valign: "top", isTextBox: true });
  card(S, C, 9.3, 2.15, 3.43, 4.15, { title: t("What the table shows", "Bảng cho thấy gì"), titleSize: 14, body: bullets([
    t(`Adding terrain lowers PR-AUC at ${worse} of ${valid} points`, `Thêm địa hình làm PR-AUC giảm ở ${worse}/${valid} điểm`),
    t(`All points together: ${f(r1.pr_auc, 3)} (M1) → ${f(r2.pr_auc, 3)} (M2), change ${f(dl.delta_pr_auc, 3)}; the 95% CI is below zero`,
      `Gộp mọi điểm: ${f(r1.pr_auc, 3)} (M1) → ${f(r2.pr_auc, 3)} (M2), thay đổi ${f(dl.delta_pr_auc, 3)}; CI 95% nằm dưới 0`),
    t("Mountain points (Ha Giang, Kon Tum, Lai Chau) do not gain either: elevation only tells the model which point it is", "Điểm vùng núi (Hà Giang, Kon Tum, Lai Châu) cũng không được lợi: độ cao chỉ cho mô hình biết đây là điểm nào"),
    t("* too few heavy-rain hours to score", "* quá ít giờ mưa lớn để chấm điểm"),
  ], 12) });
  source(S, C, t("Each fold: train + threshold on the other points, test 2024–2025 on the held-out point", "Mỗi vòng: train + chọn ngưỡng trên các điểm còn lại, test 2024–2025 trên điểm bị giữ riêng"));
},

// E6 planned
(IC) => {
  const S = slide("CONTENT", SEC(), t("E6 · Spark performance experiment (planned)", "E6 · Thí nghiệm hiệu năng Spark (dự kiến)"));
  pill(S, C, W - MX - 1.15, 0.55, "proposed");
  S.addText(t("Scheduled for the official training run on the full data set, so timings reflect the real scale.",
    "Sẽ chạy trong lần train chính thức trên toàn bộ dữ liệu, để thời gian đo phản ánh đúng quy mô thật."),
    { x: MX, y: 1.35, w: W - 2 * MX, h: 0.5, fontSize: 16, italic: true, color: C.accent1, margin: 0, isTextBox: true });
  [[t("Factor 1 · shuffle partitions", "Yếu tố 1 · số shuffle partition"), t("spark.sql.shuffle.partitions ∈ {4, 16, 64, 200}, AQE off", "spark.sql.shuffle.partitions ∈ {4, 16, 64, 200}, tắt AQE"),
    t("gold feature build: rolling windows + neighbour join", "dựng feature gold: cửa sổ trượt + join ô lân cận")],
   [t("Factor 2 · join strategy", "Yếu tố 2 · chiến lược join"), "BroadcastHashJoin vs SortMergeJoin", t("query Q2: daily rain ⋈ locations ⋈ geography", "truy vấn Q2: mưa ngày ⋈ điểm ⋈ địa lý")]]
    .forEach(([a, what, job], i) => card(S, C, MX + i * 6.1, 2.1, 5.95, 2.4, { title: a, titleSize: 16, body: bullets([
      t(`Change: ${what}`, `Thay đổi: ${what}`), t(`Workload: ${job}`, `Khối việc: ${job}`),
      t("1 warm-up + 3 timed runs per setting, noop sink", "1 lần làm nóng + 3 lần đo mỗi mức, sink noop")], 14) }));
  card(S, C, MX, 4.75, W - 2 * MX, 1.5, { title: t("Ready to run", "Sẵn sàng chạy"), titleSize: 15, body: bullets([
    t("Script: src/experiments/perf_experiment.py → reports/performance/ (runs, summary, environment, both query plans)",
      "Script: src/experiments/perf_experiment.py → reports/performance/ (từng lần chạy, tổng hợp, môi trường, cả hai query plan)"),
    t("Reported with hardware, Spark version, row count and the observed trend", "Báo cáo kèm phần cứng, phiên bản Spark, số dòng và xu hướng quan sát được"),
  ], 14) });
},

// E7 charts
(IC) => {
  const yg = D.yagi || [];
  if (!yg.length) return;
  const S = slide("CONTENT", SEC(), t("E7 · Typhoon Yagi replay: what the stream predicted", "E7 · Phát lại bão Yagi: streaming đã dự báo gì"));
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
  card(S, C, 8.5, 1.3, 4.23, 5.1, { title: t("Back-test after t+24 h", "Hậu kiểm sau t+24 h"), titleSize: 15, body: bullets([
    t(`${n(yg.length)} hourly predictions with a known label (Hai Phong, Quang Ninh)`, `${n(yg.length)} dự báo theo giờ đã có nhãn (Hải Phòng, Quảng Ninh)`),
    `TP ${tp} · FP ${fp} · FN ${fn}`,
    t("Caught the landfall peak (6–7 Sep) with high probability", "Bắt được đỉnh mưa lúc bão vào bờ (6–7/9) với xác suất cao"),
    t("Missed much of the post-landfall rain (8–9 Sep): local features see a weakening system", "Bỏ sót phần lớn mưa sau bão (8–9/9): feature tại chỗ chỉ thấy hệ thống đang suy yếu"),
    t("Next step: neighbours at 100–300 km / upwind rather than adjacent cells", "Hướng tiếp: lấy ô cách 100–300 km / phía đầu gió thay vì ô sát bên"),
  ], 13) });
  source(S, C, t("reports/yagi_replay_backtest.csv · labels computed only after the stream passed t+24 h", "reports/yagi_replay_backtest.csv · nhãn chỉ được tính sau khi stream đã qua t+24 h"));
},

// E7 sample
(IC) => {
  const yr = D.samples.yagi_rows, thrS = A.manifest.thresholds_from_validation[SEL];
  const S = slide("CONTENT", SEC(), t("E7 data sample: stream predictions vs what happened", "Dữ liệu mẫu E7: dự báo của stream so với thực tế"));
  table(S, C, [[t("Point", "Điểm"), t("Prediction time", "Thời điểm dự báo"), t("P(heavy, next 24 h)", "P(mưa lớn, 24 h tới)"), t("Alert", "Cảnh báo"),
    t("Observed next 24 h (mm)", "Mưa thật 24 h tới (mm)"), t("Truth", "Thực tế"), t("Outcome", "Kết quả")],
    ...yr.map((r) => { const a = r.is_heavy_rain_predicted === true || r.is_heavy_rain_predicted === "True", tr = r.label === 1;
      const o = a && tr ? "TP" : a && !tr ? "FP" : !a && tr ? "FN" : "TN";
      return [r.location_id, String(r.ts).slice(0, 16), f(r.heavy_rain_probability), a ? t("yes", "có") : t("no", "không"), f(r.rain_next_24h, 1),
        tr ? t("heavy", "mưa lớn") : t("no", "không"), { text: o, options: { bold: true, color: o === "TP" ? C.accent3 : o === "TN" ? C.text1 : C.accent6, align: "center" } }]; })],
    { x: MX, y: 1.35, w: 8.6, colW: [0.8, 1.7, 1.4, 1.1, 1.8, 0.9, 0.9], fontSize: 12 });
  card(S, C, 9.5, 1.35, 3.23, 4.95, { title: t("Reading", "Đọc kết quả"), titleSize: 15, body: bullets([
    t(`Model ${SEL}, threshold ${f(thrS)} (frozen from 2023)`, `Model ${SEL}, ngưỡng ${f(thrS)} (khóa từ 2023)`),
    t("Each row is a MongoDB document plus the truth computed after the replay passed t + 24 h", "Mỗi dòng là một document MongoDB kèm giá trị thật tính sau khi replay qua t + 24 h"),
    t("Alerts fire on the landfall days; rain that continues after landfall is missed (FN)", "Cảnh báo bật đúng những ngày bão vào; mưa kéo dài sau đó bị bỏ sót (FN)"),
  ], 13) });
  source(S, C, t("Hai Phong, every 12 h · predictions read back from MongoDB", "Hải Phòng, mỗi 12 h · dự báo đọc lại từ MongoDB"));
},

// validation
(IC) => {
  const mv = (D.mapreduce || {}).validation || {};
  const S = slide("CONTENT", SEC(), t("How we know the outputs are correct", "Làm sao biết kết quả là đúng"));
  [[t("Row counts reconcile", "Số dòng khớp"), t(`bronze ${n(D.quality.silver.bronze_rows)} = silver ${n(D.quality.silver.silver_rows)}; 0 duplicate (point, hour) keys in gold`,
    `bronze ${n(D.quality.silver.bronze_rows)} = silver ${n(D.quality.silver.silver_rows)}; 0 khóa (điểm, giờ) trùng trong gold`)],
   [t("Independent truth", "Đối chiếu độc lập"), t(`ERA5 vs 14 NOAA gauges: totals ratio ${f(D.noaa.median_ratio, 2)}; heavy-day hit rate reported`, `ERA5 so với 14 trạm NOAA: tỷ lệ tổng lượng ${f(D.noaa.median_ratio, 2)}; báo cáo đủ tỷ lệ bắt ngày mưa lớn`)],
   [t("MapReduce = Spark SQL", "MapReduce = Spark SQL"), t(`${mv.keys_mapreduce ?? "–"} keys compared, ${mv.mismatches ?? "–"} mismatches`, `so ${mv.keys_mapreduce ?? "–"} khóa, ${mv.mismatches ?? "–"} khóa lệch`)],
   [t("No future leakage", "Không rò rỉ tương lai"), t("Unit tests change the future and assert past features are unchanged; exact label window; split purge", "Unit test đổi dữ liệu tương lai và kiểm tra feature quá khứ không đổi; cửa sổ nhãn chính xác; loại dòng ở ranh giới")],
   [t("Train = serve", "Train = serve"), t("Stream features from a 96 h buffer equal batch features (parity test)", "Feature của stream từ bộ đệm 96 h bằng feature batch (parity test)")],
   [t("Metrics verified", "Thước đo được kiểm"), t("Average precision checked on hand-computed cases; identical models give Δ = 0", "Average precision kiểm trên ví dụ tính tay; hai model giống nhau cho Δ = 0")],
   [t("Baselines", "Baseline"), t("Persistence and climatology on the same rows and threshold method", "Persistence và climatology trên cùng các dòng và cùng cách chọn ngưỡng")],
   [t("Serving", "Phục vụ"), t(`MongoDB read-back uses indexes (IXSCAN); re-run adds 0 documents`, `Đọc lại MongoDB dùng index (IXSCAN); chạy lại không thêm document nào`)]]
    .forEach(([k, v], i) => {
      const x = MX + (i % 2) * 6.1, y = 1.4 + Math.floor(i / 2) * 1.25;
      iconCircle(S, C, IC.check, x, y + 0.05, 0.5, C.accent3);
      S.addText([{ text: k, options: { bold: true, fontSize: 15, breakLine: true } }, { text: v, options: { fontSize: 12.5 } }],
        { x: x + 0.65, y, w: 5.25, h: 1.1, color: C.text1, margin: 0, valign: "top", isTextBox: true });
    });
  source(S, C, t("Tests: tests/ (pytest in the Spark container) · reports/quality, reports/mapreduce, reports/mongo", "Test: tests/ (pytest trong container Spark) · reports/quality, reports/mapreduce, reports/mongo"));
},

// limitations
(IC) => {
  const S = slide("CONTENT", SEC(), t("Limitations and next steps", "Giới hạn và bước tiếp theo"));
  card(S, C, MX, 1.35, 5.95, 4.95, { title: t("Limitations", "Giới hạn"), titleSize: 17, body: bullets([
    t(`Label is ERA5 heavy rain on a 25 km cell; ERA5 exceeds 50 mm on only ${pct(D.noaa.hit / D.noaa.heavy_days)} of gauge heavy-rain days`,
      `Nhãn là mưa lớn theo ERA5 trên ô 25 km; ERA5 chỉ vượt 50 mm ở ${pct(D.noaa.hit / D.noaa.heavy_days)} số ngày mưa lớn tại trạm`),
    t(`Pilot scale: ${nLoc} points, train ${A.manifest.years[0]}–2022 only`, `Quy mô pilot: ${nLoc} điểm, chỉ train ${A.manifest.years[0]}–2022`),
    t("Recall 34% is below the 60% target", "Recall 34% thấp hơn mục tiêu 60%"),
    t("ERA5 arrives ~5 days late: live serving needs the Forecast API (re-validate)", "ERA5 trễ ~5 ngày: chạy thật cần Forecast API (phải kiểm chứng lại)"),
    t("Spark ran in local mode; lake not yet on HDFS", "Spark chạy local mode; lake chưa đặt trên HDFS"),
  ], 16) });
  card(S, C, 6.78, 1.35, 5.95, 4.95, { title: t("Next steps", "Bước tiếp theo"), titleSize: 17, titleColor: C.accent1, body: bullets([
    t(`Official run: 63 cells × 2005–2025 (~34 k API calls, ~4 days), train ${SEL}, run E6 there`, `Bản chính thức: 63 ô × 2005–2025 (~34 nghìn lượt API, ~4 ngày), train ${SEL}, chạy E6`),
    t("Repeat leave-location-out on 63 points (E5)", "Lặp lại giữ riêng từng điểm trên 63 điểm (E5)"),
    t("Raise recall: tune the threshold for recall, calibrate probabilities", "Tăng recall: chọn ngưỡng ưu tiên recall, hiệu chỉnh xác suất"),
    t("Upwind / 100–300 km context for post-landfall rain", "Thông tin đầu gió / 100–300 km cho mưa sau bão"),
    t("Lake on HDFS / object store; Forecast API producer for a live stream", "Lake trên HDFS / object store; producer Forecast API cho stream thật"),
  ], 16) });
},

// conclusions & team
(IC) => {
  const m1 = test("M1"), per = test("persistence_rain24h"), d21 = delta("M2 - M1");
  const S = slide("CONTENT", SEC(), t("Conclusions and team contributions", "Kết luận và phân công"));
  card(S, C, MX, 1.35, 6.4, 4.95, { title: t("Conclusions", "Kết luận"), titleSize: 17, body: bullets([
    t("The pipeline runs end to end: API → Parquet lake → MapReduce + Spark SQL → Spark ML → Structured Streaming → MongoDB", "Pipeline chạy trọn: API → Parquet lake → MapReduce + Spark SQL → Spark ML → Structured Streaming → MongoDB"),
    t(`Local weather alone gives PR-AUC ${f(m1.pr_auc)} vs ${f(per.pr_auc)} for persistence`, `Chỉ thời tiết tại chỗ đã cho PR-AUC ${f(m1.pr_auc)}, so với ${f(per.pr_auc)} của persistence`),
    t(`Terrain: ${verdict(d21)} at ${nLoc} points, and worse at unseen places`, `Địa hình: ${verdict(d21)} trên ${nLoc} điểm, và kém đi ở nơi chưa thấy`),
    [{ text: SEL === "M1" ? t("Decision: the official model trains on ERA5 local weather only (M1)", "Quyết định: model chính thức chỉ train trên thời tiết ERA5 tại điểm (M1)")
      : t(`Decision: the official model trains on ${SEL}`, `Quyết định: model chính thức train trên ${SEL}`), options: { bold: true, color: C.accent2 } }],
  ], 15) });
  [["Mr. Viet", "Mr. Việt", t("Batch layer", "Batch layer"), t("Ingestion, 63 → 34 mapping, bronze → gold, cleaning rules, MapReduce, Spark SQL", "Thu thập, ánh xạ 63 → 34, bronze → gold, luật làm sạch, MapReduce, Spark SQL"), "accent3"],
   ["Mr. Luan", "Mr. Luân", t("ML & evaluation", "ML & đánh giá"), t("EDA, NOAA validation, features, RF + ablation, imbalance, performance experiment", "EDA, kiểm chứng NOAA, feature, RF + ablation, mất cân bằng, thí nghiệm hiệu năng"), "accent5"],
   ["Mr. Thinh", "Mr. Thịnh", t("Streaming & serving", "Streaming & phục vụ"), t("Replay producer, Structured Streaming, prediction, MongoDB model + read-back", "Bộ phát lại, Structured Streaming, dự báo, mô hình MongoDB + đọc lại"), "accent2"]]
    .forEach(([en, vi, role, what, col], i) => {
      const y = 1.35 + i * 1.7, who = t(en, vi);
      S.addShape("roundRect", { x: 7.3, y, w: 5.43, h: 1.55, rectRadius: 0.08, fill: { color: C.background2 }, line: { color: C.background2 } });
      S.addText(who.split(" ")[1][0], { shape: "ellipse", x: 7.45, y: y + 0.38, w: 0.8, h: 0.8, fill: { color: C[col] }, color: C.background1,
        fontSize: 24, bold: true, align: "center", valign: "middle", margin: 0, isTextBox: true });
      S.addText([{ text: `${who} — ${role}`, options: { bold: true, fontSize: 15, breakLine: true } }, { text: what, options: { fontSize: 12.5 } }],
        { x: 8.4, y: y + 0.1, w: 4.2, h: 1.35, color: C.text1, margin: 0, valign: "middle", isTextBox: true });
    });
  S.addText(t("Questions?", "Câu hỏi?"), { x: MX, y: 6.4, w: 5, h: 0.35, fontSize: 18, bold: true, color: C.accent1, margin: 0, isTextBox: true });
},
];
