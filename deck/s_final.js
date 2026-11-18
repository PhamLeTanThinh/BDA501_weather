// Slides used only by the final-assessment deck (build_final.js): recall by place, live-demo plan, appendix divider.
const fs = require("fs"), path = require("path");
const K = require("./common");
const { C, W, MX, t, f, pct, n, slide, source } = K;

const RS = (() => {
  const q = path.join(K.ROOT, "reports", "official", "model", "province_recall_summary.json");
  return fs.existsSync(q) ? JSON.parse(fs.readFileSync(q, "utf8")) : null;
})();
const FIG = path.join(K.ROOT, "reports", "official", "model", "fig_province_recall.png");
const REG = { BacTrungBo: "North-central coast", NamTrungBo: "South-central coast", Bac: "North", TayNguyen: "Central Highlands", Nam: "South" };

function big(S, x, y, w, value, caption, color = C.accent2, size = 34) {
  S.addText(value, { x, y, w, h: size / 55, fontSize: size, bold: true, color, margin: 0, valign: "bottom", isTextBox: true, fit: "shrink" });
  S.addText(caption, { x, y: y + size / 55 + 0.05, w, h: 0.62, fontSize: 12.5, color: C.text1, margin: 0, valign: "top", isTextBox: true });
}

module.exports = {
recall: () => {
  const S = slide("CONTENT", "", "Where recall is lost: 493 cells, 34 provinces");
  S.addText("Recall follows how often heavy rain happens at a place — not how big the province is",
    { x: MX, y: 1.15, w: W - 2 * MX, h: 0.5, fontSize: 19, bold: true, color: C.text2, margin: 0, isTextBox: true, fit: "shrink" });
  S.addImage({ path: FIG, x: MX, y: 1.75, w: 8.6, h: 8.6 * 1152 / 2080 });
  if (RS) {
    const reg = Object.fromEntries(RS.by_region.map((r) => [r.region, r]));
    const tot = RS.by_region.reduce((a, r) => a + r.fn, 0);
    big(S, 9.55, 1.8, 3.3, `${pct(reg.BacTrungBo.recall_f1)} vs ${pct(reg.Nam.recall_f1)}`, "recall, north-central coast vs South (national 46%)", C.accent1);
    big(S, 9.55, 3.05, 3.3, f(RS.spearman_cell_recall_vs_heavy_rate, 2), "Spearman: cell recall vs local heavy-rain share; vs province size only −0.18");
    big(S, 9.55, 4.3, 3.3, pct(reg.Bac.fn / tot), `of the ${n(tot)} missed cell-hours are in the North (mountains); the South only ${pct(reg.Nam.fn / tot)}`, C.accent6);
    const rt = RS.regional_threshold_total;
    big(S, 9.55, 5.55, 3.3, `${pct(rt.test_recall)} / ${pct(rt.test_precision)}`, "recall / precision with one threshold per region (chosen on 2023): a trade-off, not more skill", C.text2, 26);
  }
  source(S, C, "Test 2024–2025, threshold 0.932 frozen on 2023 · 483 scored cells (10 border/coast cells lack 2 land neighbours) · src/analytics/province_recall.py");
  S.addNotes("We broke the 46% national recall down by cell, province and region. The map on the left is the recall of every scored cell: green on the central coast, orange in the northern mountains, red in the South and Lâm Đồng. On the right each bubble is a province: the further right, the more often heavy rain happens there, and recall rises with it (Spearman 0.65 at cell level). Province size does not matter: the number of cells per province is almost unrelated to recall (−0.18). Half of all missed heavy-rain hours are in the North — Lào Cai alone is 10% — while the South has the lowest recall but only 4% of the misses, because heavy rain there is rare, short and convective. A separate threshold per region, chosen on 2023 only, lifts recall to 50% but drops precision to 39%: it moves along the same curve. To catch more in the North and South the model needs new information — upwind moisture, radar or station rain — not a different cut-off.");
},

demo: () => {
  const S = slide("CONTENT", "", "Live demo: what we will show, in order");
  const steps = [
    ["1", "Data path", "raw CDS zip → bronze → silver → gold on the lake; one cell-hour traced from NetCDF to its label", "lake folders · reports/official/quality"],
    ["2", "MapReduce on YARN", "Hadoop Streaming job on the official training set: map → combine → shuffle → reduce; counters in the YARN UI", "reports/official/mapreduce/job.log"],
    ["3", "Spark SQL + plan", "three queries on 47.5 M gold rows; Exchange and BroadcastHashJoin in the formatted plan", "reports/official/spark_sql/"],
    ["4", "Spark ML result", "M4 vs baselines and the warning trade-off on 2024–2025", "reports/official/model/"],
    ["5", "Kafka → Spark → MongoDB → web", "press Start: the map fills hour by hour, junk is quarantined, counters move; one document read back from MongoDB", "http://localhost:8000"],
  ];
  steps.forEach(([k, h, d, e], i) => {
    const y = 1.3 + i * 1.12;
    S.addText(k, { shape: "ellipse", x: MX, y: y + 0.12, w: 0.62, h: 0.62, fill: { color: C.accent1 }, line: { color: C.accent1 },
      color: C.background1, fontSize: 20, bold: true, align: "center", valign: "middle", margin: 0, isTextBox: true });
    S.addText(h, { x: MX + 0.85, y, w: 3.4, h: 0.86, fontSize: 18, bold: true, color: C.text2, valign: "middle", margin: 0, isTextBox: true });
    S.addText(d, { x: MX + 4.3, y, w: 5.3, h: 0.86, fontSize: 13.5, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
    S.addText(e, { x: MX + 9.75, y, w: 2.4, h: 0.86, fontSize: 11, color: C.accent4, valign: "middle", margin: 0, isTextBox: true });
  });
  source(S, C, "Fallback if anything fails live: one screenshot per step in report/demo/");
  S.addNotes("Demo script: docs/DEMO.md. Before the session run scripts/demo_check.sh — it starts Docker services, checks Kafka, MongoDB, HDFS and YARN, and warms up the stream so it is ready at the Start button. If the live demo fails, switch to report/demo/ which has a screenshot for each step.");
},

appendix: () => {
  const S = slide("SECTION_DARK", "", "Appendix");
  S.addText("Backup slides for questions: data, EDA, pipeline steps, protocol, evidence", { placeholder: "body" });
},
};
