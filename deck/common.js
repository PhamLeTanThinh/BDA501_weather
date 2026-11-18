// Shared state for the bilingual deck: presentation, data, number helpers, slide factory.
const fs = require("fs");
const path = require("path");
const ctx = require("./ctx");
const lib = require("./lib");

const ROOT = path.resolve(__dirname, "..");
const D = JSON.parse(fs.readFileSync(path.join(ROOT, "reports", "deck_data.json"), "utf8"));
// E0 (ERA5 so với trạm NOAA): dùng kết quả trên dữ liệu CDS chính thức 2015–2025 thay cho bản pilot (Open-Meteo, 2015–2024)
const OFF = path.join(ROOT, "reports", "official", "deck_official.json");
if (fs.existsSync(OFF)) {
  const e0 = JSON.parse(fs.readFileSync(OFF, "utf8")).e0;
  if (e0) { D.noaa = e0.noaa; D.samples.e0_noaa_vs_era5 = e0.samples; }
  D.official_ablation = JSON.parse(fs.readFileSync(OFF, "utf8")).ablation || null;
}
// MapReduce (Hadoop/YARN) and Spark SQL on the official data: replace the pilot numbers when the official run exists
const OMR = path.join(ROOT, "reports", "official", "mapreduce");
D.mr_official = false;
if (fs.existsSync(path.join(OMR, "validation.json")) && fs.existsSync(path.join(OMR, "samples.json"))) {
  const log = fs.readFileSync(path.join(OMR, "job.log"), "utf8").split(/\r?\n/).map((l) => l.trim()).filter((l) => /^[A-Za-z][A-Za-z ()-]+=\d+$/.test(l));
  const sm = JSON.parse(fs.readFileSync(path.join(OMR, "samples.json"), "utf8"));
  D.mapreduce = { log, validation: JSON.parse(fs.readFileSync(path.join(OMR, "validation.json"), "utf8")) };
  D.samples.mr_input = sm.mr_input; D.samples.mr_output = sm.mr_output; D.samples.mr_highlight = sm.highlight;
  D.mr_official = true;
}
const OSQL = path.join(ROOT, "reports", "official", "spark_sql");
const csvRows = (p) => { const [h, ...r] = fs.readFileSync(p, "utf8").trim().split(/\r?\n/); const k = h.split(",");
  return r.map((l) => Object.fromEntries(l.split(",").map((v, i) => [k[i], isNaN(+v) || v === "" ? v : +v]))); };
D.sql_official = false;
if (fs.existsSync(path.join(OSQL, "q1_heavy_rate_region_month_result.csv"))) {
  D.eda_region_month = csvRows(path.join(OSQL, "q1_heavy_rate_region_month_result.csv"));
  D.eda_province_year = csvRows(path.join(OSQL, "q2_heavy_days_province_year_result.csv"));
  D.sql_official = true;
}
const { pres, C } = lib.newDeck();

const f = (x, d = 3) => (x === null || x === undefined || Number.isNaN(x) ? "–" : Number(x).toFixed(d));
const pct = (x, d = 0) => `${(100 * x).toFixed(d)}%`;
const n = (x) => Number(x).toLocaleString("en-US");
const A = D.ablation;
const test = (m) => A.results.find((r) => r.model === m && r.split === "test");
const delta = (cmp) => A.deltas.find((d) => d.comparison === cmp);
const verdict = (d) => (d.ci_lo > 0 ? ctx.t("improves", "cải thiện") : d.ci_hi < 0 ? ctx.t("worse", "kém đi")
  : ctx.t("no significant difference", "không khác biệt có ý nghĩa"));
const SEL = D.selected_model;
const nLoc = A.manifest.locations.length;
const CT = () => lib.chartText(C);

function slide(master, section, title) {
  const m = master === "CONTENT" && ctx.L === "vi" ? "CONTENT_VI" : master;
  const s = pres.addSlide({ masterName: m, sectionTitle: module.exports.currentSection || section });
  if (ctx.L === "vi") s.hidden = true;  // bản tiếng Việt: ẩn khi trình chiếu
  if (title) s.addText(title, { placeholder: "title" });
  return s;
}

module.exports = { ROOT, D, pres, C, f, pct, n, A, test, delta, verdict, SEL, nLoc, CT, slide, t: ctx.t, ctx, ...lib };
