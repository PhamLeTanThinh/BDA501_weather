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
