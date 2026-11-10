// Builds the bilingual deck: every slide in English, followed by the same slide in Vietnamese (hidden).
// Story: problem & data → experiments → official features → architecture → model building → evaluation → test.
//   DECK_NAME=<file.pptx> NODE_PATH=deck/node_modules PPTX_SKILL=<pptx skill dir> node deck/build_bilingual.js
const path = require("path");
const K = require("./common");
const { renderIcons } = require("./icons");
const { applyTheme } = require(process.env.PPTX_SKILL + "/scripts/apply_theme.js");
const tail = require("./s_tail");
const intro = require("./s_intro"), arch = require("./s_arch"), exp = require("./s_exp"), pilot = require("./s_pilot");
const off = require("./s_official");
const team = require("./s_team");

const OUT = path.join(__dirname, process.env.DECK_NAME || "BDA501_Heavy_Rain_Forecast.pptx");
const { t } = K;
const divider = (en, vi, subEn, subVi) => () => {
  const S = K.slide("SECTION_DARK", "", t(en, vi));
  S.addText(t(subEn, subVi), { placeholder: "body" });
};

const sections = [
  ["Problem & data", [intro[0], team, intro[1], intro[2], intro[3], intro[5], intro[6], intro[9], intro[7], intro[8]]],
  ["Experiments", [pilot.divider, intro[4], exp[1], exp[3], exp[5]]],
  ["Official features", [pilot.decision]],
  // bản chính thức: mỗi bước (kéo dữ liệu, tọa độ, bronze, silver, EDA, gold) một slide, thay arch[4..8] của pilot
  ["Architecture", [arch[0], arch[1], arch[2], arch[3], off.ingest, off.cells, off.bronze, off.silver, off.edaSeason, off.edaSpace,
    off.gold, off.storage, arch[9], arch[10],
    arch[11], tail.streaming, tail.mongo, tail.deploy]],
  ["Model building", [divider("Model building", "Xây dựng mô hình", "Which algorithm, and how it is trained without leaking the future",
    "Chọn thuật toán nào, và train thế nào để không dùng dữ liệu tương lai"), tail.modelChoice, tail.protocol]],
  ["Model evaluation", [divider("Model evaluation", "Đánh giá mô hình", "The official feature set on the held-out years 2024–2025",
    "Bộ feature chính thức trên các năm giữ riêng 2024–2025"), tail.e1]],
  ["Test", [divider("Test", "Kiểm thử", "End-to-end replay of Typhoon Yagi, correctness checks, performance plan, limits",
    "Chạy thử trọn luồng với bão Yagi, kiểm tra tính đúng, kế hoạch đo hiệu năng, giới hạn"), tail.yagiChart, tail.yagiSample, tail.checks, tail.perf, tail.limits, tail.conclusions]],
];

(async () => {
  const IC = await renderIcons("#FFFFFF");
  for (const [title, slides] of sections) {
    K.pres.addSection({ title });
    K.currentSection = title;
    for (const build of slides) for (const L of ["en", "vi"]) { K.ctx.L = L; build(IC); }
  }
  K.ctx.L = "en";
  await K.pres.writeFile({ fileName: OUT });
  await applyTheme(OUT, require("./lib").THEME);
  console.log("written", OUT);
})();
