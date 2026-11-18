// Final-assessment deck (Session 10): English only, ~24 main slides in the order of the required demo, rest as appendix.
// Uses the same slide builders as build_bilingual.js; the bilingual deck is not changed.
//   NODE_PATH=deck/node_modules PPTX_SKILL=<pptx skill dir> node deck/build_final.js   → deck/BDA501_Final_Presentation.pptx
const path = require("path");
const K = require("./common");
const { renderIcons } = require("./icons");
const { applyTheme } = require(process.env.PPTX_SKILL + "/scripts/apply_theme.js");
const tail = require("./s_tail");
const intro = require("./s_intro"), arch = require("./s_arch"), exp = require("./s_exp");
const off = require("./s_official");
const team = require("./s_team");
const fin = require("./s_final");

const OUT = path.join(__dirname, process.env.DECK_NAME || "BDA501_Final_Presentation.pptx");

const sections = [
  ["Problem", [intro[0], team, intro[1], intro[3]]],
  ["Architecture", [arch[1], off.ingest, off.cells, off.storage, off.gold]],
  ["MapReduce & Spark SQL", [arch[9], arch[10], arch[11]]],
  ["Machine learning", [exp[3], tail.modelCompare, tail.e1, fin.recall]],
  ["Streaming & serving", [tail.streaming, tail.mongo, tail.yagiChart]],
  ["Quality, performance, deployment", [tail.checks, tail.perf, tail.deploy]],
  ["Demo & close", [fin.demo, tail.limits, tail.conclusions]],
  ["Appendix", [fin.appendix, intro[2], intro[5], intro[6], intro[9], arch[2], arch[3], off.bronze, off.silver,
    off.edaSeason, off.edaSpace, off.edaYear, tail.modelChoice, tail.protocol, exp[5], tail.yagiSample, tail.readiness]],
];

(async () => {
  const IC = await renderIcons("#FFFFFF");
  K.ctx.L = "en";
  for (const [title, slides] of sections) {
    K.pres.addSection({ title });
    K.currentSection = title;
    for (const build of slides) build(IC);
  }
  await K.pres.writeFile({ fileName: OUT });
  await applyTheme(OUT, require("./lib").THEME);
  console.log("written", OUT);
})();
