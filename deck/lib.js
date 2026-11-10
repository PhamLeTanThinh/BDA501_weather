// Theme, layouts and drawing helpers for the BDA501 deck.
const pptxgen = require("pptxgenjs");
const ctx = require("./ctx");

const THEME = {
  name: "BDA501 Monsoon",
  headFontFace: "Calibri",
  bodyFontFace: "Calibri",
  colors: {
    dk1: "1B2A33", lt1: "FFFFFF", dk2: "0E4C5A", lt2: "EAF3F4",
    accent1: "0E7C86", accent2: "E0712B", accent3: "2F8F5B",
    accent4: "8C9AA3", accent5: "3B6EA8", accent6: "B23A48",
    hlink: "0E7C86", folHlink: "5E6B73",
  },
};
const HEX = THEME.colors;
const W = 13.333, H = 7.5, MX = 0.6;

function newDeck() {
  const pres = new pptxgen();
  pres.layout = "LAYOUT_WIDE";
  pres.theme = { headFontFace: THEME.headFontFace, bodyFontFace: THEME.bodyFontFace };
  pres.author = "Mr. Viet, Mr. Luan, Mr. Thinh";
  pres.title = "Heavy-rain forecasting for Vietnam with Big Data";
  const C = pres.SchemeColor;

  pres.defineSlideMaster({
    title: "TITLE_DARK",
    background: { color: C.text2 },
    objects: [
      { placeholder: { options: { name: "title", type: "title", x: MX, y: 2.0, w: 11.5, h: 1.8, fontSize: 44, bold: true,
        color: C.background1, valign: "bottom", align: "left", margin: 0 }, text: "" } },
      { placeholder: { options: { name: "body", type: "body", x: MX, y: 4.0, w: 11.5, h: 1.2, fontSize: 20,
        color: C.background2, valign: "top", align: "left", margin: 0 }, text: "" } },
    ],
  });
  // trang bìa: tiêu đề bên trái, bản đồ bên phải
  pres.defineSlideMaster({
    title: "TITLE_HERO",
    background: { color: "FBFAF7" },
    objects: [
      { placeholder: { options: { name: "title", type: "title", x: MX, y: 2.2, w: 7.5, h: 2.6, fontSize: 50, bold: true,
        color: C.text2, valign: "middle", align: "left", margin: 0 }, text: "" } },
    ],
  });
  pres.defineSlideMaster({
    title: "SECTION_DARK",
    // cùng phong cách trang bìa: nền sáng, tiêu đề bên trái, bản đồ tỷ lệ mưa lớn trên dải nền nhạt bên phải
    background: { color: "FBFAF7" },
    objects: [
      { rect: { x: 8.75, y: 0, w: W - 8.75, h: H, fill: { color: "EAF3F4" }, line: { color: "EAF3F4" } } },
      { image: { x: 9.15, y: 0.45, w: 6.6 * 926 / 1908, h: 6.6,
        path: require("path").join(__dirname, "..", "reports", "official", "fig_hero_map.png") } },
      { placeholder: { options: { name: "title", type: "title", x: MX, y: 2.0, w: 7.5, h: 1.9, fontSize: 40, bold: true,
        color: C.text2, valign: "bottom", align: "left", margin: 0 }, text: "" } },
      { placeholder: { options: { name: "body", type: "body", x: MX, y: 4.1, w: 7.4, h: 1.3, fontSize: 18,
        color: "3D4A52", valign: "top", align: "left", margin: 0 }, text: "" } },
    ],
    slideNumber: { x: MX, y: H - 0.45, w: 0.5, h: 0.3, fontSize: 10, color: C.accent4, align: "left" },
  });
  for (const [master, footer] of [["CONTENT", "BDA501 Final Project · Heavy-rain forecasting for Vietnam"],
    ["CONTENT_VI", "BDA501 Đồ án cuối kỳ · Dự báo mưa lớn cho Việt Nam"]]) pres.defineSlideMaster({
    title: master,
    background: { color: C.background1 },
    objects: [
      { placeholder: { options: { name: "title", type: "title", x: MX, y: 0.32, w: W - 2 * MX, h: 0.8, fontSize: 30, bold: true,
        color: C.text2, valign: "middle", align: "left", margin: 0 }, text: "" } },
      { text: { text: footer, options: { x: MX, y: H - 0.42, w: 8, h: 0.28,
        fontSize: 10, color: C.accent4, margin: 0 } } },
    ],
    slideNumber: { x: W - 1.1, y: H - 0.42, w: 0.5, h: 0.28, fontSize: 10, color: C.accent4, align: "right" },
  });
  return { pres, C };
}

// ---------- primitives ----------
const STATUS = {
  implemented: { label: ["Implemented", "Đã chạy"], fill: "accent3" },
  proposed: { label: ["Proposed", "Đề xuất"], fill: "accent4" },
  testing: { label: ["In testing", "Đang thử"], fill: "accent2" },
  done: { label: ["Done", "Xong"], fill: "accent3" },
};

function pill(slide, C, x, y, kind, w) {
  const s = STATUS[kind];
  slide.addText(ctx.t(...s.label), { shape: "roundRect", rectRadius: 0.5, x, y, w: w || 1.15, h: 0.26, fontSize: 10, bold: true,
    color: C.background1, fill: { color: C[s.fill] }, align: "center", valign: "middle", margin: 0, isTextBox: true,
    objectName: `pill-${kind}` });
}

function card(slide, C, x, y, w, h, opts = {}) {
  slide.addShape("roundRect", { x, y, w, h, rectRadius: 0.08,
    fill: { color: opts.fill || C.background2 }, line: { color: opts.line || C.background2, width: opts.lineW || 0.75,
      dashType: opts.dash || "solid" }, objectName: opts.name || "card" });
  let cy = y + 0.12;
  if (opts.title) {
    slide.addText(opts.title, { x: x + 0.15, y: cy, w: w - 0.3, h: opts.titleH || 0.36, fontSize: opts.titleSize || 16,
      bold: true, color: opts.titleColor || C.text2, margin: 0, valign: "top", isTextBox: true, fit: "shrink" });
    cy += (opts.titleH || 0.36) + 0.04;
  }
  if (opts.body) {
    slide.addText(opts.body, { x: x + 0.15, y: cy, w: w - 0.3, h: y + h - cy - 0.1, fontSize: opts.bodySize || 14,
      color: C.text1, margin: 0, valign: "top", isTextBox: true, paraSpaceAfter: 3 });
  }
}

function bullets(items, size = 15, color) {
  return items.map((t, i) => {
    const runs = Array.isArray(t) ? t : [{ text: t }];
    return runs.map((r, j) => ({ text: r.text, options: Object.assign({ fontSize: size, color,
      bullet: j === 0 ? { indent: 14 } : undefined, breakLine: j === runs.length - 1 && i < items.length - 1,
      paraSpaceAfter: 6 }, r.options || {}) }));
  }).flat();
}

function arrow(slide, C, x1, y1, x2, y2, opts = {}) {
  slide.addShape("line", { x: Math.min(x1, x2), y: Math.min(y1, y2), w: Math.abs(x2 - x1) || 0.0001,
    h: Math.abs(y2 - y1) || 0.0001, flipH: x2 < x1, flipV: y2 < y1,
    line: { color: opts.color || C.text2, width: opts.width || 1.5, endArrowType: "triangle",
      dashType: opts.dash || "solid" }, objectName: opts.name || "arrow" });
}

function badge(slide, C, x, y, n, color) {
  slide.addText(String(n), { shape: "ellipse", x, y, w: 0.3, h: 0.3, fontSize: 11, bold: true, color: C.background1,
    fill: { color: color || C.accent2 }, align: "center", valign: "middle", margin: 0, isTextBox: true,
    objectName: `step-${n}` });
}

function table(slide, C, rows, opts) {
  const head = rows[0].map((t) => ({ text: t, options: { bold: true, color: C.background1, fill: { color: C.text2 },
    fontSize: opts.headSize || opts.fontSize || 12 } }));
  const body = rows.slice(1).map((r, i) => r.map((c) => {
    const cell = typeof c === "object" && c !== null && c.text !== undefined ? c : { text: String(c) };
    return { text: cell.text, options: Object.assign({ fontSize: opts.fontSize || 12, color: C.text1,
      fill: { color: i % 2 ? C.background1 : C.background2 } }, cell.options || {}) };
  }));
  slide.addTable([head, ...body], Object.assign({ border: { type: "solid", pt: 0.5, color: "D5E2E4" }, margin: 0.06,
    valign: "middle" }, opts, { fontSize: undefined, headSize: undefined }));
}

function source(slide, C, text, y) {
  slide.addText(text, { x: MX, y: y || H - 0.78, w: W - 2 * MX, h: 0.3, fontSize: 10, italic: true,
    color: C.accent4, margin: 0, isTextBox: true, objectName: "source" });
}

const chartText = (C) => ({
  catAxisLabelColor: "5E6B73", valAxisLabelColor: "5E6B73", catAxisLabelFontFace: "+mn-lt",
  valAxisLabelFontFace: "+mn-lt", dataLabelFontFace: "+mn-lt", titleFontFace: "+mn-lt", legendFontFace: "+mn-lt",
  catAxisLabelFontSize: 11, valAxisLabelFontSize: 11, dataLabelFontSize: 10, legendFontSize: 11, titleFontSize: 13,
  titleColor: HEX.dk2, dataLabelColor: HEX.dk1, valGridLine: { color: "DDE6E8", size: 0.5 }, catGridLine: { style: "none" },
  legendColor: HEX.dk1,
});

function iconCircle(slide, C, data, x, y, d, fill) {
  slide.addShape("ellipse", { x, y, w: d, h: d, fill: { color: fill }, line: { color: fill }, objectName: "icon-bg" });
  const p = d * 0.26;
  slide.addImage({ data, x: x + p, y: y + p, w: d - 2 * p, h: d - 2 * p, altText: "icon" });
}

module.exports = { iconCircle, newDeck, THEME, HEX, W, H, MX, pill, card, bullets, arrow, badge, table, source, chartText };
