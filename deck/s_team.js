// Slide giới thiệu thành viên nhóm (ngay sau trang bìa).
const K = require("./common");
const { C, W, t, slide } = K;

const MEMBERS = [["V", "Mr. Việt", "Mr. Viet"], ["L", "Mr. Luân", "Mr. Luan"], ["T", "Mr. Thịnh", "Mr. Thinh"]];

module.exports = (IC) => {
  const S = slide("CONTENT", t("Problem & data", "Bài toán & dữ liệu"), t("Our team", "Thành viên nhóm"));
  // tối giản: vòng tròn chữ cái đầu cùng một màu nhạt + tên, không khung, không vạch màu
  const d = 1.9, gap = 1.6, x0 = (W - 3 * d - 2 * gap) / 2, y = 2.2;
  MEMBERS.forEach(([initial, vi, en], i) => {
    const x = x0 + i * (d + gap);
    S.addText(initial, { shape: "ellipse", x, y, w: d, h: d, fill: { color: "EAF3F4" }, line: { color: "EAF3F4" },
      fontSize: 54, bold: true, color: C.text2, align: "center", valign: "middle", margin: 0, isTextBox: true });
    S.addText(t(en, vi), { x: x - 0.8, y: y + d + 0.3, w: d + 1.6, h: 0.55, fontSize: 24, bold: true, color: C.text2,
      align: "center", margin: 0, isTextBox: true });
  });
};
