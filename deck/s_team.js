// Slide giới thiệu thành viên nhóm (ngay sau trang bìa).
const K = require("./common");
const { C, W, t, slide } = K;

const MEMBERS = [["V", "Trần Quốc Việt", "Tran Quoc Viet", "25MS23304"], ["L", "Nguyễn Phạm Thành Luân", "Nguyen Pham Thanh Luan", "25MS23265"],
  ["T", "Phạm Lê Tấn Thịnh", "Pham Le Tan Thinh", "25MS23273"]];

module.exports = (IC) => {
  const S = slide("CONTENT", t("Problem & data", "Bài toán & dữ liệu"), t("Our team", "Thành viên nhóm"));
  // tối giản: vòng tròn chữ cái đầu cùng một màu nhạt + tên, không khung, không vạch màu
  const d = 1.9, gap = 1.6, x0 = (W - 3 * d - 2 * gap) / 2, y = 2.2;
  MEMBERS.forEach(([initial, vi, en, id], i) => {
    const x = x0 + i * (d + gap);
    S.addText(initial, { shape: "ellipse", x, y, w: d, h: d, fill: { color: "EAF3F4" }, line: { color: "EAF3F4" },
      fontSize: 54, bold: true, color: C.text2, align: "center", valign: "middle", margin: 0, isTextBox: true });
    S.addText(t(en, vi), { x: x - 0.8, y: y + d + 0.3, w: d + 1.6, h: 0.5, fontSize: 18, bold: true, color: C.text2,
      align: "center", margin: 0, isTextBox: true });
    S.addText(id, { x: x - 0.8, y: y + d + 0.8, w: d + 1.6, h: 0.4, fontSize: 15, color: C.text2,
      align: "center", margin: 0, isTextBox: true });
  });
};
