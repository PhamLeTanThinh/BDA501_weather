// Ngôn ngữ đang dựng: mỗi slide được gọi hai lần, L = "en" rồi L = "vi".
const ctx = { L: "en" };
ctx.t = (en, vi) => (ctx.L === "en" ? en : vi);
module.exports = ctx;
