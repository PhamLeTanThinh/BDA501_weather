// Pilot section: divider and the feature decision that closes the pilot.
const K = require("./common");
const { C, D, A, W, MX, t, f, pct, n, slide, card, bullets, iconCircle, delta, verdict, SEL } = K;
const SEC = () => t("Pilot: feature selection", "Thử nghiệm: chọn feature");

const GROUPS = () => [
  [t("Rain", "Mưa"), ["rain_3h", "rain_24h", "rain_72h", "rain_max_1h_24h", "rain_hours_24h"], "accent1",
    t("sums / max / rainy hours over the last 3, 24, 72 h (mm, h)", "tổng / max / số giờ mưa trong 3, 24, 72 h qua (mm, giờ)")],
  [t("Humidity", "Độ ẩm"), ["rh", "rh_mean_6h", "dew_point", "dewpoint_depression"], "accent5",
    t("% now and 6 h mean; dew point and T − Td (°C)", "% hiện tại và trung bình 6 h; điểm sương và T − Td (°C)")],
  [t("Pressure", "Áp suất"), ["pressure_msl", "pressure_change_3h", "pressure_change_24h"], "accent6",
    t("hPa now and change over 3 h / 24 h", "hPa hiện tại và mức thay đổi trong 3 h / 24 h")],
  [t("Wind", "Gió"), ["wind_speed", "wind_speed_change_24h", "wind_dir_sin", "wind_dir_cos"], "accent3",
    t("km/h now and 24 h change; direction as sin/cos", "km/h hiện tại và thay đổi 24 h; hướng dưới dạng sin/cos")],
  [t("Temperature & cloud", "Nhiệt độ & mây"), ["temperature", "temp_change_24h", "cloud_cover", "cloud_mean_6h"], "accent2",
    t("°C now and 24 h change; cloud % now and 6 h mean", "°C hiện tại và thay đổi 24 h; mây % hiện tại và trung bình 6 h")],
  [t("Calendar", "Thời gian"), ["month_sin", "month_cos", "hour_sin", "hour_cos"], "accent4",
    t("season and time of day as sin/cos", "mùa và giờ trong ngày dưới dạng sin/cos")],
];

module.exports = {
  GROUPS,
  divider: (IC) => {
    const S = slide("SECTION_DARK", SEC(), t("Pilot: which features should the model use?", "Thử nghiệm: mô hình nên dùng feature nào?"));
    S.addText(t(`${A.manifest.locations.length} points × 2019–2025 · three feature sets · one Random Forest · one decision`,
      `${A.manifest.locations.length} điểm × 2019–2025 · ba bộ feature · một Random Forest · một quyết định`), { placeholder: "body" });
  },

  decision: (IC) => {
    // chỉ nêu quyết định; số liệu đã ở các slide E2–E5 trước đó
    const S = slide("CONTENT", SEC(), t("Decision: train M4, drop terrain and neighbours", "Quyết định: train M4, bỏ địa hình và ô lân cận"));
    const cols = [
      [t("Keep", "Giữ"), C.accent3, t("Local weather (24)", "Thời tiết tại chỗ (24)"),
        t("The cell's own ERA5 surface weather: rain, humidity, pressure, wind, temperature, cloud, season and hour",
          "Thời tiết bề mặt ERA5 của chính ô: mưa, độ ẩm, áp suất, gió, nhiệt độ, mây, mùa và giờ")],
      [t("Add", "Thêm"), C.accent1, t("Atmosphere (9)", "Khí quyển (9)"),
        t("Moisture convergence, moisture flow, water vapour, CAPE and K-index from the CDS download: clearly better than local weather alone",
          "Hội tụ ẩm, dòng ẩm, hơi nước, CAPE và chỉ số K từ dữ liệu CDS: tốt hơn rõ so với chỉ dùng thời tiết tại chỗ")],
      [t("Drop", "Bỏ"), C.accent6, t("Terrain · neighbour cells", "Địa hình · ô lân cận"),
        t("No real change, on the pilot and on the full data", "Không khác biệt, ở pilot lẫn trên dữ liệu đầy đủ")],
    ];
    const w = (W - 2 * MX - 2 * 0.35) / 3;
    cols.forEach(([tag, col, head, body], i) => {
      const x = MX + i * (w + 0.35);
      S.addShape("rect", { x, y: 1.9, w, h: 3.5, fill: { color: C.background2 }, line: { color: C.background2 } });
      S.addText(tag.toUpperCase(), { x: x + 0.35, y: 2.25, w: w - 0.7, h: 0.4, fontSize: 14, bold: true, charSpacing: 2, color: col, margin: 0, isTextBox: true });
      S.addText(head, { x: x + 0.35, y: 2.75, w: w - 0.7, h: 1.0, fontSize: 24, bold: true, color: C.text2, margin: 0, valign: "top", isTextBox: true });
      S.addText(body, { x: x + 0.35, y: 3.8, w: w - 0.7, h: 1.45, fontSize: 15, color: C.text1, margin: 0, valign: "top", isTextBox: true });
    });
    S.addNotes(t("The final model is M4: the 24 local-weather features plus 9 atmosphere features. Terrain and neighbours are dropped.",
      "Mô hình cuối là M4: 24 feature thời tiết tại chỗ cộng 9 feature khí quyển. Bỏ địa hình và ô lân cận."));
  },
};
