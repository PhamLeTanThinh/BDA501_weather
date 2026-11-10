// Slide báo cáo tiến độ lần 2 (ngay sau slide thành viên nhóm): bảng những gì nhóm đã làm được.
// Số liệu đọc từ kết quả thật: reports/official/deck_official.json và reports/streaming/stream_report.json.
const fs = require("fs"), path = require("path");
const K = require("./common");
const { C, W, MX, t, f, n, slide, table, source } = K;
const O = JSON.parse(fs.readFileSync(path.join(K.ROOT, "reports", "official", "deck_official.json"), "utf8"));
const srp = path.join(K.ROOT, "reports", "streaming", "stream_report.json");
const SR = fs.existsSync(srp) ? JSON.parse(fs.readFileSync(srp, "utf8")) : null;

module.exports = (IC) => {
  const S = slide("CONTENT", t("Problem & data", "Bài toán & dữ liệu"), t("Progress report 2: what the team has done", "Báo cáo tiến độ lần 2: nhóm đã làm được gì"));
  const res = (m) => O.ablation.results.find((r) => r.model === m && r.split === "test");
  const d4 = O.ablation.deltas.find((d) => d.comparison === "M4 - M1");
  const mil = (x) => t(`${f(x / 1e6, 1)} M`, `${f(x / 1e6, 1).replace(".", ",")} triệu`);
  const ST = {
    done: { text: t("Done", "Xong"), options: { bold: true, color: C.accent3, align: "center" } },
    part: { text: t("Partly", "Một phần"), options: { bold: true, color: C.accent2, align: "center" } },
    plan: { text: t("Planned", "Dự kiến"), options: { bold: true, color: C.accent4, align: "center" } },
  };
  const B = (s) => ({ text: s, options: { bold: true, color: C.text2 } });
  const rows = [
    [t("Work item", "Hạng mục"), t("What was done", "Đã làm"), t("Result", "Kết quả"), t("Status", "Trạng thái")],
    [B(t("1. Data pull", "1. Kéo dữ liệu")), t("ERA5 from Copernicus CDS, 13 variables, hourly 2015–2025", "ERA5 từ Copernicus CDS, 13 biến, theo giờ 2015–2025"),
      t(`${O.download.zip_files} NetCDF files · ${f(O.volume.raw_gb, 1)} GB · on HDFS`, `${O.download.zip_files} file NetCDF · ${f(O.volume.raw_gb, 1).replace(".", ",")} GB · có trên HDFS`), ST.done],
    [B(t("2. Cells → provinces", "2. Gán ô → tỉnh")), t("point-in-polygon on old provinces, then old → new lookup", "so điểm mẫu với ranh giới tỉnh cũ, rồi tra tỉnh mới"),
      t(`${O.cells.n} land cells → ${O.cells.new_provinces} new provinces`, `${O.cells.n} ô đất liền → ${O.cells.new_provinces} tỉnh mới`), ST.done],
    [B(t("3. Data lake", "3. Data lake")), t("bronze → silver (clean, units, VN time) → gold (label + features) on Spark", "bronze → silver (làm sạch, đổi đơn vị, giờ VN) → gold (nhãn + feature) trên Spark"),
      t(`${mil(O.silver.silver_rows)} cell-hours · 33 features · 0 duplicate keys`, `${mil(O.silver.silver_rows)} ô-giờ · 33 feature · 0 khóa trùng`), ST.done],
    [B(t("4. EDA", "4. EDA")), t("rainy seasons, where heavy rain happens, year-to-year change", "mùa mưa, mưa lớn ở đâu, thay đổi theo năm"),
      t("notebook + charts on official data", "notebook + biểu đồ trên dữ liệu chính thức"), ST.done],
    [B(t("5. Features + model", "5. Feature + model")), t("4 feature sets M1–M4; 3 algorithms (logistic, RF, boosted trees)", "4 bộ feature M1–M4; 3 thuật toán (logistic, RF, boosted trees)"),
      t(`M4 PR-AUC ${f(res("M4").pr_auc)} vs ${f(res("persistence_rain24h").pr_auc)} persistence · atmosphere +${f(d4.delta_pr_auc, 3)}`,
        `M4 PR-AUC ${f(res("M4").pr_auc)} so với ${f(res("persistence_rain24h").pr_auc)} persistence · khí quyển +${f(d4.delta_pr_auc, 3)}`), ST.done],
    [B(t("6. Streaming", "6. Streaming")), t("Kafka → Spark Structured Streaming → MongoDB → live web map, with dirty data and a crash", "Kafka → Spark Structured Streaming → MongoDB → bản đồ web trực tiếp, có dữ liệu rác và sự cố"),
      SR ? t(`${n(SR.kafka.messages_in_topic)} messages, 0 lost · stream = batch on ${n(SR.parity_stream_vs_batch_same_rows.cell_hours_both_scored)} cell-hours`,
        `${n(SR.kafka.messages_in_topic)} bản tin, 0 mất · stream = batch trên ${n(SR.parity_stream_vs_batch_same_rows.cell_hours_both_scored)} ô-giờ`) : "–", ST.done],
    [B(t("7. MapReduce + Spark SQL", "7. MapReduce + Spark SQL")), t("climatology on YARN, 3 analytical queries", "climatology trên YARN, 3 truy vấn phân tích"),
      t("0 / 144 differences vs Spark SQL · pilot data, rerun on official data next", "0 / 144 khác biệt so với Spark SQL · dữ liệu pilot, sẽ chạy lại trên dữ liệu chính thức"), ST.part],
    [B(t("8. Alert threshold", "8. Ngưỡng cảnh báo")), t("threshold for F1 vs for recall, chosen on 2023", "ngưỡng theo F1 và theo recall, chọn trên 2023"),
      t(`recall ${f(100 * res("M4").recall, 0)}% at F1 threshold; 60% target not yet met`, `recall ${f(100 * res("M4").recall, 0)}% ở ngưỡng F1; chưa đạt mục tiêu 60%`), ST.part],
    [B(t("9. Spark performance (E6)", "9. Hiệu năng Spark (E6)")), t("shuffle partitions and join strategy", "số shuffle partition và cách join"),
      t("script ready, not measured yet", "script sẵn sàng, chưa đo"), ST.plan],
  ];
  table(S, C, rows, { x: MX, y: 1.25, w: W - 2 * MX, colW: [2.35, 4.6, 4.03, 1.15], fontSize: 11.5,
    rowH: [0.38, 0.5, 0.5, 0.5, 0.5, 0.5, 0.58, 0.58, 0.5, 0.5] });
  source(S, C, t("Numbers from the official run (ERA5 CDS, 493 land cells, test 2024–2025) and the streaming test run.",
    "Số liệu từ bản chính thức (ERA5 CDS, 493 ô đất liền, test 2024–2025) và lượt chạy thử streaming."));
  S.addNotes(t("Progress since the last report: the full official data set is in the lake, the feature choice is settled on official data (M4 = local weather + atmosphere), and the streaming lane now runs end to end on Kafka with the official model, tested against dirty data and a crash. Still open: rerun MapReduce and Spark SQL on official data, raise recall towards 60%, and measure Spark performance (E6).\n\nWhy item 7 (MapReduce + Spark SQL) is only partly done: the MapReduce climatology job on YARN and the 3 Spark SQL queries are written and verified (0 of 144 keys differ from Spark SQL), but they ran on the pilot data (12 cities, 2019–2022, Open-Meteo) because the official CDS data (493 cells, 2015–2025) did not exist yet. To finish: export the official gold training rows (about 34.6 million rows, 4 columns, ~1 GB of CSV) to HDFS, start the Hadoop cluster, rerun MapReduce and the 3 queries on official gold, then update the slides. On the laptop, Hadoop and Spark together have run out of memory before, so the streaming containers must be stopped first; drive D has only ~6 GB free. Estimated 1–1.5 hours.\n\nWhy item 8 (alert threshold) is only partly done: the work itself is done — the threshold was re-chosen in three ways on validation 2023 (best F1, recall ≥ 60%, precision ≥ 30%; results in reports/official/model/m4_threshold_choice.csv). No threshold reaches recall ≥ 60% and precision ≥ 30% at the same time: the F1 threshold gives 46% recall / 44% precision, the recall-first threshold gives 74% / 19%. This is a limit of the model, not unfinished work; reaching the target needs better features (e.g. upwind context 100–300 km away), which is a next step.",
    "Tiến độ từ lần báo cáo trước: toàn bộ dữ liệu chính thức đã vào lake, việc chọn feature đã chốt trên dữ liệu chính thức (M4 = thời tiết tại chỗ + khí quyển), và làn streaming chạy trọn trên Kafka với model chính thức, đã kiểm thử với dữ liệu rác và sự cố. Còn lại: chạy lại MapReduce và Spark SQL trên dữ liệu chính thức, nâng recall về gần 60%, và đo hiệu năng Spark (E6).\n\nVì sao mục 7 (MapReduce + Spark SQL) mới xong một phần: job MapReduce (climatology trên YARN) và 3 truy vấn Spark SQL đã viết và kiểm chứng xong (0/144 khóa khác biệt so với Spark SQL), nhưng lúc chạy dữ liệu vẫn là bản pilot (12 thành phố, 2019–2022, Open-Meteo) vì khi đó chưa có dữ liệu CDS chính thức (493 ô, 2015–2025). Để xong: export tập train của gold chính thức (khoảng 34,6 triệu dòng, 4 cột, ~1 GB CSV) lên HDFS, bật cụm Hadoop, chạy lại MapReduce và 3 truy vấn trên gold chính thức, rồi cập nhật slide. Trên laptop, Hadoop chạy cùng Spark từng bị hết RAM nên phải tắt các container streaming trước; ổ D chỉ còn khoảng 6 GB. Ước tính 1–1,5 giờ.\n\nVì sao mục 8 (ngưỡng cảnh báo) mới xong một phần: phần việc đã làm xong — ngưỡng được chọn lại theo ba cách trên validation 2023 (F1 cao nhất, recall ≥ 60%, precision ≥ 30%; kết quả ở reports/official/model/m4_threshold_choice.csv). Không ngưỡng nào đạt cùng lúc recall ≥ 60% và precision ≥ 30%: ngưỡng F1 cho recall 46% / precision 44%, ngưỡng ưu tiên recall cho 74% / 19%. Đây là giới hạn của model chứ không phải việc còn dở; muốn đạt mục tiêu cần feature tốt hơn (ví dụ thông tin đầu gió cách 100–300 km), là bước tiếp theo."));
};
