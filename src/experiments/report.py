"""Hình và báo cáo markdown cho ablation, sinh hoàn toàn từ kết quả đã chạy (không có số viết tay)."""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from src.experiments import metrics as M  # noqa: E402

COLORS = {"M1": "#4c6a92", "M2": "#c2581a", "M3": "#2f7d4f",
          "persistence_rain24h": "#999999", "climatology_loc_month": "#cccccc"}


def plot_pr(preds, thresholds, out):
    fig, ax = plt.subplots(figsize=(6.5, 5))
    for k, p in preds.items():
        t = p[p.split == "test"]
        prec, rec, thr = M.pr_curve(t["label"].to_numpy(), t["p"].to_numpy())
        ap = M.average_precision(t["label"].to_numpy(), t["p"].to_numpy())
        ax.step(rec, prec, where="post", color=COLORS.get(k), lw=1.8 if k.startswith("M") else 1,
                ls="-" if k.startswith("M") else "--", label=f"{k} (PR-AUC {ap:.3f})")
        c = M.confusion(t["label"].to_numpy(), t["p"].to_numpy(), thresholds[k])
        if k.startswith("M"):
            ax.plot(c["recall"], c["precision"], "o", color=COLORS[k], ms=6)
    prev = preds["M1"][preds["M1"].split == "test"]["label"].mean()
    ax.axhline(prev, color="#888", lw=0.8, ls=":", label=f"tỷ lệ dương {prev:.3f}")
    ax.set_xlabel("Recall"); ax.set_ylabel("Precision"); ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.set_title("Precision–Recall trên test 2024–2025\n(chấm tròn: ngưỡng chọn trên validation 2023)")
    ax.legend(fontsize=8, loc="upper right")
    fig.tight_layout(); fig.savefig(out / "pr_curves_test.png", dpi=130); plt.close(fig)


def plot_confusion(results, out):
    t = results[(results.split == "test") & results.model.isin(["M1", "M2", "M3"])]
    fig, axes = plt.subplots(1, 3, figsize=(11, 3.6))
    for ax, (_, r) in zip(axes, t.iterrows()):
        mat = np.array([[r.tn, r.fp], [r.fn, r.tp]])
        ax.imshow(mat, cmap="Blues")
        for i in range(2):
            for j in range(2):
                ax.text(j, i, f"{mat[i, j]:,}", ha="center", va="center",
                        color="white" if mat[i, j] > mat.max() / 2 else "black")
        ax.set_xticks([0, 1], ["dự báo 0", "dự báo 1"]); ax.set_yticks([0, 1], ["thật 0", "thật 1"])
        ax.set_title(f"{r.model} · ngưỡng {r.threshold:.3f}")
    fig.suptitle("Confusion matrix trên test 2024–2025")
    fig.tight_layout(); fig.savefig(out / "confusion_matrices_test.png", dpi=130); plt.close(fig)


def plot_importance(imp, out):
    pivot = imp.pivot(index="feature", columns="model", values="importance").fillna(0)
    pivot = pivot.loc[pivot.max(axis=1).sort_values().index]
    fig, ax = plt.subplots(figsize=(7, 0.28 * len(pivot) + 1.5))
    y = np.arange(len(pivot)); h = 0.27
    for i, m in enumerate(["M1", "M2", "M3"]):
        if m in pivot:
            ax.barh(y + (i - 1) * h, pivot[m], height=h, color=COLORS[m], label=m)
    ax.set_yticks(y, pivot.index, fontsize=8); ax.set_xlabel("RF feature importance (giảm impurity)")
    ax.legend(); ax.set_title("Feature importance theo thí nghiệm")
    fig.tight_layout(); fig.savefig(out / "feature_importance.png", dpi=130); plt.close(fig)


def _fmt(x, nd=3):
    return "–" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:.{nd}f}"


def _table(df):
    rows = ["| " + " | ".join(map(str, df.columns)) + " |", "|" + "---|" * len(df.columns)]
    rows += ["| " + " | ".join(f"{x:,}" if isinstance(x, (int, np.integer)) else str(x) for x in r) + " |"
             for r in df.itertuples(index=False)]
    return "\n".join(rows)


def _verdict(row):
    if row.ci_lo > 0:
        return "cải thiện (CI 95% của hiệu > 0)"
    if row.ci_hi < 0:
        return "kém đi (CI 95% của hiệu < 0)"
    return "chưa đủ bằng chứng (CI 95% chứa 0)"


def write_report(cfg, out, results, deltas, breakdown, cohorts, imp, preds, thresholds, manifest, lolo=None):
    fig = out / "figures"
    plot_pr(preds, thresholds, fig)
    plot_confusion(results, fig)
    plot_importance(imp, fig)

    t = results[results.split == "test"]
    v = results[results.split == "validation"]
    L = [f"# Ablation M1 / M2 / M3 — mưa lớn 24 giờ tới ({manifest['profile']})", "",
         f"Run `{manifest['run_id']}` · Spark {manifest['spark']} · seed {manifest['seed']} · "
         f"dữ liệu {manifest['years'][0]}–{manifest['years'][1]} · {len(manifest['locations'])} điểm: "
         f"{', '.join(manifest['locations'])}", "",
         "Nhãn: tổng mưa ERA5 trong (t, t+24h] > "
         f"{cfg['label']['threshold_mm']:.0f} mm. Mô hình: Spark MLlib RandomForest cố định "
         f"({cfg['model']['numTrees']} cây, maxDepth {cfg['model']['maxDepth']}), trọng số lớp dương "
         f"w1 = N0/N1 = {manifest['class_weight_positive']:.1f} tính trên train. Ngưỡng chọn theo F1 cao nhất "
         "trên validation 2023 rồi khóa cho test 2024–2025.", ""]
    if manifest["profile"] != "full":
        L += ["> **Đây là chạy pilot**, không phải kết quả cuối: ít điểm, train chỉ "
              f"{manifest['years'][0]}–2022. Kết luận chỉ mang tính kiểm chứng pipeline và xu hướng ban đầu.", ""]

    L += ["## 1. Kết quả trên test (cohort chung)", "",
          "| Model | Số feature | PR-AUC | CI 95% (block bootstrap theo ngày) | ROC-AUC | Ngưỡng | Recall | Precision | F1 | TP | FP | FN |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for _, r in t.iterrows():
        ci = f"[{_fmt(r.pr_auc_ci_lo)}, {_fmt(r.pr_auc_ci_hi)}]" if r.role == "ablation" else "–"
        L.append(f"| {r.model} | {r.n_features} | **{_fmt(r.pr_auc)}** | {ci} | {_fmt(r.roc_auc)} | "
                 f"{_fmt(r.threshold)} | {_fmt(r.recall)} | {_fmt(r.precision)} | {_fmt(r.f1)} | "
                 f"{r.tp:,} | {r.fp:,} | {r.fn:,} |")
    r0 = t.iloc[0]
    L += ["", f"Test: {r0.n:,} dòng điểm × giờ, {r0.positives:,} dòng dương (tỷ lệ {r0.prevalence:.2%}). "
          "PR-AUC của một bộ đoán ngẫu nhiên bằng đúng tỷ lệ dương. persistence và climatology là baseline "
          "tham khảo, không thuộc ablation.", "",
          "Validation 2023 (dùng để chọn ngưỡng): " + ", ".join(
              f"{r.model} PR-AUC {_fmt(r.pr_auc)}" for _, r in v[v.role == "ablation"].iterrows()), ""]

    L += ["## 2. Hiệu giữa các thí nghiệm (test)", "",
          "| So sánh | Δ PR-AUC | CI 95% | Tỷ lệ mẫu bootstrap có Δ ≤ 0 | Δ Recall | Δ Precision | Δ F1 | Đánh giá |",
          "|---|---|---|---|---|---|---|---|"]
    for _, d in deltas.iterrows():
        L.append(f"| {d.comparison} | {d.delta_pr_auc:+.4f} | [{d.ci_lo:+.4f}, {d.ci_hi:+.4f}] | "
                 f"{d.share_resamples_le_0:.2f} | {d.delta_recall:+.3f} | {d.delta_precision:+.3f} | "
                 f"{d.delta_f1:+.3f} | {_verdict(d)} |")
    L += ["", f"Bootstrap: {cfg['evaluation']['bootstrap']['n_resamples']} lần lấy mẫu lại theo ngày "
          "(mọi giờ, mọi điểm của một ngày đi cùng nhau), ba model dùng chung mẫu nên hiệu được ghép cặp. "
          "Δ Recall/Precision/F1 đo tại ngưỡng riêng của từng model.", ""]

    L += ["## 3. Theo vùng và mùa (test, ngưỡng đã khóa)", "",
          "| Nhóm | Model | n | Dương | PR-AUC | Recall | Precision | F1 |", "|---|---|---|---|---|---|---|---|"]
    for _, r in breakdown.sort_values(["dimension", "group", "model"]).iterrows():
        L.append(f"| {r.dimension}={r.group} | {r.model} | {r.n:,} | {r.positives:,} | {_fmt(r.pr_auc)} | "
                 f"{_fmt(r.recall)} | {_fmt(r.precision)} | {_fmt(r.f1)} |")
    L += ["", "Nhóm có ít dòng dương thì PR-AUC dao động mạnh, không nên kết luận riêng từng nhóm.", ""]

    L += ["## 4. Cohort và dữ liệu thiếu", "",
          "Mỗi dòng dưới đây là số dòng có nhãn hợp lệ, và số dòng đủ feature nếu mỗi thí nghiệm dùng "
          "cohort riêng. Ablation chỉ dùng cohort chung (đủ feature M3), nên M1, M2, M3 được chấm trên "
          "đúng cùng tập dòng.", "", _table(cohorts), "",
          "Chi tiết null theo feature: `reports/quality/gold_missingness_by_split.csv`. Dòng bị purge vì "
          "cửa sổ nhãn vượt ranh giới split: `reports/quality/gold_split_exclusions.csv`.", ""]

    top = imp.sort_values("importance", ascending=False).groupby("model").head(5)
    L += ["## 5. Feature importance (5 cao nhất mỗi model)", ""]
    for m in ["M1", "M2", "M3"]:
        s = top[top.model == m]
        L.append(f"- **{m}**: " + ", ".join(f"{f} {i:.3f}" for f, i in zip(s.feature, s.importance)))
    L += ["", "Importance kiểu giảm impurity thiên vị feature liên tục, nhiều giá trị và chia sẻ điểm "
          "giữa các feature tương quan; chỉ dùng để mô tả, không phải bằng chứng nhân quả.", ""]

    L += ["## 6. Hình", "", "- `figures/pr_curves_test.png`", "- `figures/confusion_matrices_test.png`",
          "- `figures/feature_importance.png`", ""]

    if lolo is not None:
        lres, ldel, lper = lolo
        L += ["## 6b. Leave-location-out (điểm chưa từng thấy)", "",
              "Mỗi fold giữ riêng một điểm: model train và chọn ngưỡng trên các điểm còn lại, rồi dự báo "
              "test 2024–2025 cho điểm bị giữ lại. Feature địa lý chỉ có ích ở đây nếu model học được quy luật "
              "địa hình chung, không thể dùng như mã nhận diện điểm.", "",
              "| Model | PR-AUC gộp | CI 95% | Recall | Precision | F1 | TP | FP | FN |",
              "|---|---|---|---|---|---|---|---|---|"]
        for _, r in lres.iterrows():
            L.append(f"| {r.model} | **{_fmt(r.pr_auc)}** | [{_fmt(r.pr_auc_ci_lo)}, {_fmt(r.pr_auc_ci_hi)}] | "
                     f"{_fmt(r.recall)} | {_fmt(r.precision)} | {_fmt(r.f1)} | {r.tp:,} | {r.fp:,} | {r.fn:,} |")
        L += ["", "| So sánh | Δ PR-AUC | CI 95% | Tỷ lệ mẫu có Δ ≤ 0 | Đánh giá |", "|---|---|---|---|---|"]
        for _, d in ldel.iterrows():
            L.append(f"| {d.comparison} | {d.delta_pr_auc:+.4f} | [{d.ci_lo:+.4f}, {d.ci_hi:+.4f}] | "
                     f"{d.share_resamples_le_0:.2f} | {_verdict(d)} |")
        piv = lper.pivot(index="location_id", columns="model", values="pr_auc")
        pos = lper.drop_duplicates("location_id").set_index("location_id")["positives"]
        L += ["", "PR-AUC theo từng điểm bị giữ lại (điểm ít dòng dương thì dao động mạnh):", "",
              "| Điểm | Dòng dương | M1 | M2 | M3 |", "|---|---|---|---|---|"]
        for loc, r in piv.iterrows():
            L.append(f"| {loc} | {pos[loc]:,} | {_fmt(r.get('M1'))} | {_fmt(r.get('M2'))} | {_fmt(r.get('M3'))} |")
        L += ["", "PR-AUC gộp tính trên xác suất của nhiều model fold khác nhau; chỉ so sánh M1/M2/M3 với nhau.", ""]
        L += ["## 7. Diễn giải và giới hạn", ""]
        for _, d in ldel.iterrows():
            L.append(f"- Leave-location-out, {d.comparison}: Δ PR-AUC {d.delta_pr_auc:+.4f}, {_verdict(d)}.")
    else:
        L += ["## 7. Diễn giải và giới hạn", ""]
    for _, d in deltas.iterrows():
        L.append(f"- {d.comparison}: Δ PR-AUC {d.delta_pr_auc:+.4f}, {_verdict(d)}.")
    L += ["- Feature địa lý là hằng số theo điểm. Với ít điểm, RF có thể dùng chúng như mã nhận diện điểm "
          "(học tần suất mưa lớn riêng của từng nơi) chứ không phải học quan hệ địa hình tổng quát. Muốn tách "
          "hai hiệu ứng cần nhiều điểm hơn hoặc đánh giá leave-location-out.",
          "- Nhãn là mưa ERA5 trên ô ~25 km. Kiểm chứng với 14 trạm NOAA (2015–2024) cho thấy ERA5 bắt đúng "
          "ngày khoảng 24% số ngày trạm > 50 mm (`validation/report_era5.md`); kết quả ở đây là khả năng dự "
          "báo mưa lớn theo ERA5, không phải theo trạm đo.",
          "- Các giờ liên tiếp tương quan mạnh; CI dùng bootstrap theo ngày nhưng một đợt mưa nhiều ngày vẫn "
          "có thể làm CI hẹp hơn thực tế.",
          "- Feature lân cận chỉ dùng giá trị tại hoặc trước t của các ô lân cận, ô trung tâm không được tính "
          "làm hàng xóm (`data/reference/grid_plan.csv`).", ""]
    L += ["## 8. Tái lập", "", "```", "python -m src.ingest.collect_era5 --profile " + manifest["profile"],
          "python -m src.ingest.collect_geography --profile " + manifest["profile"],
          "python -m src.pipeline.bronze_silver --profile " + manifest["profile"],
          "python -m src.pipeline.gold_features --profile " + manifest["profile"],
          "python -m src.experiments.ablation --profile " + manifest["profile"], "```", "",
          "Metadata đầy đủ (config hash, phiên bản, ngưỡng, số dòng): `reports/ablation_manifest.json`.", ""]
    (out / "ablation_report.md").write_text("\n".join(L), encoding="utf-8")
