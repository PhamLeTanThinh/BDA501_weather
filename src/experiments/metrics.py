"""Metric cho bài toán lớp hiếm, viết bằng numpy để dùng chung cho đánh giá và bootstrap.

PR-AUC = average precision (tổng có trọng số dạng bậc thang), không nội suy tuyến tính giữa các điểm
như areaUnderPR của Spark, vì nội suy tuyến tính làm PR-AUC lạc quan với lớp hiếm.
Mọi hàm nhận trọng số w để block bootstrap chỉ cần đổi trọng số, không phải sắp xếp lại.
"""
import numpy as np


def _sorted_curve(y, s, w=None):
    """TP, FP tích lũy tại từng ngưỡng phân biệt (điểm hòa nhau được gộp làm một ngưỡng)."""
    y = np.asarray(y, dtype=float)
    s = np.asarray(s, dtype=float)
    w = np.ones_like(y) if w is None else np.asarray(w, dtype=float)
    order = np.argsort(-s, kind="mergesort")
    return _curve_from_order(y[order], s[order], w[order])


def _curve_from_order(y, s, w):
    tp = np.cumsum(w * y)
    fp = np.cumsum(w * (1 - y))
    last = np.r_[np.where(np.diff(s) != 0)[0], len(s) - 1]  # chỉ số cuối mỗi nhóm điểm bằng nhau
    return tp[last], fp[last], s[last]


def pr_curve(y, s, w=None):
    tp, fp, thr = _sorted_curve(y, s, w)
    precision = tp / np.maximum(tp + fp, 1e-12)
    recall = tp / max(tp[-1], 1e-12)
    return precision, recall, thr


def average_precision(y, s, w=None):
    tp, fp, _ = _sorted_curve(y, s, w)
    return _ap(tp, fp)


def _ap(tp, fp):
    if tp[-1] <= 0:
        return float("nan")
    precision = tp / np.maximum(tp + fp, 1e-12)
    recall = tp / tp[-1]
    return float(np.sum(np.diff(np.r_[0.0, recall]) * precision))


def roc_auc(y, s):
    """AUC qua hạng (Mann–Whitney), điểm hòa nhau lấy hạng trung bình."""
    y = np.asarray(y, dtype=bool)
    s = np.asarray(s, dtype=float)
    order = np.argsort(s, kind="mergesort")
    ranks = np.empty(len(s))
    ss = s[order]
    i = 0
    while i < len(ss):
        j = i
        while j + 1 < len(ss) and ss[j + 1] == ss[i]:
            j += 1
        ranks[order[i:j + 1]] = (i + j) / 2 + 1
        i = j + 1
    n1, n0 = y.sum(), (~y).sum()
    if n1 == 0 or n0 == 0:
        return float("nan")
    return float((ranks[y].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def confusion(y, s, threshold):
    y = np.asarray(y, dtype=bool)
    p = np.asarray(s) >= threshold
    tp = int(np.sum(p & y)); fp = int(np.sum(p & ~y)); fn = int(np.sum(~p & y)); tn = int(np.sum(~p & ~y))
    prec = tp / (tp + fp) if tp + fp else float("nan")
    rec = tp / (tp + fn) if tp + fn else float("nan")
    f1 = 2 * prec * rec / (prec + rec) if tp else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn, "precision": prec, "recall": rec, "f1": f1}


def best_f1_threshold(y_val, s_val):
    """Ngưỡng cho F1 cao nhất trên validation. Chỉ nhận dữ liệu validation để không thể chạm test."""
    precision, recall, thr = pr_curve(y_val, s_val)
    f1 = np.where(precision + recall > 0, 2 * precision * recall / np.maximum(precision + recall, 1e-12), 0)
    k = int(np.argmax(f1))
    return float(thr[k]), float(f1[k])


def block_bootstrap(y, scores: dict, blocks, n_resamples=500, seed=42, ci=0.95):
    """Bootstrap theo khối (vd. ngày): mọi dòng của một ngày đi cùng nhau, các model dùng chung mẫu
    (paired), nên khoảng tin cậy của hiệu PR-AUC phản ánh tương quan giữa các giờ và giữa các điểm.

    Trả về {model: (lo, hi)} cho PR-AUC và {(a, b): (mean, lo, hi, p_le_0)} cho hiệu b − a.
    """
    rng = np.random.default_rng(seed)
    y = np.asarray(y, dtype=float)
    uniq, inv = np.unique(np.asarray(blocks), return_inverse=True)
    orders = {m: np.argsort(-np.asarray(s, float), kind="mergesort") for m, s in scores.items()}
    draws = {m: np.empty(n_resamples) for m in scores}
    for r in range(n_resamples):
        counts = rng.multinomial(len(uniq), np.full(len(uniq), 1 / len(uniq)))
        w = counts[inv].astype(float)
        for m, o in orders.items():
            tp, fp, _ = _curve_from_order(y[o], np.asarray(scores[m], float)[o], w[o])
            draws[m][r] = _ap(tp, fp)
    a = (1 - ci) / 2
    per_model = {m: (float(np.nanquantile(d, a)), float(np.nanquantile(d, 1 - a))) for m, d in draws.items()}
    names = list(scores)
    deltas = {}
    for i, ma in enumerate(names):
        for mb in names[i + 1:]:
            d = draws[mb] - draws[ma]
            deltas[(ma, mb)] = (float(np.nanmean(d)), float(np.nanquantile(d, a)),
                                float(np.nanquantile(d, 1 - a)), float(np.mean(d <= 0)))
    return per_model, deltas
