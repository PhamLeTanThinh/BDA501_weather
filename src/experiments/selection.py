"""Chọn tập feature cho model huấn luyện cuối từ kết quả ablation.

Quy tắc (định trước, không chỉnh theo kết quả): mặc định M1 (đơn giản nhất). Chỉ thêm một nhóm feature
khi khoảng tin cậy 95% của mức cải thiện PR-AUC trên test nằm hẳn trên 0:
  M1 → M2 nếu CI(M2 − M1) > 0;  rồi → M3 nếu CI(M3 − M2) > 0.
  Nếu M2 không hơn M1 nhưng CI(M3 − M1) > 0 thì chọn M3 (lợi ích đến từ nhóm lân cận).
"""
import csv
from pathlib import Path


def _ci_above_zero(rows, comparison):
    r = next((x for x in rows if x["comparison"] == comparison), None)
    return r is not None and float(r["ci_lo"]) > 0


def select_model(deltas_csv: Path):
    with open(deltas_csv, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if _ci_above_zero(rows, "M2 - M1"):
        return "M3" if _ci_above_zero(rows, "M3 - M2") else "M2"
    return "M3" if _ci_above_zero(rows, "M3 - M1") else "M1"
