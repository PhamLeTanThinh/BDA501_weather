import csv

from src.experiments.selection import select_model


def _write(tmp_path, cis):
    p = tmp_path / "d.csv"
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["comparison", "ci_lo", "ci_hi"])
        for k, (lo, hi) in cis.items():
            w.writerow([k, lo, hi])
    return p


def test_default_is_simplest_model(tmp_path):
    assert select_model(_write(tmp_path, {"M2 - M1": (-0.02, 0.01), "M3 - M1": (-0.01, 0.02), "M3 - M2": (-0.003, 0.019)})) == "M1"


def test_adds_groups_only_when_ci_above_zero(tmp_path):
    assert select_model(_write(tmp_path, {"M2 - M1": (0.01, 0.03), "M3 - M1": (0.01, 0.04), "M3 - M2": (-0.01, 0.02)})) == "M2"
    assert select_model(_write(tmp_path, {"M2 - M1": (0.01, 0.03), "M3 - M1": (0.02, 0.05), "M3 - M2": (0.001, 0.02)})) == "M3"
    assert select_model(_write(tmp_path, {"M2 - M1": (-0.01, 0.01), "M3 - M1": (0.005, 0.03), "M3 - M2": (0.0, 0.02)})) == "M3"
