import numpy as np
import pytest

from src.experiments.metrics import average_precision, block_bootstrap, confusion, roc_auc


def test_average_precision_known_values():
    assert average_precision([0, 0, 1, 1], [0.1, 0.2, 0.8, 0.9]) == 1.0
    # thứ tự 1,0,1,0 → AP = 0.5*1 + 0.5*(2/3)
    assert average_precision([1, 0, 1, 0], [0.9, 0.8, 0.7, 0.6]) == pytest.approx(0.5 + 1 / 3)


def test_weights_equal_duplication():
    rng = np.random.default_rng(0)
    y = rng.integers(0, 2, 200); s = rng.random(200); w = rng.integers(0, 3, 200)
    assert average_precision(y, s, w) == pytest.approx(average_precision(np.repeat(y, w), np.repeat(s, w)))


def test_random_scores_ap_close_to_prevalence():
    rng = np.random.default_rng(1)
    y = rng.random(50_000) < 0.03
    assert average_precision(y, rng.random(50_000)) == pytest.approx(0.03, abs=0.006)


def test_roc_auc_and_confusion():
    assert roc_auc([0, 0, 1, 1], [0.1, 0.4, 0.35, 0.8]) == 0.75
    c = confusion([0, 0, 1, 1], [0.1, 0.6, 0.4, 0.9], 0.5)
    assert (c["tp"], c["fp"], c["fn"], c["tn"]) == (1, 1, 1, 1)


def test_block_bootstrap_identical_models_have_zero_delta():
    rng = np.random.default_rng(2)
    y = rng.random(2000) < 0.1; s = rng.random(2000) + y * 0.3
    blocks = np.repeat(np.arange(100), 20)
    per, deltas = block_bootstrap(y, {"a": s, "b": s.copy()}, blocks, n_resamples=50)
    mean, lo, hi, _ = deltas[("a", "b")]
    assert mean == 0 and lo == 0 and hi == 0
    assert per["a"][0] < average_precision(y, s) < per["a"][1]
