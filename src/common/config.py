"""Đọc configs/ablation.yaml và gộp profile (full / pilot) vào cấu hình chung."""
import copy
import csv
import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def _merge(base, over):
    out = copy.deepcopy(base)
    for k, v in over.items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def load_config(path="configs/ablation.yaml", profile="pilot"):
    cfg = yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))
    prof = cfg.pop("profiles")[profile]
    cfg = _merge(cfg, {k: v for k, v in prof.items() if k not in ("years", "points")})
    cfg["profile"] = profile
    cfg["years"] = prof["years"]
    cfg["point_filter"] = prof["points"]
    return cfg


def path(cfg, key):
    """Đường dẫn trong cfg["paths"]. ${BDA_DATA} trỏ tới thư mục dữ liệu lớn ngoài repo:
    D:/BDA501_data trên máy, /data trong container Spark (scripts/run_in_spark.sh mount sẵn)."""
    os.environ.setdefault("BDA_DATA", "D:/BDA501_data")
    return ROOT / os.path.expandvars(cfg["paths"][key])


def load_points(cfg):
    """Danh sách điểm đo của profile. location_id là khóa bất biến, không đổi khi tỉnh sáp nhập."""
    with open(path(cfg, "points"), encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r["lat"], r["lon"] = float(r["lat"]), float(r["lon"])
    if cfg["point_filter"] == "pilot":
        rows = [r for r in rows if r["pilot"] == "1"]
    return rows
