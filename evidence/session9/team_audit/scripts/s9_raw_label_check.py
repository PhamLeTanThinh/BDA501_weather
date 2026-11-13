"""Session 9 - independent (known-answer) check of the heavy-rain label from RAW CDS, WITHOUT Spark and WITHOUT the pipeline code.

Re-implements the label definition in numpy directly from the original NetCDF zips (variable tp):
  precipitation(t) = max(tp, 0) * 1000 mm in hour (t-1h, t]; ts = valid_time UTC + 7 h (Vietnam time)
  rain_next_24h(t) = sum precipitation(t+1h .. t+24h), all 24 hours present; label = rain_next_24h > 50
  split by t (Vietnam time), purged when t+24h passes the end of the split.
Compared with the saved Spark outputs: reports/official/quality/gold_by_split.csv and reports/official/eda/label_by_cell.csv.
Also: province labels under 3 definitions (any-area/MAX, coverage >= 25%, p90 > 50 mm) on the test split,
     and an expver check (0001 = ERA5 final, 0005 = ERA5T) in the raw files.

Needs: numpy, netCDF4.
    python -I evidence/session9/scripts/s9_raw_label_check.py <raw era5_cds folder> <cells_land.csv>
"""
import csv
import io
import sys
import time
import zipfile
from collections import Counter, defaultdict
from pathlib import Path

import netCDF4
import numpy as np

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "evidence" / "session9" / "out"
H = 3600
SPLITS = {"train": ("2015-01-01T00", "2022-12-31T23"), "validation": ("2023-01-01T00", "2023-12-31T23"),
          "test": ("2024-01-01T00", "2025-12-31T23")}


def vn_epoch(s):  # "YYYY-MM-DDTHH" Vietnam time -> UTC epoch seconds
    return int(np.datetime64(s + ":00:00", "s").astype("int64")) - 7 * H


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    raw, cells_csv = Path(sys.argv[1]), Path(sys.argv[2])
    with open(cells_csv, encoding="utf-8") as f:
        cells = list(csv.DictReader(f))
    t0 = time.time()
    chunks, expver = {}, Counter()
    zips = sorted(raw.glob("year=*/era5_vn_*.zip"))
    for zp in zips:
        with zipfile.ZipFile(zp) as z:
            for name in z.namelist():
                if "accum" not in name:
                    continue
                with netCDF4.Dataset("mem", memory=z.read(name)) as d:
                    lat = np.round(np.asarray(d.variables["latitude"][:], dtype="float64"), 4)
                    lon = np.round(np.asarray(d.variables["longitude"][:], dtype="float64"), 4)
                    li = np.array([int(np.where(lat == round(float(c["grid_lat"]), 4))[0][0]) for c in cells])
                    lj = np.array([int(np.where(lon == round(float(c["grid_lon"]), 4))[0][0]) for c in cells])
                    vt = np.asarray(d.variables["valid_time"][:], dtype="int64")
                    tp = np.ma.filled(d.variables["tp"][:].astype("float32"), np.nan)[:, li, lj]
                    ev = d.variables["expver"][:]
                    for t, e in zip(vt, ev):
                        expver[(str(np.datetime64(int(t), "s"))[:4], "".join(map(str, np.atleast_1d(e))) if not isinstance(e, str) else e)] += 1
                    for k, t in enumerate(vt):
                        chunks[int(t)] = tp[k]
        print(f"{zp.name}: {len(chunks):,} accumulated hours, {time.time() - t0:.0f}s", flush=True)

    start, end = vn_epoch("2015-01-01T00"), vn_epoch("2025-12-31T23")
    times = np.arange(start, end + H, H)
    missing = [t for t in times if t not in chunks]
    P = np.stack([chunks[int(t)] for t in times]).astype("float64")  # (hour, cell)
    P = np.maximum(P, 0.0) * 1000.0  # NaN stays NaN
    nh, nc = P.shape
    valid = ~np.isnan(P)
    Pz = np.where(valid, P, 0.0)
    cs = np.vstack([np.zeros((1, nc)), np.cumsum(Pz, axis=0)])
    cv = np.vstack([np.zeros((1, nc)), np.cumsum(valid, axis=0)])
    nxt = np.full((nh, nc), np.nan)
    if nh > 24:
        s = cs[25:] - cs[1:-24]          # sum P[i+1..i+24]
        n = cv[25:] - cv[1:-24]
        nxt[:-24] = np.where(n == 24, s, np.nan)
    label = np.where(np.isnan(nxt), np.nan, (nxt > 50.0).astype("float64"))

    split = np.full(nh, "", dtype=object)
    for name, (a, b) in SPLITS.items():
        a, b = vn_epoch(a), vn_epoch(b)
        m = (times >= a) & (times <= b) & (times + 24 * H <= b)
        split[m] = name

    lines = [f"raw zip: {len(zips)}; hours in the Vietnam-time frame 2015-2025: {nh:,} (expected 96,432); missing hours: {len(missing)}; cells: {nc}",
             f"precipitation NaN: {int((~valid).sum())}"]
    with open(ROOT / "reports/official/quality/gold_by_split.csv", encoding="utf-8") as f:
        ref = {r["split"]: r for r in csv.DictReader(f)}
    for name in SPLITS:
        m = split == name
        rows = int(m.sum()) * nc
        pos = int(np.nansum(label[m]))
        rp, rr = int(float(ref[name]["positives"])), int(ref[name]["rows"])
        lines.append(f"[{'PASS' if (pos, rows) == (rp, rr) else 'FAIL'}] {name}: rows {rows:,} vs Spark {rr:,}; "
                     f"positives {pos:,} vs Spark {rp:,} (diff {pos - rp})")
    with open(ROOT / "reports/official/eda/label_by_cell.csv", encoding="utf-8") as f:
        lbc = {r["location_id"]: r for r in csv.DictReader(f)}
    diffs = []
    for j, c in enumerate(cells):
        r = lbc.get(c["grid_id"])
        n_lab = int((~np.isnan(label[:, j])).sum())
        p = int(np.nansum(label[:, j]))
        if r is None or int(r["rows"]) != n_lab or int(float(r["positives"])) != p:
            diffs.append((c["grid_id"], n_lab, p, r and r["rows"], r and r["positives"]))
    lines.append(f"[{'PASS' if not diffs else 'FAIL'}] per-cell labelled rows & positives vs label_by_cell.csv: "
                 f"{nc - len(diffs)}/{nc} cells match; differences: {diffs[:5]}")
    # Yagi trace point
    j = next(k for k, c in enumerate(cells) if c["grid_id"] == "g20.75_106.75")
    i = int(np.where(times == vn_epoch("2024-09-07T00"))[0][0])
    lines.append(f"trace g20.75_106.75 @ 2024-09-07 00:00 Vietnam time: rain_next_24h = {nxt[i, j]:.1f} mm, label = {label[i, j]:.0f}, "
                 f"past rain_24h = {Pz[i - 23:i + 1, j].sum():.1f} mm")
    # expver
    lines.append("expver by year (hours): " + ", ".join(f"{y}:{e}={n}" for (y, e), n in sorted(expver.items())))

    # province-level labels on test
    prov = defaultdict(list)
    for k, c in enumerate(cells):
        prov[c["new_province"]].append(k)
    m = split == "test"
    out_rows = []
    for pname, idx in sorted(prov.items()):
        L = label[m][:, idx]
        R = nxt[m][:, idx]
        ok = ~np.isnan(L).any(axis=1)
        L, R = L[ok], R[ok]
        npos = L.sum(axis=1)
        out_rows.append({"new_province": pname, "cells": len(idx), "test_hours": int(ok.sum()),
                         "pos_any_area": int((npos >= 1).sum()),
                         "pos_coverage25": int((npos >= 0.25 * len(idx)).sum()),
                         "pos_p90": int((np.percentile(R, 90, axis=1) > 50).sum()),
                         "pos_mean_cell_rate": round(float(L.mean()), 5)})
    with open(OUT / "province_label_definitions_test.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0]))
        w.writeheader()
        w.writerows(out_rows)
    tot = {k: sum(r[k] for r in out_rows) for k in ("test_hours", "pos_any_area", "pos_coverage25", "pos_p90")}
    lines.append(f"province labels (test 2024-2025, 34 provinces x hours): {tot}")
    big = sorted(out_rows, key=lambda r: -r["cells"])[:3] + sorted(out_rows, key=lambda r: r["cells"])[:3]
    for r in big:
        lines.append(f"  {r['new_province']}: {r['cells']} cells; any-area {r['pos_any_area']} hours, coverage25 {r['pos_coverage25']}, "
                     f"p90 {r['pos_p90']} (/{r['test_hours']}); positive cell-hour rate {r['pos_mean_cell_rate']}")
    lines.append(f"run time {time.time() - t0:.0f}s")
    (OUT / "raw_label_check.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
