"""Session 9 - run the repo's REAL mapper/combiner/reducer (src/mapreduce) on the FULL official training set (493 cells, 2015-2022),
simulating Hadoop Streaming locally (no Hadoop/YARN on the audit machine):
  input  = location_id,month,label,precipitation built with numpy from raw CDS (same definition as gold, confirmed by
           s9_raw_label_check.py to match Spark with 0 difference)
  map + combiner per "split" (each training year = 1 split, like one mapper), sort/shuffle, reduce --final
Compared with a numpy groupby (known answer) using the same tolerances as src/mapreduce/mr_io.validate (mean 1e-5, max 0.051).

    python -I evidence/session9/scripts/s9_mr_official_local.py <raw era5_cds folder> <cells_land.csv> <temp folder>
"""
import csv
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import netCDF4
import numpy as np

ROOT = Path(__file__).resolve().parents[3]
MR = ROOT / "src" / "mapreduce"
OUT = ROOT / "evidence" / "session9" / "out"
H = 3600


def vn_epoch(s):
    return int(np.datetime64(s + ":00:00", "s").astype("int64")) - 7 * H


def load_tp(raw, cells):
    chunks = {}
    for zp in sorted(raw.glob("year=*/era5_vn_*.zip")):
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
                    for k, t in enumerate(vt):
                        chunks[int(t)] = tp[k]
    return chunks


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    raw, cells_csv, tmp = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    tmp.mkdir(parents=True, exist_ok=True)
    with open(cells_csv, encoding="utf-8") as f:
        cells = list(csv.DictReader(f))
    ids = [c["grid_id"] for c in cells]
    t0 = time.time()
    chunks = load_tp(raw, cells)
    times = np.arange(vn_epoch("2015-01-01T00"), vn_epoch("2025-12-31T23") + H, H)
    P = np.maximum(np.stack([chunks[int(t)] for t in times]).astype("float64"), 0.0) * 1000.0
    cs = np.vstack([np.zeros((1, P.shape[1])), np.cumsum(P, axis=0)])
    nxt = np.full(P.shape, np.nan)
    nxt[:-24] = cs[25:] - cs[1:-24]
    label = (nxt > 50.0).astype("int64")
    end_train = vn_epoch("2022-12-31T23")
    train = (times <= end_train) & (times + 24 * H <= end_train)
    vn = (times + 7 * H).astype("datetime64[s]")
    month = vn.astype("datetime64[M]").astype("int64") % 12 + 1
    year = vn.astype("datetime64[Y]").astype("int64") + 1970
    print(f"raw loaded: {time.time() - t0:.0f}s; training hours {int(train.sum())}", flush=True)

    combined, map_out, map_in = [], 0, 0
    for y in sorted(set(year[train].tolist())):
        rows = np.where(train & (year == y))[0]
        split_file = tmp / f"mr_input_{y}.csv"
        with open(split_file, "w", encoding="utf-8", newline="\n") as f:
            for i in rows:
                m, pv, lv = int(month[i]), P[i], label[i]
                f.write("".join(f"{ids[j]},{m},{lv[j]},{pv[j]:.6f}\n" for j in range(len(ids))))
        map_in += len(rows) * len(ids)
        with open(split_file, encoding="utf-8") as fin:
            mp = subprocess.run([sys.executable, "-I", str(MR / "clim_mapper.py")], stdin=fin, capture_output=True, text=True, check=True)
        lines = sorted(l for l in mp.stdout.splitlines() if l)
        map_out += len(lines)
        cb = subprocess.run([sys.executable, "-I", str(MR / "clim_reducer.py")], input="\n".join(lines) + "\n",
                            capture_output=True, text=True, check=True)
        combined += [l for l in cb.stdout.splitlines() if l]
        split_file.unlink()
        print(f"split {y}: {len(rows) * len(ids):,} rows -> combiner {len(cb.stdout.splitlines())} records, {time.time() - t0:.0f}s", flush=True)
    red = subprocess.run([sys.executable, "-I", str(MR / "clim_reducer.py"), "--final"], input="\n".join(sorted(combined)) + "\n",
                         capture_output=True, text=True, check=True)
    mr = {}
    for line in red.stdout.splitlines():
        k, v = line.split("\t")
        n, h, rate, mean_p, max_p = v.split(",")
        mr[k] = (int(n), int(h), float(mean_p), float(max_p))

    # numpy known answer (values rounded to 6 digits like the MR input)
    Pr = np.round(P, 6)
    ref = {}
    for j, g in enumerate(ids):
        for m in range(1, 13):
            sel = train & (month == m)
            v = Pr[sel, j]
            ref[f"{g}|{m:02d}"] = (int(sel.sum()), int(label[sel, j].sum()), float(v.mean()), float(np.round(v.max(), 1)))
    mism = [k for k in ref if k not in mr or mr[k][:2] != ref[k][:2] or abs(mr[k][2] - ref[k][2]) > 1e-5
            or abs(mr[k][3] - ref[k][3]) > 0.051]
    lines = [f"input (map input records): {map_in:,} official training rows, {len(ids)} cells, 8 splits (each year 2015-2022 = 1 mapper)",
             f"map output records: {map_out:,}; combine output records: {len(combined):,}; reduce output keys: {len(mr):,}",
             f"keys numpy: {len(ref):,}; missing_or_extra: {len(set(ref) ^ set(mr))}; mismatches: {len(mism)} {mism[:5]}",
             f"rows MR: {sum(v[0] for v in mr.values()):,}; rows numpy: {sum(v[0] for v in ref.values()):,}; "
             f"heavy hours MR: {sum(v[1] for v in mr.values()):,}",
             f"time: {time.time() - t0:.0f}s (local Python, NOT Hadoop/YARN)"]
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "mapreduce_official_local.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
