#!/usr/bin/env python3
"""Reducer / combiner (Hadoop Streaming) cho climatology mưa lớn.

Phép gộp là cộng (số giờ, số giờ dương, tổng mưa) và max (mưa giờ lớn nhất): kết hợp và giao hoán,
nên chạy làm combiner ở phía map không đổi kết quả. Tỷ lệ và trung bình chỉ tính ở reducer cuối
(--final), vì trung bình của các trung bình thì sai.

    combiner:  clim_reducer.py           → key \t n,heavy,psum,pmax
    reducer :  clim_reducer.py --final   → key \t n,heavy,heavy_rate,mean_precip_mm_h,max_precip_mm_h
"""
import sys

final = "--final" in sys.argv


def emit(key, n, h, s, m):
    if final:
        print(f"{key}\t{n},{h},{h / n:.6f},{s / n:.6f},{m:.1f}")
    else:
        print(f"{key}\t{n},{h},{s:.4f},{m}")


cur, n, h, s, m = None, 0, 0, 0.0, float("-inf")
for line in sys.stdin:
    key, val = line.rstrip("\n").split("\t")
    vn, vh, vs, vm = val.split(",")
    if key != cur:
        if cur is not None:
            emit(cur, n, h, s, m)
        cur, n, h, s, m = key, 0, 0, 0.0, float("-inf")
    n += int(vn); h += int(vh); s += float(vs); m = max(m, float(vm))
if cur is not None:
    emit(cur, n, h, s, m)
