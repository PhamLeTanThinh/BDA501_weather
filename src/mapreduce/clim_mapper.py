#!/usr/bin/env python3
"""Mapper (Hadoop Streaming): climatology mưa lớn theo (điểm, tháng) trên tập train.

Input  : location_id,month,label,precipitation   (CSV, 1 dòng = 1 điểm × 1 giờ, từ gold/features_hourly)
Output : location_id|month \t 1,label,precipitation,precipitation
         = (số giờ, số giờ có nhãn mưa lớn, tổng mưa giờ, max mưa giờ)
"""
import sys

for line in sys.stdin:
    parts = line.rstrip("\n").split(",")
    if len(parts) != 4 or parts[0] == "location_id":
        continue
    loc, month, label, p = parts
    if not p:
        continue  # mưa null không được tính là 0
    print(f"{loc}|{int(month):02d}\t1,{int(float(label))},{p},{p}")
