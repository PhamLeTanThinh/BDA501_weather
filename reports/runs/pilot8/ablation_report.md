# Ablation M1 / M2 / M3 — mưa lớn 24 giờ tới (pilot)

Run `20261031T115439Z_pilot` · Spark 4.2.0 · seed 42 · dữ liệu 2019–2025 · 8 điểm: HCMC, HNOI, HPHO, HUE, KHOA, NAN, QNGA, QNIN

Nhãn: tổng mưa ERA5 trong (t, t+24h] > 50 mm. Mô hình: Spark MLlib RandomForest cố định (100 cây, maxDepth 12), trọng số lớp dương w1 = N0/N1 = 68.4 tính trên train. Ngưỡng chọn theo F1 cao nhất trên validation 2023 rồi khóa cho test 2024–2025.

> **Đây là chạy pilot**, không phải kết quả cuối: ít điểm, train chỉ 2019–2022. Kết luận chỉ mang tính kiểm chứng pipeline và xu hướng ban đầu.

## 1. Kết quả trên test (cohort chung)

| Model | Số feature | PR-AUC | CI 95% (block bootstrap theo ngày) | ROC-AUC | Ngưỡng | Recall | Precision | F1 | TP | FP | FN |
|---|---|---|---|---|---|---|---|---|---|---|---|
| M1 | 24 | **0.440** | [0.351, 0.524] | 0.954 | 0.752 | 0.512 | 0.441 | 0.474 | 1,249 | 1,581 | 1,191 |
| M2 | 27 | **0.432** | [0.340, 0.515] | 0.960 | 0.869 | 0.365 | 0.510 | 0.425 | 890 | 855 | 1,550 |
| M3 | 31 | **0.440** | [0.344, 0.525] | 0.960 | 0.824 | 0.439 | 0.484 | 0.461 | 1,071 | 1,140 | 1,369 |
| persistence_rain24h | 1 | **0.197** | – | 0.891 | 51.200 | 0.289 | 0.300 | 0.294 | 705 | 1,645 | 1,735 |
| climatology_loc_month | 1 | **0.070** | – | 0.747 | 0.174 | 0.088 | 0.144 | 0.109 | 214 | 1,274 | 2,226 |

Test: 140,160 dòng điểm × giờ, 2,440 dòng dương (tỷ lệ 1.74%). PR-AUC của một bộ đoán ngẫu nhiên bằng đúng tỷ lệ dương. persistence và climatology là baseline tham khảo, không thuộc ablation.

Validation 2023 (dùng để chọn ngưỡng): M1 PR-AUC 0.264, M2 PR-AUC 0.303, M3 PR-AUC 0.294

## 2. Hiệu giữa các thí nghiệm (test)

| So sánh | Δ PR-AUC | CI 95% | Tỷ lệ mẫu bootstrap có Δ ≤ 0 | Δ Recall | Δ Precision | Δ F1 | Đánh giá |
|---|---|---|---|---|---|---|---|
| M2 - M1 | -0.0079 | [-0.0230, +0.0091] | 0.77 | -0.147 | +0.069 | -0.049 | chưa đủ bằng chứng (CI 95% chứa 0) |
| M3 - M1 | -0.0001 | [-0.0174, +0.0173] | 0.43 | -0.073 | +0.043 | -0.013 | chưa đủ bằng chứng (CI 95% chứa 0) |
| M3 - M2 | +0.0079 | [-0.0026, +0.0187] | 0.08 | +0.074 | -0.026 | +0.035 | chưa đủ bằng chứng (CI 95% chứa 0) |

Bootstrap: 500 lần lấy mẫu lại theo ngày (mọi giờ, mọi điểm của một ngày đi cùng nhau), ba model dùng chung mẫu nên hiệu được ghép cặp. Δ Recall/Precision/F1 đo tại ngưỡng riêng của từng model.

## 3. Theo vùng và mùa (test, ngưỡng đã khóa)

| Nhóm | Model | n | Dương | PR-AUC | Recall | Precision | F1 |
|---|---|---|---|---|---|---|---|
| region=Bac | M1 | 52,560 | 785 | 0.426 | 0.394 | 0.533 | 0.453 |
| region=Bac | M2 | 52,560 | 785 | 0.417 | 0.173 | 0.613 | 0.270 |
| region=Bac | M3 | 52,560 | 785 | 0.433 | 0.259 | 0.585 | 0.359 |
| region=BacTrungBo | M1 | 35,040 | 1,108 | 0.540 | 0.600 | 0.506 | 0.549 |
| region=BacTrungBo | M2 | 35,040 | 1,108 | 0.527 | 0.510 | 0.561 | 0.534 |
| region=BacTrungBo | M3 | 35,040 | 1,108 | 0.526 | 0.570 | 0.537 | 0.553 |
| region=Nam | M1 | 17,520 | 1 | 0.000 | 0.000 | 0.000 | 0.000 |
| region=Nam | M2 | 17,520 | 1 | 0.000 | 0.000 | – | 0.000 |
| region=Nam | M3 | 17,520 | 1 | 0.000 | 0.000 | – | 0.000 |
| region=NamTrungBo | M1 | 35,040 | 546 | 0.334 | 0.504 | 0.315 | 0.387 |
| region=NamTrungBo | M2 | 35,040 | 546 | 0.323 | 0.346 | 0.366 | 0.356 |
| region=NamTrungBo | M3 | 35,040 | 546 | 0.335 | 0.432 | 0.344 | 0.383 |
| season=dry | M1 | 80,016 | 282 | 0.335 | 0.383 | 0.414 | 0.398 |
| season=dry | M2 | 80,016 | 282 | 0.332 | 0.202 | 0.449 | 0.279 |
| season=dry | M3 | 80,016 | 282 | 0.356 | 0.376 | 0.555 | 0.448 |
| season=rainy | M1 | 60,144 | 2,158 | 0.453 | 0.529 | 0.444 | 0.483 |
| season=rainy | M2 | 60,144 | 2,158 | 0.444 | 0.386 | 0.515 | 0.441 |
| season=rainy | M3 | 60,144 | 2,158 | 0.452 | 0.447 | 0.478 | 0.462 |

Nhóm có ít dòng dương thì PR-AUC dao động mạnh, không nên kết luận riêng từng nhóm.

## 4. Cohort và dữ liệu thiếu

Mỗi dòng dưới đây là số dòng có nhãn hợp lệ, và số dòng đủ feature nếu mỗi thí nghiệm dùng cohort riêng. Ablation chỉ dùng cohort chung (đủ feature M3), nên M1, M2, M3 được chấm trên đúng cùng tập dòng.

| split | labelled_rows | M1_eligible | M1_positives | M2_eligible | M2_positives | M3_eligible | M3_positives |
|---|---|---|---|---|---|---|---|
| test | 140,160 | 140,160 | 2,440 | 140,160 | 2,440 | 140,160 | 2,440 |
| validation | 69,888 | 69,888 | 743 | 69,888 | 743 | 69,888 | 743 |
| train | 280,320 | 279,752 | 4,032 | 279,752 | 4,032 | 279,752 | 4,032 |

Chi tiết null theo feature: `reports/quality/gold_missingness_by_split.csv`. Dòng bị purge vì cửa sổ nhãn vượt ranh giới split: `reports/quality/gold_split_exclusions.csv`.

## 5. Feature importance (5 cao nhất mỗi model)

- **M1**: rain_3h 0.180, cloud_mean_6h 0.154, rain_hours_24h 0.089, cloud_cover 0.070, rain_24h 0.059
- **M2**: rain_3h 0.179, cloud_mean_6h 0.125, rain_hours_24h 0.110, cloud_cover 0.081, rain_max_1h_24h 0.049
- **M3**: neighbor_rain_sum_6h 0.133, rain_3h 0.128, cloud_mean_6h 0.102, neighbor_rain_sum_24h 0.077, rain_hours_24h 0.073

Importance kiểu giảm impurity thiên vị feature liên tục, nhiều giá trị và chia sẻ điểm giữa các feature tương quan; chỉ dùng để mô tả, không phải bằng chứng nhân quả.

## 6. Hình

- `figures/pr_curves_test.png`
- `figures/confusion_matrices_test.png`
- `figures/feature_importance.png`

## 7. Diễn giải và giới hạn

- M2 - M1: Δ PR-AUC -0.0079, chưa đủ bằng chứng (CI 95% chứa 0).
- M3 - M1: Δ PR-AUC -0.0001, chưa đủ bằng chứng (CI 95% chứa 0).
- M3 - M2: Δ PR-AUC +0.0079, chưa đủ bằng chứng (CI 95% chứa 0).
- Feature địa lý là hằng số theo điểm. Với ít điểm, RF có thể dùng chúng như mã nhận diện điểm (học tần suất mưa lớn riêng của từng nơi) chứ không phải học quan hệ địa hình tổng quát. Muốn tách hai hiệu ứng cần nhiều điểm hơn hoặc đánh giá leave-location-out.
- Nhãn là mưa ERA5 trên ô ~25 km. Kiểm chứng với 14 trạm NOAA (2015–2024) cho thấy ERA5 bắt đúng ngày khoảng 24% số ngày trạm > 50 mm (`validation/report_era5.md`); kết quả ở đây là khả năng dự báo mưa lớn theo ERA5, không phải theo trạm đo.
- Các giờ liên tiếp tương quan mạnh; CI dùng bootstrap theo ngày nhưng một đợt mưa nhiều ngày vẫn có thể làm CI hẹp hơn thực tế.
- Feature lân cận chỉ dùng giá trị tại hoặc trước t của các ô lân cận, ô trung tâm không được tính làm hàng xóm (`data/reference/grid_plan.csv`).

## 8. Tái lập

```
python -m src.ingest.collect_era5 --profile pilot
python -m src.ingest.collect_geography --profile pilot
python -m src.pipeline.bronze_silver --profile pilot
python -m src.pipeline.gold_features --profile pilot
python -m src.experiments.ablation --profile pilot
```

Metadata đầy đủ (config hash, phiên bản, ngưỡng, số dòng): `reports/ablation_manifest.json`.
