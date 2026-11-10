# Ablation M1 / M2 / M3 — mưa lớn 24 giờ tới (pilot)

Run `20261031T140936Z_pilot` · Spark 4.2.0 · seed 42 · dữ liệu 2019–2025 · 12 điểm: HCMC, HGIA, HNOI, HPHO, HUE, KHOA, KTUM, LCHA, LDON, NAN, QNGA, QNIN

Nhãn: tổng mưa ERA5 trong (t, t+24h] > 50 mm. Mô hình: Spark MLlib RandomForest cố định (100 cây, maxDepth 12), trọng số lớp dương w1 = N0/N1 = 79.6 tính trên train. Ngưỡng chọn theo F1 cao nhất trên validation 2023 rồi khóa cho test 2024–2025.

> **Đây là chạy pilot**, không phải kết quả cuối: ít điểm, train chỉ 2019–2022. Kết luận chỉ mang tính kiểm chứng pipeline và xu hướng ban đầu.

## 1. Kết quả trên test (cohort chung)

| Model | Số feature | PR-AUC | CI 95% (block bootstrap theo ngày) | ROC-AUC | Ngưỡng | Recall | Precision | F1 | TP | FP | FN |
|---|---|---|---|---|---|---|---|---|---|---|---|
| M1 | 24 | **0.375** | [0.298, 0.451] | 0.945 | 0.848 | 0.342 | 0.467 | 0.395 | 1,103 | 1,259 | 2,124 |
| M2 | 27 | **0.362** | [0.287, 0.435] | 0.946 | 0.837 | 0.326 | 0.450 | 0.378 | 1,053 | 1,288 | 2,174 |
| M3 | 31 | **0.376** | [0.297, 0.451] | 0.946 | 0.825 | 0.373 | 0.445 | 0.406 | 1,205 | 1,504 | 2,022 |
| persistence_rain24h | 1 | **0.184** | – | 0.885 | 36.700 | 0.390 | 0.223 | 0.283 | 1,259 | 4,397 | 1,968 |
| climatology_loc_month | 1 | **0.058** | – | 0.715 | 0.174 | 0.066 | 0.144 | 0.091 | 214 | 1,274 | 3,013 |

Test: 210,240 dòng điểm × giờ, 3,227 dòng dương (tỷ lệ 1.53%). PR-AUC của một bộ đoán ngẫu nhiên bằng đúng tỷ lệ dương. persistence và climatology là baseline tham khảo, không thuộc ablation.

Validation 2023 (dùng để chọn ngưỡng): M1 PR-AUC 0.226, M2 PR-AUC 0.243, M3 PR-AUC 0.242

## 2. Hiệu giữa các thí nghiệm (test)

| So sánh | Δ PR-AUC | CI 95% | Tỷ lệ mẫu bootstrap có Δ ≤ 0 | Δ Recall | Δ Precision | Δ F1 | Đánh giá |
|---|---|---|---|---|---|---|---|
| M2 - M1 | -0.0127 | [-0.0264, +0.0053] | 0.93 | -0.015 | -0.017 | -0.016 | chưa đủ bằng chứng (CI 95% chứa 0) |
| M3 - M1 | +0.0010 | [-0.0114, +0.0154] | 0.44 | +0.032 | -0.022 | +0.011 | chưa đủ bằng chứng (CI 95% chứa 0) |
| M3 - M2 | +0.0137 | [+0.0008, +0.0257] | 0.02 | +0.047 | -0.005 | +0.028 | cải thiện (CI 95% của hiệu > 0) |

Bootstrap: 500 lần lấy mẫu lại theo ngày (mọi giờ, mọi điểm của một ngày đi cùng nhau), ba model dùng chung mẫu nên hiệu được ghép cặp. Δ Recall/Precision/F1 đo tại ngưỡng riêng của từng model.

## 3. Theo vùng và mùa (test, ngưỡng đã khóa)

| Nhóm | Model | n | Dương | PR-AUC | Recall | Precision | F1 |
|---|---|---|---|---|---|---|---|
| region=Bac | M1 | 87,600 | 1,289 | 0.358 | 0.199 | 0.539 | 0.291 |
| region=Bac | M2 | 87,600 | 1,289 | 0.322 | 0.143 | 0.524 | 0.224 |
| region=Bac | M3 | 87,600 | 1,289 | 0.360 | 0.233 | 0.541 | 0.325 |
| region=BacTrungBo | M1 | 35,040 | 1,108 | 0.528 | 0.526 | 0.587 | 0.555 |
| region=BacTrungBo | M2 | 35,040 | 1,108 | 0.530 | 0.543 | 0.531 | 0.537 |
| region=BacTrungBo | M3 | 35,040 | 1,108 | 0.531 | 0.562 | 0.530 | 0.546 |
| region=Nam | M1 | 17,520 | 1 | 0.000 | 0.000 | 0.000 | 0.000 |
| region=Nam | M2 | 17,520 | 1 | 0.000 | 0.000 | – | 0.000 |
| region=Nam | M3 | 17,520 | 1 | 0.000 | 0.000 | – | 0.000 |
| region=NamTrungBo | M1 | 35,040 | 546 | 0.324 | 0.410 | 0.333 | 0.368 |
| region=NamTrungBo | M2 | 35,040 | 546 | 0.308 | 0.416 | 0.333 | 0.370 |
| region=NamTrungBo | M3 | 35,040 | 546 | 0.326 | 0.432 | 0.314 | 0.364 |
| region=TayNguyen | M1 | 35,040 | 283 | 0.122 | 0.138 | 0.213 | 0.167 |
| region=TayNguyen | M2 | 35,040 | 283 | 0.110 | 0.141 | 0.229 | 0.175 |
| region=TayNguyen | M3 | 35,040 | 283 | 0.119 | 0.163 | 0.202 | 0.180 |
| season=dry | M1 | 114,768 | 367 | 0.280 | 0.294 | 0.489 | 0.367 |
| season=dry | M2 | 114,768 | 367 | 0.274 | 0.272 | 0.420 | 0.331 |
| season=dry | M3 | 114,768 | 367 | 0.274 | 0.327 | 0.430 | 0.372 |
| season=rainy | M1 | 95,472 | 2,860 | 0.387 | 0.348 | 0.465 | 0.398 |
| season=rainy | M2 | 95,472 | 2,860 | 0.373 | 0.333 | 0.453 | 0.384 |
| season=rainy | M3 | 95,472 | 2,860 | 0.389 | 0.379 | 0.447 | 0.410 |

Nhóm có ít dòng dương thì PR-AUC dao động mạnh, không nên kết luận riêng từng nhóm.

## 4. Cohort và dữ liệu thiếu

Mỗi dòng dưới đây là số dòng có nhãn hợp lệ, và số dòng đủ feature nếu mỗi thí nghiệm dùng cohort riêng. Ablation chỉ dùng cohort chung (đủ feature M3), nên M1, M2, M3 được chấm trên đúng cùng tập dòng.

| split | labelled_rows | M1_eligible | M1_positives | M2_eligible | M2_positives | M3_eligible | M3_positives |
|---|---|---|---|---|---|---|---|
| test | 210,240 | 210,240 | 3,227 | 210,240 | 3,227 | 210,240 | 3,227 |
| validation | 104,832 | 104,832 | 896 | 104,832 | 896 | 104,832 | 896 |
| train | 420,480 | 419,628 | 5,204 | 419,628 | 5,204 | 419,628 | 5,204 |

Chi tiết null theo feature: `reports/quality/gold_missingness_by_split.csv`. Dòng bị purge vì cửa sổ nhãn vượt ranh giới split: `reports/quality/gold_split_exclusions.csv`.

## 5. Feature importance (5 cao nhất mỗi model)

- **M1**: rain_3h 0.182, cloud_mean_6h 0.169, cloud_cover 0.077, rain_24h 0.068, rain_hours_24h 0.068
- **M2**: rain_3h 0.181, cloud_mean_6h 0.133, cloud_cover 0.116, rain_hours_24h 0.069, rain_max_1h_24h 0.061
- **M3**: rain_3h 0.136, neighbor_rain_sum_6h 0.130, cloud_mean_6h 0.113, cloud_cover 0.078, neighbor_rain_sum_24h 0.062

Importance kiểu giảm impurity thiên vị feature liên tục, nhiều giá trị và chia sẻ điểm giữa các feature tương quan; chỉ dùng để mô tả, không phải bằng chứng nhân quả.

## 6. Hình

- `figures/pr_curves_test.png`
- `figures/confusion_matrices_test.png`
- `figures/feature_importance.png`

## 6b. Leave-location-out (điểm chưa từng thấy)

Mỗi fold giữ riêng một điểm: model train và chọn ngưỡng trên các điểm còn lại, rồi dự báo test 2024–2025 cho điểm bị giữ lại. Feature địa lý chỉ có ích ở đây nếu model học được quy luật địa hình chung, không thể dùng như mã nhận diện điểm.

| Model | PR-AUC gộp | CI 95% | Recall | Precision | F1 | TP | FP | FN |
|---|---|---|---|---|---|---|---|---|
| M1 | **0.363** | [0.282, 0.441] | 0.354 | 0.478 | 0.406 | 1,141 | 1,246 | 2,086 |
| M2 | **0.344** | [0.273, 0.418] | 0.249 | 0.504 | 0.333 | 803 | 789 | 2,424 |
| M3 | **0.352** | [0.275, 0.430] | 0.277 | 0.507 | 0.359 | 895 | 869 | 2,332 |

| So sánh | Δ PR-AUC | CI 95% | Tỷ lệ mẫu có Δ ≤ 0 | Đánh giá |
|---|---|---|---|---|
| M2 - M1 | -0.0193 | [-0.0377, -0.0011] | 0.98 | kém đi (CI 95% của hiệu < 0) |
| M3 - M1 | -0.0111 | [-0.0283, +0.0065] | 0.90 | chưa đủ bằng chứng (CI 95% chứa 0) |
| M3 - M2 | +0.0082 | [-0.0005, +0.0181] | 0.03 | chưa đủ bằng chứng (CI 95% chứa 0) |

PR-AUC theo từng điểm bị giữ lại (điểm ít dòng dương thì dao động mạnh):

| Điểm | Dòng dương | M1 | M2 | M3 |
|---|---|---|---|---|
| HCMC | 1 | 0.000 | 0.000 | 0.000 |
| HGIA | 241 | 0.330 | 0.199 | 0.315 |
| HNOI | 276 | 0.366 | 0.263 | 0.218 |
| HPHO | 245 | 0.427 | 0.434 | 0.438 |
| HUE | 690 | 0.558 | 0.550 | 0.545 |
| KHOA | 177 | 0.288 | 0.283 | 0.308 |
| KTUM | 225 | 0.148 | 0.131 | 0.127 |
| LCHA | 263 | 0.339 | 0.315 | 0.313 |
| LDON | 58 | 0.086 | 0.082 | 0.108 |
| NAN | 418 | 0.468 | 0.454 | 0.450 |
| QNGA | 369 | 0.325 | 0.279 | 0.321 |
| QNIN | 264 | 0.424 | 0.404 | 0.427 |

PR-AUC gộp tính trên xác suất của nhiều model fold khác nhau; chỉ so sánh M1/M2/M3 với nhau.

## 7. Diễn giải và giới hạn

- Leave-location-out, M2 - M1: Δ PR-AUC -0.0193, kém đi (CI 95% của hiệu < 0).
- Leave-location-out, M3 - M1: Δ PR-AUC -0.0111, chưa đủ bằng chứng (CI 95% chứa 0).
- Leave-location-out, M3 - M2: Δ PR-AUC +0.0082, chưa đủ bằng chứng (CI 95% chứa 0).
- M2 - M1: Δ PR-AUC -0.0127, chưa đủ bằng chứng (CI 95% chứa 0).
- M3 - M1: Δ PR-AUC +0.0010, chưa đủ bằng chứng (CI 95% chứa 0).
- M3 - M2: Δ PR-AUC +0.0137, cải thiện (CI 95% của hiệu > 0).
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
