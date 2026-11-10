"""Feature khí quyển M4 từ 6 biến cột khí quyển của ERA5 (chỉ có ở dữ liệu CDS).

Cùng quy ước với local_weather: feature tại t chỉ dùng các mốc <= t, cửa sổ theo event time và
phân vùng theo ô/điểm, cửa sổ thiếu giờ → null.

  tcwv, tcwv_change_24h     lượng hơi nước cả cột (mm) và biến thiên 24 giờ: ẩm có sẵn
  ivt, ivt_dir_sin/cos      độ lớn và hướng dòng vận chuyển hơi nước (kg m⁻¹ s⁻¹): ẩm được đưa tới
  vimd_sum_6h               tổng độ phân kỳ ẩm 6 giờ; âm = ẩm đang hội tụ về ô
  cape, cape_max_24h        năng lượng đối lưu (J/kg) hiện tại và lớn nhất 24 giờ qua
  k_index                   chỉ số K: khả năng dông
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.features.local_weather import _at, _complete

ATMOS_FEATURES = ["tcwv", "tcwv_change_24h", "ivt", "ivt_dir_sin", "ivt_dir_cos",
                  "vimd_sum_6h", "cape", "cape_max_24h", "k_index"]


def atmos_features(df: DataFrame, key="location_id") -> DataFrame:
    """Thêm ATMOS_FEATURES. df cần: key, ts_epoch, tcwv, ivt_east, ivt_north, vimd, cape, k_index."""
    e, n = F.col("ivt_east"), F.col("ivt_north")
    ivt = F.sqrt(e * e + n * n)
    return (df
            .withColumn("tcwv_change_24h", F.col("tcwv") - F.first("tcwv").over(_at(key, 24)))
            .withColumn("ivt", ivt)
            # hướng dòng ẩm đi tới (không phải hướng gió thổi từ); ivt = 0 → null
            .withColumn("ivt_dir_sin", F.when(ivt > 0, e / ivt))
            .withColumn("ivt_dir_cos", F.when(ivt > 0, n / ivt))
            .withColumn("vimd_sum_6h", _complete("vimd", key, 6, F.sum))
            .withColumn("cape_max_24h", _complete("cape", key, 24, F.max)))
