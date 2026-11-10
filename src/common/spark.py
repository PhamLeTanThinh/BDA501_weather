"""SparkSession dùng chung. Múi giờ phiên cố định Asia/Ho_Chi_Minh để chuỗi giờ của API (giờ VN, không
có ký hiệu múi) được hiểu đúng; Việt Nam không có giờ mùa hè nên ts_epoch cách đều 3600 giây."""
import os
import time

from pyspark.sql import SparkSession

TZ = "Asia/Ho_Chi_Minh"


def get_spark(app, master="local[*]", driver_memory=None, shuffle_partitions=16, conf=None):
    driver_memory = driver_memory or os.environ.get("SPARK_DRIVER_MEMORY") or "4g"
    # collect() đổi timestamp sang giờ địa phương của tiến trình Python, nên đặt cùng múi với phiên Spark
    os.environ["TZ"] = TZ
    if hasattr(time, "tzset"):
        time.tzset()
    b = SparkSession.builder.appName(app).master(master)
    for k, v in (conf or {}).items():
        b = b.config(k, v)
    return (b.config("spark.driver.memory", driver_memory)
            .config("spark.sql.session.timeZone", TZ)
            .config("spark.sql.shuffle.partitions", shuffle_partitions)
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())
