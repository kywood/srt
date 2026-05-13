from pyspark.sql import SparkSession

print("🚀 Spark Connect 연결 중...")
spark = SparkSession.builder.remote("sc://172.22.156.174:15002").getOrCreate()
print("✅ 연결 성공!\n")

# 1. Iceberg 테이블 삭제
print("🗑️ Iceberg 테이블 삭제 중...")
spark.sql("DROP TABLE IF EXISTS nessie.herb24.detection_logs")
print("✅ Iceberg 테이블 삭제 완료\n")
#
# # 2. S3 raw parquet 삭제
# print("🗑️ S3 raw parquet 삭제 중...")
# spark.sql("DROP TABLE IF EXISTS nessie.herb24.detection_logs_raw PURGE")
spark.stop()