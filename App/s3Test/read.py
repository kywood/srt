from pyspark.sql import SparkSession
import time

print("🚀 Spark Connect 연결 중...")
spark = SparkSession.builder.remote("sc://172.22.156.174:15002").getOrCreate()
print("✅ 연결 성공!\n")

# distribution-mode 설정
spark.conf.set("spark.sql.iceberg.distribution-mode", "hash")
print("✅ distribution-mode = hash 설정 완료\n")

# 10개 배치 한번에 읽기
print("⏳ S3에서 전체 parquet 읽는 중...")
df = spark.read.parquet("s3a://warehouse/raw/detection_logs_batch_*")
print(f"✅ 읽기 완료: {df.count()}건\n")

# Iceberg에 적재
print("⏳ Iceberg 테이블에 저장 중...")
start = time.time()
df.sortWithinPartitions("detect_time", "user_id").writeTo("nessie.herb24.detection_logs").append()
print(f"✅ 적재 완료! ({time.time() - start:.2f}초)\n")

# 검증
spark.sql("SELECT count(*) as total FROM nessie.herb24.detection_logs").show()

spark.stop()