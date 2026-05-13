from pyspark.sql import SparkSession
import time

## wsl 주소 : http://172.22.156.174/

print("🚀 Spark Connect 서버에 연결 중...")
spark = SparkSession.builder.remote("sc://172.22.156.174:15002").getOrCreate()
print("✅ 연결 성공!\n")

for i in range(1, 11):
    print(f"⏳ [{i}/10] 3만건 생성 중...")
    df = spark.range(30000).selectExpr(
        "uuid() as detect_id",
        "cast(rand() * 10000 as bigint) + 1 as user_id",
        "element_at(array('홍삼', '당귀', '감초', '도라지', '인삼', '천남성', '하수오', '천마'), cast(rand() * 8 as int) + 1) as herb_name",
        "round(rand() * 0.5 + 0.5, 4) as confidence",
        "round(33.0 + rand() * 5.0, 6) as latitude",
        "round(126.0 + rand() * 4.0, 6) as longitude",
        "element_at(array('Android', 'iOS'), cast(rand() * 2 as int) + 1) as device_os",
        "timestamp('2025-01-01 00:00:00') + interval 1 second * cast(rand() * 31536000 as int) as detect_time"
    )

    output_path = f"s3a://warehouse/raw/detection_logs_batch_{i:02d}"
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    print(f"✅ [{i}/10] 저장 완료: {output_path}")

print("\n🎉 전체 10개 배치 (30만건) 저장 완료!")
spark.stop()