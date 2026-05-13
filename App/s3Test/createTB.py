from pyspark.sql import SparkSession
import time

## wsl 주소 : http://172.22.156.174/

print("🚀 Spark Connect 서버에 연결 중...")
spark = SparkSession.builder.remote("sc://172.22.156.174:15002").getOrCreate()
print("✅ 연결 성공!\n")

# 1. 네임스페이스 및 테이블 생성 (기존과 동일)
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.herb24")


# # ✨ 핵심 추가: 기존에 만든 4컬럼짜리 테이블을 완전히 삭제합니다!
# print("🗑️ 기존의 구형 테이블을 삭제합니다...")
# spark.sql("DROP TABLE IF EXISTS nessie.herb24.detection_logs")

create_table_query = """
CREATE TABLE IF NOT EXISTS nessie.herb24.detection_logs (
    detect_id STRING,
    user_id BIGINT,
    herb_name STRING,
    confidence DOUBLE,
    latitude DOUBLE,
    longitude DOUBLE,
    device_os STRING,
    detect_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (months(detect_time), bucket(4, user_id))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read',
    'write.target-file-size-bytes' = '134217728'
)
"""
spark.sql(create_table_query)
print("✅ 테이블 준비 완료!\n")



spark.stop()