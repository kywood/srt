import random
from pyspark.sql import SparkSession

SPARK_CONNECT_SERVERS = [
    "sc://172.22.156.174:15002",
    "sc://172.22.156.174:15003",
    "sc://172.22.156.174:15004",
]


class SparkPool:
    def __init__(self):
        self._spark = None

    def get(self) -> SparkSession:
        if self._spark:
            try:
                self._spark.sql("SELECT 1").collect()
                return self._spark
            except Exception:
                print("⚠️ 기존 연결 끊김. 재연결 중...")
                self._spark = None

        servers = SPARK_CONNECT_SERVERS.copy()
        random.shuffle(servers)
        for server in servers:
            try:
                spark = SparkSession.builder.remote(server).getOrCreate()
                spark.sql("SELECT 1").collect()
                print(f"✅ connected: {server}")
                self._spark = spark
                return spark
            except Exception:
                print(f"❌ dead: {server}")
        raise RuntimeError("🚨 살아있는 서버 없음!")


pool = SparkPool()


# from spark_pool import pool

# 잡 1 - 기존 세션 재사용
spark = pool.get()
spark.sql("SELECT count(*) FROM nessie.herb24.detection_logs").show()

# 잡 2 - 같은 세션 그대로
spark = pool.get()  # 헬스체크 통과하면 기존 세션 반환
df = spark.read.parquet("s3a://warehouse/raw/detection_logs_batch_*")
df.writeTo("nessie.herb24.detection_logs").append()

# 잡 3 - 서버 죽었으면 자동 failover
spark = pool.get()  # 기존 서버 죽었으면 다른 서버로
spark.sql("SELECT count(*) FROM nessie.herb24.detection_logs").show()
