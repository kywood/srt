import time
import random
from datetime import datetime
from pyspark.sql import SparkSession


SPARK_CONNECT_SERVERS = [
    "sc://172.22.156.174:15002",
    "sc://172.22.156.174:15003",
    "sc://172.22.156.174:15004",
]


def get_alive_session():
    servers = SPARK_CONNECT_SERVERS.copy()
    random.shuffle(servers)

    for server in servers:
        try:
            spark = SparkSession.builder.remote(server).getOrCreate()
            spark.sql("SELECT 1").collect()
            print(f"✅ alive: {server}")
            return spark, server
        except Exception:
            print(f"❌ dead: {server}")
            continue
    return None, None


def main():

    print("11111111")
    spark, server = get_alive_session()
    print("22222222222")
    if not spark:
        print("🚨 살아있는 서버 없음!")
        return

    while True:
        st = datetime.now()
        try:
            result = spark.sql("SELECT 1").collect()
            print(f"[{server}] {result}")
        except Exception:
            print(f"⚠️ {server} 연결 끊김. 다른 서버 탐색 중...")
            spark, server = get_alive_session()
            if not spark:
                print("🚨 살아있는 서버 없음!")
                return
            continue

        diff_sec = (datetime.now() - st).total_seconds()
        print(f"⏱️ {diff_sec:.3f}초")
        time.sleep(1)


if __name__ == '__main__':
    main()