import time


def healthCheck():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote("sc://172.22.156.174:15002").getOrCreate()
    result = spark.sql("SELECT 1").collect()
    print(result)
    spark.stop()

    pass

def main():

    while True:
        print("1")
        from datetime import datetime
        st = datetime.now()
        healthCheck()
        ed = datetime.now()

        diff_sec = (ed-st).total_seconds()

        print(f" diff : {diff_sec}")

        time.sleep(1)
    pass


if __name__ == '__main__':
    main()