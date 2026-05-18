from abc import abstractmethod

from App.threads.Thading import abThreading


class SparkPingThading(abThreading):



    def __init__(self , * , parent ,  spark_session , sleep_time = 10):
        abThreading.__init__(self, sleep_time)
        self.__parent = parent
        self.__sparkSession = spark_session

    def _getParent(self):
        return self.__parent

    def HandleThread(self):
        print("abThreading::sparkSession.sq ")

        sparkSession = self._getParent().getSparkSession()

        if sparkSession:
            sparkSession.sql("select  1").collect()


        pass


    pass

class SparkLocal:

    def __init__(self , * , spark_session):
        self.__sparkSession = None
        self.__threading =  SparkPingThading(parent = self , spark_session=spark_session , sleep_time=10 )

        pass

    def getSparkSession(self):
        return self.__sparkSession


    def SetSaprkSession(self , spark_session):
        self.__sparkSession = spark_session
        pass

    def Start(self):
        self.__threading.Start()

    def Stop(self):
        self.__threading.Stop()


def main():
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("SparkLocal")
             .getOrCreate())
    sparkLocal = SparkLocal(spark_session = spark)

    sparkLocal.Start()





    pass

if __name__ == '__main__':
    main()