


import time
from abc import ABC, abstractmethod

from App.threads.Thread import abThread


class abThreading ( abThread, ABC):

    def __init__(self , sleep_time = 0):
        abThread.__init__(self)
        self.__sleep_time = sleep_time

    def buildSleepTime(self , sleep_time):
        self.__sleep_time=sleep_time
        return self

    def run(self):
        while not self.IsStop():
            self.HandleThread()
            time.sleep(self.__sleep_time)

    @abstractmethod
    def HandleThread(self):
        print("abThreading ")
        pass



