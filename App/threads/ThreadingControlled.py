import time
from abc import ABC, abstractmethod

from App.threads.Thading import abThreading


class IThreadingControlled(ABC):

    def run(self): ...

    def getName(self): ...

    def setException(self , e ): ...

    @abstractmethod
    def HandleThread(self): ...


    def Start(self): ...

    def Stop(self): ...





class abThreadingControlled (IThreadingControlled , abThreading , ABC ):

    def __init__(self,thread_controller , name, sleep_time=0):
        abThreading.__init__(self,sleep_time = sleep_time)
        self.__thread_controller = thread_controller
        self.__name = name
    def run(self):

        try:
            while not self.IsStop():
                self.HandleThread()
                time.sleep(self.__sleep_time)
        except Exception as e:
            print(f"error:{e}")
            self.setException(e)

    def getName(self):
        return self.__name

    def setException(self , e ):
        self.__thread_controller.setException(e , self)
        pass

    @abstractmethod
    def HandleThread(self):
        print("abThreading ")
        pass
