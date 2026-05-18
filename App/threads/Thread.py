import threading
from abc import abstractmethod, ABC

from App.Defs.IEnum import IENUM


class cThreadEvent:
    def __init__(self):
        self.vset=False
    def evt_set(self):
        self.vset=True
        return self.vset
    def evt_reset(self):
        self.vset=False
        return self.vset
    def evt_clear(self):
        self.vset=False
    def evt_is_set(self):
        return self.vset


class E_THREAD_STATUS(IENUM):
    WAITING = 0
    RUNNING = 1
    STOPING = 2
    STOPPED = 3


class IThread(ABC):


    def setThreadStatus(self , status ): ...

    def GetThreadStatus(self): ...

    def Stop(self): ...

    def IsStop(self): ...

    def IsRunning(self): ...

    def Start(self): ...

    def run(self): ...

    @abstractmethod
    def HandleThread(self): ...


class abThread ( threading.Thread , cThreadEvent , IThread , ABC ):


    def __init__( self ):
        threading.Thread.__init__(self)
        cThreadEvent.__init__(self)

        self.__thread_status = E_THREAD_STATUS.WAITING


    def setThreadStatus(self , status ):
        self.__thread_status=status

    def GetThreadStatus(self):
        return self.__thread_status

    def Stop(self):
        self.evt_set()
        self.setThreadStatus(E_THREAD_STATUS.STOPING)

    def IsStop(self):
        return self.evt_is_set()

    def IsRunning(self):
        return not self.IsStop()

    def Start(self):
        self.start()
        self.evt_clear()
        self.setThreadStatus(E_THREAD_STATUS.RUNNING)

    def run(self):
        self.HandleThread()
        self.Stop()

    @abstractmethod
    def HandleThread(self):
        print("abThread ")
        pass

