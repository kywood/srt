from abc import ABC, abstractmethod
from typing import Any

class ITask(ABC):

    def __init__(self, name: str, *args, **kwargs):
        self._name = name
        self._args = args
        self._kwargs = kwargs

    @property
    def name(self):
        return self._name

    def __call__(self) -> Any:
        return self.execute()

    @abstractmethod
    def execute(self) -> Any:
        raise NotImplementedError




class CalcTask(ITask):

    def __init__(self, name, a, b):
        super().__init__(name, a, b)

    def execute(self) -> Any:
        a, b = self._args
        return a + b





def main():

    c= CalcTask("aa" , 10 , 20)

    rr=c.execute()

    rr=c()

    print(rr)



    pass


if __name__ == '__main__':
    main()