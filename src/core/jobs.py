from abc import ABC, abstractmethod


class Job(ABC):

    @abstractmethod
    def __call__(self, *args, **kwargs):
        raise NotImplementedError