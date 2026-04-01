from abc import ABC, abstractmethod


class AcademicCalendar(ABC):

    @abstractmethod
    async def get_events(self, date):
        raise NotImplementedError
