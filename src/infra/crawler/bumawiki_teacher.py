from collections import defaultdict
from enum import Enum

from src.core.crawler import Crawler
from src.core.requester import Requester

class TeacherType(str, Enum):
    GENERAL = "teacher"
    MAJOR = "major_teacher"
    MENTOR = "mentor_teacher"
    # 보통 교과 = teacher
    # 전공 교과 = major_teacher
    # 멘토 선생님 = mentor_teacher

class BumawikiTeacherCrawler(Crawler):

    def __init__(
            self,
            requester : Requester,
    ):
        super().__init__(requester)
        self.base_url = "https://buma.wiki/api/docs/{teacher}"


    def _fetch(self):
        dict_teachers = defaultdict(list)
        for teacher_type in TeacherType:
            url = self.base_url.format(teacher=teacher_type.value)
            fetch_data = self.requester.get(url=url)
            fetch_data.raise_for_status()
            data = fetch_data.json()["data"]

            for year, teachers in data.items():
                dict_teachers[str(year)].extend(teachers)
        return dict_teachers

    def _parse(self, fetched_data):
        return dict(fetched_data)
