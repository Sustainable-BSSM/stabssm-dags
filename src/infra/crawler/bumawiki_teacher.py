from collections import defaultdict
from src.common.enum.bumawiki.teacher_type import TeacherType
from src.core.crawler import Crawler
from src.core.requester import Requester

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
