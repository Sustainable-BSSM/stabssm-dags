import warnings
from collections import defaultdict

from src.core.bumawiki.model import TeacherType
from src.core.crawler import Crawler
from src.core.requester import Requester


class BumawikiTeacherCrawler(Crawler):
    """
    .. deprecated::
        buma.wiki 서비스가 schoolwiki.org로 이전됨.
        SchoolwikiDocsCrawler(category="TEACHER") 사용 권장.
    """

    def __init__(
            self,
            requester: Requester,
    ):
        warnings.warn(
            "BumawikiTeacherCrawler는 deprecated입니다. SchoolwikiDocsCrawler를 사용하세요.",
            DeprecationWarning,
            stacklevel=2,
        )
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
