import logging
import re
from datetime import date

from src.dependencies.llm import get_ari_llm
from src.infra.bssm_academi_calender import BSSMAcademiCalender

logger = logging.getLogger(__name__)

_WEEKDAY_KO = ["월", "화", "수", "목", "금", "토", "일"]

_SEASONS = {
    3: "봄", 4: "봄", 5: "봄",
    6: "여름", 7: "여름", 8: "여름",
    9: "가을", 10: "가을", 11: "가을",
    12: "겨울", 1: "겨울", 2: "겨울",
}

# NEIS API 실패 시 사용하는 월별 fallback 힌트
_ACADEMIC_HINTS = {
    3: "새 학기 시작, 개학",
    4: "중간고사 시험기간",
    5: "중간고사 이후, 프로젝트 시즌",
    6: "기말고사 시험기간",
    7: "여름방학 시작",
    8: "여름방학",
    9: "2학기 개학",
    10: "중간고사 시험기간",
    11: "기말고사 시험기간",
    12: "겨울방학 시작, 학기 마무리",
    1: "겨울방학",
    2: "겨울방학, 졸업 및 입학 시즌",
}

_PROMPT = """발간일: {today}
계절: {season}
이번 주 학사 일정: {events}
발간호: {issue}

위 정보를 바탕으로, 독자(BSSM 학생·교사·학부모)에게 건네는 인사말을 2~3문장으로 써줘.
- 계절감과 학사 일정을 자연스럽게 녹여
- 이어질 뉴스에 대한 기대감으로 마무리
- 인사말 텍스트만 출력
- 괄호 안 설명, 메타 주석, 작성 의도 설명 절대 포함하지 마"""

_FALLBACK = "안녕. 아리야. 이번 주도 BSSM 소식 가져왔어. 같이 확인해보자."


def _strip_meta(text: str) -> str:
    """LLM이 출력한 괄호 안 메타 주석 제거."""
    return re.sub(r'\s*\([^)]*\)', '', text).strip()


class GreetingGenerator:

    def __init__(self):
        self._llm = get_ari_llm()
        self._calendar = BSSMAcademiCalender()

    async def generate(self, week: str, today: date) -> str:
        year, month, week_num = week.split("-")
        issue = f"{year}년 {int(month)}월 {int(week_num)}주차"
        today_str = f"{today.year}년 {today.month}월 {today.day}일 ({_WEEKDAY_KO[today.weekday()]})"
        season = _SEASONS[today.month]

        events = await self._calendar.get_events(today)
        if events:
            events_str = ", ".join(events)
        else:
            events_str = _ACADEMIC_HINTS[today.month]

        try:
            result = await self._llm.chat(
                query=_PROMPT,
                variables=[today_str, season, events_str, issue],
            )
            return _strip_meta(result)
        except Exception as e:
            logger.warning(f"[GreetingGenerator] LLM 실패: {e}")
            return _FALLBACK
