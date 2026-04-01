import asyncio
import logging
import os
from datetime import date, timedelta

import requests

from src.core.academic_calendar import AcademicCalendar

logger = logging.getLogger(__name__)

_NEIS_URL = "https://open.neis.go.kr/hub/SchoolSchedule"

# 부산광역시교육청 / 부산소프트웨어마이스터고등학교
# SD_SCHUL_CODE: NEIS 학교정보 API(schoolInfo)로 확인 가능
_ATPT_CODE = "C10"
_SCHUL_CODE = "7150658"


class BSSMAcademiCalender(AcademicCalendar):

    async def get_events(self, target_date: date) -> list[str]:
        """target_date 기준 ±7일 학사 일정 이벤트명 목록을 반환합니다."""
        return await asyncio.to_thread(self._fetch, target_date)

    def _fetch(self, target_date: date) -> list[str]:
        from_date = target_date - timedelta(days=3)
        to_date = target_date + timedelta(days=7)

        params = {
            "KEY": os.environ.get("NICES_API_KEY", ""),
            "Type": "json",
            "pIndex": 1,
            "pSize": 100,
            "ATPT_OFCDC_SC_CODE": _ATPT_CODE,
            "SD_SCHUL_CODE": _SCHUL_CODE,
            "AA_FROM_YMD": from_date.strftime("%Y%m%d"),
            "AA_TO_YMD": to_date.strftime("%Y%m%d"),
        }

        try:
            resp = requests.get(_NEIS_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"[BSSMAcademiCalender] NEIS API 요청 실패: {e}")
            return []

        schedule = data.get("SchoolSchedule", [])
        # NEIS API: [0] = HEADER, [1] = ROW
        if len(schedule) < 2:
            return []

        rows = schedule[1].get("row", [])
        return [row["EVENT_NM"] for row in rows if row.get("EVENT_NM")]
