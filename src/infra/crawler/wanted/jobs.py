import logging
import time
from dataclasses import dataclass, asdict
from typing import Optional

from src.core.requester import Requester

logger = logging.getLogger(__name__)

LIST_API_URL = "https://www.wanted.co.kr/api/chaos/navigation/v1/results"
DETAIL_API_URL = "https://www.wanted.co.kr/api/chaos/jobs/v4/{id}/details"

DEFAULT_PARAMS = {
    "job_group_id": 518,
    "country": "kr",
    "job_sort": "job.latest_order",
    "years": 0,
    "locations": "seoul.all",
    "limit": 20,
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://www.wanted.co.kr/",
}


@dataclass
class WantedJobPosting:
    id: int
    company_id: int
    company_name: str
    position: str
    location: str
    district: str
    employment_type: str
    annual_from: int
    annual_to: int
    is_newbie: bool
    category_id: int
    additional_apply_type: Optional[list]
    requirements: Optional[str]

    def to_dict(self) -> dict:
        return asdict(self)


class WantedJobsCrawler:
    def __init__(self, requester: Requester):
        self.requester = requester

    def run(self) -> list[WantedJobPosting]:
        all_items: list[WantedJobPosting] = []
        offset = 0

        while True:
            params = {**DEFAULT_PARAMS, "offset": offset}
            try:
                resp = self.requester.get(url=LIST_API_URL, headers=DEFAULT_HEADERS, params=params)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.warning(f"[wanted] offset={offset} 요청 실패: {e}")
                break

            items = data.get("data", [])
            if not items:
                break

            parsed = [self._parse_item(item) for item in items]
            all_items.extend(parsed)
            logger.info(f"[wanted] offset={offset} → {len(parsed)}건 수집 (누적 {len(all_items)}건)")

            if not data.get("links", {}).get("next"):
                break

            offset += DEFAULT_PARAMS["limit"]
            time.sleep(0.5)

        logger.info(f"[wanted] 목록 {len(all_items)}건 수집 완료. 상세 조회 시작")

        for posting in all_items:
            posting.requirements = self._fetch_requirements(posting.id)
            time.sleep(0.3)

        logger.info(f"[wanted] 상세 조회 완료")
        return all_items

    def _fetch_requirements(self, job_id: int) -> Optional[str]:
        url = DETAIL_API_URL.format(id=job_id)
        try:
            resp = self.requester.get(url=url, headers=DEFAULT_HEADERS)
            resp.raise_for_status()
            detail = resp.json()
            return detail["data"]["job"]["detail"].get("requirements")
        except Exception as e:
            logger.warning(f"[wanted] id={job_id} 상세 조회 실패: {e}")
            return None

    @staticmethod
    def _parse_item(item: dict) -> WantedJobPosting:
        return WantedJobPosting(
            id=item["id"],
            company_id=item["company"]["id"],
            company_name=item["company"]["name"],
            position=item["position"],
            location=item["address"]["location"],
            district=item["address"]["district"],
            employment_type=item.get("employment_type", ""),
            annual_from=item.get("annual_from", 0),
            annual_to=item.get("annual_to", 0),
            is_newbie=item.get("is_newbie", False),
            category_id=item["category_tag"]["id"],
            additional_apply_type=item.get("additional_apply_type"),
            requirements=None,  # 상세 조회 후 채워짐
        )
