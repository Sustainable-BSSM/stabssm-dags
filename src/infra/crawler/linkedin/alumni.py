import json
import re
from typing import Dict, Any, List, Optional
from src.core.crawler import Crawler
from src.core.model.linkedin import LinkedInPerson
from src.core.requester import Requester
from src.common.util.dict import DictUtils
from src.infra.requester.playwright import PlaywrightRequester
from src.infra.requester.http import HttpRequester

PROFILE_ID_RE = re.compile(r"urn:li:fsd_profile:([A-Za-z0-9_-]+)")

class LinkedInAlumniCrawler(Crawler):

    LINKEDIN_URL = "https://www.linkedin.com"
    GRAPHQL_API_URL = "https://www.linkedin.com/voyager/api/graphql"
    QUERY_ID = "voyagerSearchDashClusters.05111e1b90ee7fea15bebe9f9410ced9"
    SCHOOL_ID = "110553433"

    def __init__(
            self,
            playwright_requester: PlaywrightRequester,
            requester : Requester,
    ):
        super().__init__(requester=requester)
        self.playwright_requester = playwright_requester
        self._cookies: Optional[List[Dict[str, Any]]] = None
        self._user_agent: Optional[str] = None

    def _fetch(self, start: int = 0, count: int = 12):
        if not self._cookies:
            self._fetch_cookies()

        headers = {
            "csrf-token": self._get_csrf_token(),
            "cookie": self._get_cookie_header(),
            "user-agent": self._user_agent,
        }

        queries = {
            "start": start,
            "origin": "FACETED_SEARCH",
            "query": {
                "flagshipSearchIntent": "ORGANIZATIONS_PEOPLE_ALUMNI",
                "queryParameters": [
                    {
                        "key": "resultType",
                        "value": ["ORGANIZATION_ALUMNI"],
                    },
                    {
                        "key": "schoolFilter",
                        "value": [self.SCHOOL_ID],
                    },
                ],
                "includeFiltersInResponse": True,
            },
            "count": count,
        }
        variables = json.dumps(queries, separators=(",", ":"))
        params = {
            "variables": variables,
            "queryId": self.QUERY_ID,
        }

        fetched_data = self.requester.get(
            url=self.GRAPHQL_API_URL,
            headers=headers,
            params=params,
        )

        fetched_data.raise_for_status()

        return fetched_data

    def _parse(self, fetched_data) -> List[LinkedInPerson]:
        search_json = fetched_data.json()

        root = DictUtils.get_nested(search_json, ["data", "data", "searchDashClustersByAll"], {})
        elements = root.get("elements", []) or []
        included = search_json.get("included", []) or []

        # included에서 entityResultViewModel만 인덱싱
        by_profile_id: Dict[str, Dict[str, Any]] = {}
        for obj in included:
            if not isinstance(obj, dict):
                continue
            entity_urn = obj.get("entityUrn") or ""
            m = PROFILE_ID_RE.search(entity_urn)
            if not m:
                continue
            profile_id = m.group(1)
            if "title" in obj or "navigationUrl" in obj:
                by_profile_id[profile_id] = obj

        # elements에서 결과 순서대로 profile_id를 뽑고 by_profile_id로 조인
        results = []
        seen = set()

        for cluster in elements:
            items = (cluster or {}).get("items", []) or []
            for it in items:
                item = (it or {}).get("item", {}) or {}
                ent_ref = item.get("*entityResult") or ""
                m = PROFILE_ID_RE.search(ent_ref)
                if not m:
                    continue
                pid = m.group(1)
                if pid in seen:
                    continue
                seen.add(pid)

                obj = by_profile_id.get(pid, {})
                if not obj.get("navigationUrl"): # navigationUrl가 없다 = 비공개 프로필(수집 가치 없음)
                    continue
                results.append(
                    LinkedInPerson(
                        profile_id=pid,
                        name=DictUtils.get_nested(obj, ["title", "text"]),
                        headline=DictUtils.get_nested(obj, ["primarySubtitle", "text"]),
                        location=DictUtils.get_nested(obj, ["secondarySubtitle", "text"]),
                        profile_url=obj.get("navigationUrl"),
                        distance=DictUtils.get_nested(obj, ["badgeText", "text"]),
                    )
                )
        return results

    def _fetch_cookies(self):
        # LinkedIn 페이지 방문하여 세션 확인
        self.playwright_requester.get(
            url=f"{self.LINKEDIN_URL}/feed/",
        )
        self._cookies = self.playwright_requester.get_cookies(self.LINKEDIN_URL)
        self._user_agent = self.playwright_requester.get_user_agent()

    def _get_cookie_header(self) -> str:
        if not self._cookies:
            raise RuntimeError("Cookies not fetched. Call _fetch_cookies first.")
        return "; ".join(f"{c['name']}={c['value']}" for c in self._cookies)

    def _get_csrf_token(self) -> str:
        if not self._cookies:
            raise RuntimeError("Cookies not fetched. Call _fetch_cookies first.")
        for cookie in self._cookies:
            if cookie["name"] == "JSESSIONID":
                return cookie["value"].strip('"')
        raise RuntimeError("JSESSIONID cookie not found")



if __name__ == "__main__":
    with PlaywrightRequester(headless=False, profile_dir="Profile 2") as playwright_requester:
        crawler = LinkedInAlumniCrawler(
            playwright_requester=playwright_requester,
            requester=HttpRequester(),
        )
        result = crawler.run(0, 12)
        print(result)
