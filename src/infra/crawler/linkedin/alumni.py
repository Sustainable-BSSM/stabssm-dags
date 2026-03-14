import json
import re
from pathlib import Path
from typing import Dict, Any, List, Optional

from playwright.sync_api import sync_playwright

from src.common.util.dict import DictUtils
from src.core.crawler import Crawler
from src.core.linkedin import LinkedInPerson
from src.core.requester import Requester
from src.infra.requester.http import HttpRequester

PROFILE_ID_RE = re.compile(r"urn:li:fsd_profile:([A-Za-z0-9_-]+)")


class LinkedInAlumniCrawler(Crawler):
    LINKEDIN_URL = "https://www.linkedin.com"
    GRAPHQL_API_URL = "https://www.linkedin.com/voyager/api/graphql"
    QUERY_ID = "voyagerSearchDashClusters.05111e1b90ee7fea15bebe9f9410ced9"
    SCHOOL_ID = "110553433"

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )

    def __init__(
            self,
            requester: Requester,
            login_info_path: str = "login_info.json",
    ):
        super().__init__(requester=requester)
        self.login_info_path = Path(login_info_path)
        self._cookies: Optional[Dict[str, str]] = None
        self._csrf_token: Optional[str] = None
        self._user_agent: Optional[str] = None

    def _fetch(self, start: int = 0, count: int = 12):
        if not self._cookies:
            self._fetch_cookies()

        params = {
            "includeWebMetadata": "true",
            "variables": (
                f"(start:{start},origin:FACETED_SEARCH,"
                f"query:(flagshipSearchIntent:ORGANIZATIONS_PEOPLE_ALUMNI,"
                f"queryParameters:List("
                f"(key:resultType,value:List(ORGANIZATION_ALUMNI)),"
                f"(key:schoolFilter,value:List({int(self.SCHOOL_ID)}))"
                f"),includeFiltersInResponse:true),"
                f"count:{count})"
            ),
            "queryId": self.QUERY_ID,
        }

        headers = {
            "csrf-token": self._get_csrf_token(),
            "user-agent": self._user_agent,
            "accept": "application/vnd.linkedin.normalized+json+2.1",
            "accept-language": "ko,en-US;q=0.9,en;q=0.8",
            "x-restli-protocol-version": "2.0.0",
            "x-li-lang": "ko_KR",
            "x-li-track": '{"clientVersion":"1.13.42584","mpVersion":"1.13.42584","osName":"web","timezoneOffset":9,"timezone":"Asia/Seoul","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":2,"displayWidth":2880,"displayHeight":1800}',
            "x-li-page-instance": "urn:li:page:schools_school_people_index;015c8357-b7ff-45f2-bdd8-7302157e49f0",
            "x-li-pem-metadata": "Voyager - Organization - Member=organization-people-card",
            "referer": "https://www.linkedin.com/school/bssmhs/people/?viewAsMember=true",
        }

        fetched_data = self.requester.get(
            url=self.GRAPHQL_API_URL,
            headers=headers,
            params=params,
            cookies=self._cookies,
        )

        breakpoint()
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
                if not obj.get("navigationUrl"):  # navigationUrl가 없다 = 비공개 프로필(수집 가치 없음)
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

    PRIME_URL = "https://www.linkedin.com/school/bssmhs/people/?viewAsMember=true"

    def _fetch_cookies(self):
        with open(self.login_info_path) as f:
            li_at = json.load(f)["li_at"]

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=self.USER_AGENT)
            page = context.new_page()

            # 1단계: 빈 상태로 홈 방문 → bcookie·bscookie 등 초기 쿠키 확보
            page.goto(self.LINKEDIN_URL, wait_until="domcontentloaded")

            # 2단계: li_at 주입
            context.add_cookies([{
                "name": "li_at",
                "value": li_at,
                "domain": ".linkedin.com",
                "path": "/",
                "httpOnly": True,
                "secure": True,
            }])

            # 3단계: 타겟 페이지 방문 → JSESSIONID 포함 전체 쿠키 확보
            page.goto(self.PRIME_URL, wait_until="domcontentloaded")

            if "/login" in page.url:
                browser.close()
                raise RuntimeError("li_at이 만료되었습니다. login_info.json을 갱신하세요.")

            cookies = context.cookies(self.LINKEDIN_URL)
            browser.close()

        jsessionid = next((c["value"] for c in cookies if c["name"] == "JSESSIONID"), None)
        if not jsessionid:
            raise RuntimeError("JSESSIONID를 받지 못했습니다.")

        self._cookies = {c["name"]: c["value"] for c in cookies}
        self._csrf_token = jsessionid.strip('"')
        self._user_agent = self.USER_AGENT

    def _get_csrf_token(self) -> str:
        if not self._csrf_token:
            raise RuntimeError("CSRF token not loaded. Call _fetch_cookies first.")
        return self._csrf_token


if __name__ == "__main__":
    crawler = LinkedInAlumniCrawler(requester=HttpRequester())
    result = crawler.run(0, 12)
    print(result)
