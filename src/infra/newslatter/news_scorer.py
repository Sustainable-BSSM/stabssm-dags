import json
import logging
import re
from urllib.parse import urlparse

from src.dependencies.llm import get_llm

logger = logging.getLogger(__name__)

_PROMPT = """다음 뉴스 기사를 평가해주세요.

제목: {title}
내용: {description}
출처: {publisher}

아래 JSON 형식으로만 응답하세요:
{{"relevance_score": 0.9, "publisher_score": 0.8, "category": "수상"}}

평가 기준:
- relevance_score (0~1): 부산소프트웨어마이스터고등학교가 기사의 핵심 주제인 정도 (단순 언급이면 낮게)
- publisher_score (0~1): 언론사 신뢰도 (연합뉴스·주요 일간지는 높게, 블로그·소규모 매체는 낮게)
- category: 수상/행사/입학/취업/기타 중 하나"""


def _extract_publisher(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return url


def _parse_response(response: str) -> dict:
    try:
        match = re.search(r'\{.*\}', response, re.DOTALL)
        if match:
            return json.loads(match.group())
    except Exception:
        pass
    return {"relevance_score": 0.5, "publisher_score": 0.5, "category": "기타"}


class NewsScorer:

    def __init__(self):
        self._llm = get_llm()

    async def score(self, article: dict) -> dict:
        publisher = _extract_publisher(article.get("original_link") or article.get("link", ""))
        try:
            response = await self._llm.chat(
                query=_PROMPT,
                variables=[
                    article.get("title", ""),
                    article.get("description", ""),
                    publisher,
                ],
            )
            return _parse_response(response)
        except Exception as e:
            logger.warning(f"[NewsScorer] LLM 실패: {e}")
            return {"relevance_score": 0.5, "publisher_score": 0.5, "category": "기타"}
