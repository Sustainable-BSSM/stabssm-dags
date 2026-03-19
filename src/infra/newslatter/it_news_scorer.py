import json
import logging
import re
from urllib.parse import urlparse

from src.core.newslatter.scorer import NewsScorer as AbstractNewsScorer
from src.dependencies.llm import get_llm

logger = logging.getLogger(__name__)

_PROMPT = """다음 IT 뉴스 기사를 평가해주세요.

제목: {title}
내용: {description}
출처: {publisher}

아래 JSON 형식으로만 응답하세요:
{{"relevance_score": 0.9, "publisher_score": 0.8, "category": "AI"}}

평가 기준:
- relevance_score (0~1): 개발자·학생에게 실질적으로 유익한 IT 정보인 정도 (채용/트렌드/기술은 높게, 단순 홍보는 낮게)
- publisher_score (0~1): 언론사 신뢰도 (주요 IT 매체·일간지는 높게, 블로그·소규모 매체는 낮게)
- category: AI/클라우드/보안/개발/채용/스타트업/기타 중 하나"""


def _extract_publisher(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return url


def _parse_response(response: str) -> dict:
    defaults = {"relevance_score": 0.5, "publisher_score": 0.5, "category": "기타"}
    try:
        match = re.search(r'\{.*\}', response, re.DOTALL)
        if match:
            parsed = json.loads(match.group())
            logger.debug(f"[ITNewsScorer] parsed: {parsed}")
            return {**defaults, **parsed}
    except Exception as e:
        logger.warning(f"[ITNewsScorer] parse failed: {e} | raw={response!r}")
    return defaults


class ITNewsScorer(AbstractNewsScorer):

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
            logger.warning(f"[ITNewsScorer] LLM 실패: {e}")
            return {"relevance_score": 0.5, "publisher_score": 0.5, "category": "기타"}
