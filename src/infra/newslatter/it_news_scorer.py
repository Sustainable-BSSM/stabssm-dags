import logging
import re
from typing import Literal
from urllib.parse import urlparse

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.runnables import RunnableLambda
from pydantic import BaseModel, Field

from src.core.newslatter.scorer import NewsScorer as AbstractNewsScorer
from src.infra.llm.openrouter import ChatOpenRouter


def _strip_code_fence(msg) -> str:
    text = msg.content if hasattr(msg, "content") else str(msg)
    return re.sub(r"^```(?:json)?\s*\n?|\n?```\s*$", "", text.strip())

logger = logging.getLogger(__name__)


class ITNewsScore(BaseModel):
    relevance_score: float = Field(description="개발자·학생에게 실질적으로 유익한 IT 정보인 정도 (0~1)")
    publisher_score: float = Field(description="언론사 신뢰도 (0~1)")
    category: Literal["AI", "클라우드", "보안", "개발", "채용", "스타트업", "기타"] = Field(description="기사 카테고리")


_DEFAULTS = {"relevance_score": 0.5, "publisher_score": 0.5, "category": "기타"}

_parser = PydanticOutputParser(pydantic_object=ITNewsScore)

_PROMPT = ChatPromptTemplate.from_messages([
    ("system", "You are a news article evaluator. Respond only with valid JSON matching the required schema. No explanation."),
    ("human", (
        "제목: {title}\n내용: {description}\n출처: {publisher}\n\n"
        "평가 기준:\n"
        "- relevance_score: 개발자·학생에게 유익한 IT 정보인 정도 (채용/트렌드/기술 높게, 단순 홍보 낮게)\n"
        "- publisher_score: 언론사 신뢰도 (주요 IT 매체·일간지 높게, 블로그 낮게)\n"
        "- category: AI/클라우드/보안/개발/채용/스타트업/기타 중 하나\n\n"
        "{format_instructions}"
    )),
])


def _extract_publisher(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return url


class ITNewsScorer(AbstractNewsScorer):

    def __init__(self):
        llm = ChatOpenRouter(model="deepseek/deepseek-chat-v3-0324:floor", temperature=0.0)
        self._chain = _PROMPT | llm | RunnableLambda(_strip_code_fence) | _parser

    async def score(self, article: dict) -> dict:
        publisher = _extract_publisher(article.get("original_link") or article.get("link", ""))
        try:
            result: ITNewsScore = await self._chain.ainvoke({
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "publisher": publisher,
                "format_instructions": _parser.get_format_instructions(),
            })
            return result.model_dump()
        except Exception as e:
            logger.warning(f"[ITNewsScorer] 실패: {e}")
            return _DEFAULTS
