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


class SchoolNewsScore(BaseModel):
    relevance_score: float = Field(description="부산소프트웨어마이스터고등학교 관련도 (0~1)")
    publisher_score: float = Field(description="언론사 신뢰도 (0~1)")
    category: Literal["수상", "행사", "입학", "취업", "기타"] = Field(description="기사 카테고리")


_DEFAULTS = {"relevance_score": 0.5, "publisher_score": 0.5, "category": "기타"}

_parser = PydanticOutputParser(pydantic_object=SchoolNewsScore)

_PROMPT = ChatPromptTemplate.from_messages([
    ("system", "You are a news article evaluator. Respond only with valid JSON matching the required schema. No explanation."),
    ("human", (
        "제목: {title}\n내용: {description}\n출처: {publisher}\n\n"
        "relevance_score 채점 기준 (부산소프트웨어마이스터고 관련도):\n"
        "- 0.9~1.0: 기사의 핵심 주제가 본교 자체 (행사, 공식 발표, 입학 등)\n"
        "- 0.7~0.8: 본교 학생의 구체적 성과(수상, 취업·선발, 프로젝트 등)가 기사에서 비중 있게 다뤄짐\n"
        "- 0.3~0.5: 본교 또는 학생이 여러 사례 중 하나로 짧게 언급됨\n"
        "- 0.0~0.2: 본교와 실질적 관련 없음 (일반 IT·교육 기사, 타 학교 위주)\n\n"
        "publisher_score: 언론사 신뢰도 (연합뉴스·주요 일간지 높게, 블로그 낮게)\n"
        "category: 수상/행사/입학/취업/기타 중 하나\n\n"
        "{format_instructions}"
    )),
])


def _extract_publisher(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return url


class NewsScorer(AbstractNewsScorer):

    def __init__(self):
        llm = ChatOpenRouter(model="deepseek/deepseek-chat-v3-0324:floor", temperature=0.0)
        self._chain = _PROMPT | llm | RunnableLambda(_strip_code_fence) | _parser

    async def score(self, article: dict) -> dict:
        publisher = _extract_publisher(article.get("original_link") or article.get("link", ""))
        try:
            result: SchoolNewsScore = await self._chain.ainvoke({
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "publisher": publisher,
                "format_instructions": _parser.get_format_instructions(),
            })
            return result.model_dump()
        except Exception as e:
            logger.warning(f"[NewsScorer] 실패: {e}")
            return _DEFAULTS
