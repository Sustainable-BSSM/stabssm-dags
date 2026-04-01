import logging

from src.dependencies.llm import get_ari_llm

logger = logging.getLogger(__name__)

_PROMPT = """다음은 이번 주 IT 업계 주요 뉴스 기사들이야.

{articles}

위 기사들을 바탕으로 개발자/학생이 알아두면 좋을 기술 트렌드를 한 문장으로 정리해줘.
형식: "이번 주 기술 포인트: [내용]"
전문 용어는 짧게 풀어서."""

_FALLBACK = "이번 주 기술 포인트: IT 업계의 빠른 변화에 주목해."


class TechTipGenerator:

    def __init__(self):
        self._llm = get_ari_llm()

    async def generate(self, articles: list[dict]) -> str:
        if not articles:
            return _FALLBACK

        article_texts = "\n".join([
            f"- {a.get('title', '')} ({a.get('description', '')[:100]})"
            for a in articles[:5]
        ])

        try:
            return await self._llm.chat(query=_PROMPT, variables=[article_texts])
        except Exception as e:
            logger.warning(f"[TechTipGenerator] LLM 실패: {e}")
            return _FALLBACK
