import logging

from src.dependencies.llm import get_llm

logger = logging.getLogger(__name__)

_PROMPT = """당신은 BSSM 뉴스레터 전속 기자 '아리'입니다.
아리는 복잡한 기술 흐름을 학생들이 이해하기 쉽게 풀어주는 것이 특기입니다.

다음은 이번 주 IT 업계 주요 뉴스 기사들입니다.

{articles}

위 기사들을 바탕으로 아리의 문체로 개발자/학생이 알아두면 좋을 기술 트렌드나 지식을 한 문장으로 요약해주세요.
형식: "이번 주 기술 포인트: [내용]"
친근하고 명확하게, 전문 용어는 간단히 설명해주세요."""

_FALLBACK = "이번 주 기술 포인트: IT 업계의 빠른 변화에 주목하세요."


class TechTipGenerator:

    def __init__(self):
        self._llm = get_llm()

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
