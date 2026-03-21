import asyncio
import logging

from src.dependencies.llm import get_llm

logger = logging.getLogger(__name__)

_LINK_COLOR = "#0066cc"

_PROMPT = """당신은 BSSM(부산소프트웨어마이스터고) 뉴스레터 전속 기자 '아리'입니다.
아리는 BSSM 구성원 모두에게 친근하고 신뢰받는 기자로, 어렵고 딱딱한 뉴스를 읽기 쉽고 따뜻하게 전달하는 것이 특기입니다.

아래 뉴스 원문을 아리의 문체로 뉴스레터 기사로 재작성하세요.

원문 제목: {title}
원문 내용: {description}

작성 규칙:
- 독자는 BSSM 학생, 교사, 학부모입니다
- 단순 나열이 아닌 맥락과 의미를 담아 2~3문장으로 작성합니다
- 아리답게 친근하고 명확한 문체를 사용합니다
- 헤드라인은 핵심 정보가 담긴 간결한 문장으로 작성합니다

반드시 아래 형식으로만 응답하세요 (다른 내용 없이):
제목: [헤드라인]
본문: [2~3문장 기사 내용]"""


class ArticleRewriter:

    def __init__(self):
        self._llm = get_llm()

    async def rewrite_all(self, articles: list[dict]) -> list[dict]:
        return await asyncio.gather(*[self._rewrite_one(a) for a in articles])

    async def _rewrite_one(self, article: dict) -> dict:
        title = article.get("title", "")
        description = article.get("description", "")
        link = article.get("original_link", "")

        try:
            result = await self._llm.chat(
                query=_PROMPT,
                variables=[title, description],
            )
            rewritten_title, rewritten_body = _parse(result, title, description)
        except Exception as e:
            logger.warning(f"[ArticleRewriter] LLM 실패, 원문 사용: {e}")
            rewritten_title = title
            rewritten_body = description

        if link:
            rewritten_body += f' <a href="{link}"><font color="{_LINK_COLOR}">원문 보기</font></a>'

        return {"title": rewritten_title, "body": rewritten_body}


def _parse(result: str, fallback_title: str, fallback_body: str) -> tuple[str, str]:
    title = fallback_title
    body = fallback_body
    for line in result.strip().splitlines():
        if line.startswith("제목:"):
            title = line.removeprefix("제목:").strip()
        elif line.startswith("본문:"):
            body = line.removeprefix("본문:").strip()
    return title, body
