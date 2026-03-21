import logging
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from src.infra.llm.openrouter import ChatOpenRouter
from src.core.llm import LLM

logger = logging.getLogger(__name__)

_ARI_SYSTEM = """당신은 BSSM 뉴스레터 전속 기자 '아리'야.

말투 규칙:
- 반말 사용 (~했어, ~야, ~이야)
- 짧은 문장, 한 문장에 한 정보
- 문장 구조: 관찰 → 설명 → 의미 → (가끔) 말걸기
- 용어는 유지하되 짧게 풀어 설명
- "정리해보면,", "핵심은 이거야.", "지금 흐름은 이거야." 같은 정리 문장 활용
- 말걸기는 가끔만 ("이거 해봤지?", "한 번 막혔었지?")
- 공감 한 줄 추가 가능 ("처음엔 좀 어렵게 느껴질 수 있어.")
- 금지: 과한 귀여움(~냥), 유튜버 톤(대박, 레전드), 과도한 격식(~습니다), 용어 유치하게 바꾸기"""


class AriLLM(LLM):

    def __init__(
            self,
            model: str = "deepseek/deepseek-chat-v3-0324",
            temperature: float = 1.0,
    ):
        super().__init__(temperature=temperature)
        self.model = ChatOpenRouter(model=model, temperature=temperature)

    async def _call(self, prompt: str) -> Any:
        messages = [
            SystemMessage(content=_ARI_SYSTEM),
            HumanMessage(content=prompt),
        ]
        try:
            response = await self.model.ainvoke(messages)
            return response.content
        except Exception as e:
            logger.error(f"[AriLLM] failed. prompt length={len(prompt)}\nerror={e}")
            raise
