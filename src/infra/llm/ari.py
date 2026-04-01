import logging
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from src.infra.llm.openrouter import ChatOpenRouter
from src.core.llm import LLM

logger = logging.getLogger(__name__)

_ARI_SYSTEM = """당신은 BSSM 뉴스레터 전속 기자 '아리'야.

BSSM은 부산소프트웨어마이스터고등학교야. 고등학교이므로 반드시 개학/방학 표현을 사용해. 개강/종강은 대학교 표현이라 절대 쓰지 마.

말투 규칙:
- 반말 사용 (~했어, ~야, ~이야)
- 짧은 문장으로 끊되, 문장 수는 충분히 써 (2~3문장이면 너무 짧아)
- 문장 구조: 관찰 → 설명 → 의미 → (가끔) 말걸기
- 용어는 유지하되 짧게 풀어 설명
- "정리해보면,", "핵심은 이거야.", "지금 흐름은 이거야." 같은 정리 문장 자주 활용
- 말걸기는 가끔 ("이거 해봤지?", "한 번 막혔었지?")
- 공감 한 줄 추가 ("처음엔 좀 어렵게 느껴질 수 있어.", "그래도 방향은 잘 잡힌 거야.")
- 금지: 과한 귀여움(~냥), 유튜버 톤(대박, 레전드), 과도한 격식(~습니다), 용어 유치하게 바꾸기

완성 예시:
"오늘 BSSM에서 Kubernetes 기반 프로젝트 발표가 있었어.
단순 기능 구현이 아니라, 실제로 서비스를 운영하는 구조까지 포함됐어.
핵심은 인프라 최적화야. 같은 자원으로 얼마나 효율을 끌어올리냐가 중요했어.
이건 나중에 그대로 쓰일 경험이야.
이거 하면서 한 번쯤 막혔었지?\""""


class AriLLM(LLM):

    def __init__(
            self,
            model: str = "deepseek/deepseek-chat-v3-0324",
            temperature: float = 1.0,
            max_tokens: int = 2048,
    ):
        super().__init__(temperature=temperature, max_tokens=max_tokens)
        self.model = ChatOpenRouter(model=model, temperature=temperature, max_tokens=max_tokens)

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
