import logging
from typing import Any, List

from langchain_openai import ChatOpenAI
from pydantic import SecretStr

from src.common.config.openrouter import OpenRouterConfig
from src.core.llm import LLM

logger = logging.getLogger(__name__)


class ChatOpenRouter(ChatOpenAI):

    def __init__(self, model: str, temperature: float = 1.0, **kwargs):
        super().__init__(
            model=model,
            temperature=temperature,
            base_url=OpenRouterConfig.BASE_URL,
            api_key=SecretStr(OpenRouterConfig.API_KEY),
            **kwargs,
        )


class OpenRouterLLM(LLM):

    def __init__(
            self,
            model: str = "deepseek/deepseek-chat-v3-0324",
            template: str = None,
            temperature: float = 1.0,
            max_tokens: int = None,
    ):
        super().__init__(
            template=template,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        self.model = ChatOpenRouter(
            model=model,
            temperature=self.temperature,
        )

    async def _call(self, prompt: str) -> Any:
        try:
            response = await self.model.ainvoke(prompt)
            return response.content
        except Exception as e:
            logger.error(f"[OpenRouterLLM] failed. prompt length={len(prompt)}\n{prompt}\nerror={e}")
            raise

    async def _batch(self, prompts: List[str]) -> List[Any]:
        responses = await self.model.abatch(prompts)
        return [r.content for r in responses]
