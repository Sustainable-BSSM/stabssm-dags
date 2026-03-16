import logging
from typing import Any, List

from langchain_openai import ChatOpenAI
from pydantic import SecretStr

from src.common.config.openai import OpenAIConfig
from src.core.llm import LLM
from typing_extensions import Protocol

logger = logging.getLogger(__name__)

class OpenAI(Protocol):
    ...

class OpenAILLM(LLM):

    def __init__(
            self,
            template: str = None,
            temperature: float = 1.0,
            max_tokens: int = None,
    ):
        super().__init__(
            template=template,
            temperature=temperature,
            max_tokens=max_tokens
        )

        self.model = ChatOpenAI(
            model="gpt-5",
            temperature=self.temperature,
            api_key=SecretStr(OpenAIConfig.API_KEY),
        )

    async def _call(
            self,
            prompt: str
    ) -> Any:
        try:
            response = await self.model.ainvoke(prompt)
            return response.content
        except Exception as e:
            logger.error(f"[OpenAILLM] failed. prompt length={len(prompt)}\n{prompt}\nerror={e}")
            raise

class BatchOpenAI(LLM):

    def __init__(
            self,
            template: str = None,
            temperature: float = 1.0,
            max_tokens: int = None,
    ):
        super().__init__(template=template, temperature=temperature, max_tokens=max_tokens)
        self.model = ChatOpenAI(
            model="gpt-4o",
            temperature=self.temperature,
            api_key=SecretStr(OpenAIConfig.API_KEY),
        )

    async def _call(self, prompt: str) -> Any:
        response = await self.model.ainvoke(prompt)
        return response.content

    async def _batch(self, prompts: List[str]) -> List[Any]:
        responses = await self.model.abatch(prompts)
        return [r.content for r in responses]

