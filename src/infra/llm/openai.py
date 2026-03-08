from typing import Any

from langchain_openai import ChatOpenAI
from pydantic import SecretStr

from src.common.config.openai import OpenAIConfig
from src.core.llm import LLM


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
        response = await self.model.ainvoke(prompt)
        return response.content
