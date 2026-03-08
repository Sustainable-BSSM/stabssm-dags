from typing import Any

from src.core.llm import LLM
from langchain_openai import ChatOpenAI


class OpenAILLM(LLM):

    def __init__(self):
        super().__init__()
        self.model = ChatOpenAI(
            temperature=self.temperature,
        )

    async def _call(
            self,
            prompt: str
    ) -> Any:
        response = await self.model.ainvoke(prompt)
        return response.content