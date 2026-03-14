import re
from abc import ABC, abstractmethod
from typing import Any, List

class LLM(ABC):

    def __init__(
            self,
            template: str = None,
            temperature: float = 1.0,
            max_tokens: int = None,
    ) -> None:
        self.template = template
        self.temperature = temperature
        self.max_tokens = max_tokens

    def _inject(self, query: str, variables: List) -> str:
        names = re.findall(r'\{([^{}]+)\}', query)
        return query.format(**dict(zip(names, variables)))

    async def chat(
            self,
            query: str,
            variables: List = None,
    ) -> Any:
        prompt = self._inject(query, variables) if variables else query
        return await self._call(prompt)

    @abstractmethod
    async def _call(self, prompt: str | List[str]) -> Any:
        raise NotImplementedError

class FakeLLM(LLM):
    async def _call(self, prompt: str) -> str:
        return prompt

class BatchLLM(LLM):
    async def _call(self, prompt: str) -> str:
        ...
