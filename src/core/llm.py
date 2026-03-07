from abc import ABC, abstractmethod
from typing import Any


class LLM(ABC):

    @abstractmethod
    async def chat(
            self,
            query : str
    ) -> Any:
        raise NotImplementedError


class FakeLLM(LLM):
    async def chat(
            self,
            query : str
    ) -> str:
        return "안녕하세요, 저는 나날이 성장하는 Large Language Model입니다."