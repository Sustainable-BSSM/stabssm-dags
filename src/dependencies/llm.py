from src.core.llm import LLM
from src.infra.llm.openai import OpenAILLM


def get_llm(template: str = None) -> LLM:
    return OpenAILLM(template=template)
