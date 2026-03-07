from src.core.llm import LLM, FakeLLM


def get_llm(template: str = None) -> LLM:
    return FakeLLM(template=template)
