from src.core.llm import LLM
from src.infra.llm.openrouter import OpenRouterLLM


def get_llm(
        template: str = None,
        is_batch: bool = False,
) -> LLM:
    return OpenRouterLLM(template=template)
