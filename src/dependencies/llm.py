from typing import cast
from src.core.llm import LLM
from src.infra.llm.openai import OpenAILLM, OpenAI, BatchOpenAI


def get_llm(
        template: str = None,
        is_batch: bool = False,
) -> LLM:
    return cast(
        LLM,
        get_openai_llm(template=template, is_batch=is_batch)
    )


def get_openai_llm(
        template: str = None,
        is_batch: bool = False,
) -> OpenAI:
    if is_batch:
        return BatchOpenAI(template=template)

    return OpenAILLM(template=template)
