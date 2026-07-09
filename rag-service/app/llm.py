"""Chat model factories. Generation uses a capable model; grading uses a fast/cheap one."""

from langchain_anthropic import ChatAnthropic

from .config import get_settings


def get_generation_llm() -> ChatAnthropic:
    settings = get_settings()
    # Note: newer Claude models deprecate the `temperature` parameter, so it is omitted.
    return ChatAnthropic(
        model=settings.generation_model,
        api_key=settings.anthropic_api_key,
        max_tokens=1024,
    )


def get_grading_llm() -> ChatAnthropic:
    settings = get_settings()
    # Grading runs many small calls; use the faster/cheaper model.
    return ChatAnthropic(
        model=settings.grading_model,
        api_key=settings.anthropic_api_key,
        max_tokens=512,
    )
