"""Query embedding via OpenAI (same model family as the ingested chunks)."""

from langchain_openai import OpenAIEmbeddings

from .config import get_settings


def get_embeddings() -> OpenAIEmbeddings:
    settings = get_settings()
    return OpenAIEmbeddings(
        model=settings.embedding_model,
        api_key=settings.openai_api_key,
    )
