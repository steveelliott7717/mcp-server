"""Application settings, loaded from environment / .env via pydantic-settings."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- Supabase (PostgREST) — data-plane only, no direct DB connection ---
    supabase_url: str = Field(default="", description="https://<project-ref>.supabase.co")
    supabase_api_key: str = ""

    # --- Model providers ---
    openai_api_key: str = ""
    anthropic_api_key: str = ""

    # --- Models (env-overridable; defaults are current provider IDs) ---
    embedding_model: str = "text-embedding-3-large"
    generation_model: str = "claude-sonnet-5"
    grading_model: str = "claude-haiku-4-5-20251001"

    # --- Retrieval defaults ---
    default_schema: str = "professional_profile"
    default_table: str = "work_experience_chunks"
    candidate_pool: int = 25  # vector candidates the RPC returns before lexical re-rank
    match_count: int = 5  # docs handed to the generator
    cosine_weight: float = 0.55  # mirrors the existing Edge Function's hybrid weighting
    bm25_weight: float = 0.45

    # --- Agentic loop bounds (prevent infinite retry) ---
    max_retrieval_retries: int = 2
    max_generation_retries: int = 2

    # --- Allowlist: "schema.table" -> PostgREST RPC that vector-searches it. ---
    # Only these sources are queryable. The functions are authored as DDL by the
    # operator (see sql/match_functions.sql); the service never runs DDL itself.
    source_rpcs: dict[str, str] = {
        "professional_profile.work_experience_chunks": "match_work_experience_chunks",
    }

    # --- Auth: bearer token the MCP server presents (blank disables auth) ---
    trust_token: str = ""

    # --- Server ---
    host: str = "127.0.0.1"
    port: int = 8000


@lru_cache
def get_settings() -> Settings:
    return Settings()
