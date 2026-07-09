"""Hybrid retriever: pgvector cosine via a PostgREST RPC (in-DB) + BM25 re-rank (in-app).

Deliberately uses the PostgREST data-plane (API key) rather than a direct Postgres
connection, so the service holds no DDL/write capability — the vector search lives in a
Postgres function authored out-of-band (sql/match_functions.sql). Mirrors the existing
Edge Function's 55/45 hybrid weighting, but the `<=>` cosine ranking + top-k selection
happen inside the database, so only the top candidates cross the wire.
"""

import math
import re
from collections import Counter

import httpx

from .config import get_settings
from .embeddings import get_embeddings
from .models import RetrievedDoc

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOP = {
    "the", "a", "an", "and", "or", "of", "to", "in", "for", "on", "with",
    "is", "are", "was", "were", "by", "at", "as", "that", "this", "it", "be",
}


def _tokenize(text: str) -> list[str]:
    return [t for t in _TOKEN_RE.findall(text.lower()) if len(t) > 2 and t not in _STOP]


def bm25_scores(query: str, docs: list[str], k1: float = 1.5, b: float = 0.75) -> list[float]:
    """Standard Okapi BM25 over a candidate set."""
    q_terms = set(_tokenize(query))
    if not q_terms or not docs:
        return [0.0] * len(docs)

    doc_toks = [_tokenize(d) for d in docs]
    n = len(docs)
    doc_lens = [len(t) or 1 for t in doc_toks]
    avgdl = sum(doc_lens) / max(n, 1)

    df: Counter[str] = Counter()
    for toks in doc_toks:
        for term in set(toks) & q_terms:
            df[term] += 1

    scores = [0.0] * n
    for i, toks in enumerate(doc_toks):
        tf = Counter(toks)
        for term in q_terms:
            if term not in tf:
                continue
            idf = math.log(1 + (n - df[term] + 0.5) / (df[term] + 0.5))
            denom = tf[term] + k1 * (1 - b + b * doc_lens[i] / avgdl)
            scores[i] += idf * (tf[term] * (k1 + 1)) / denom
    return scores


def _min_max(xs: list[float]) -> list[float]:
    if not xs:
        return xs
    lo, hi = min(xs), max(xs)
    if hi - lo < 1e-9:
        return [0.0 for _ in xs]
    return [(x - lo) / (hi - lo) for x in xs]


class HybridRetriever:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client
        self._settings = get_settings()
        self._embeddings = get_embeddings()

    def _resolve(self, schema: str | None, table: str | None) -> tuple[str, str]:
        settings = self._settings
        schema = schema or settings.default_schema
        table = table or settings.default_table
        source = f"{schema}.{table}"
        rpc = settings.source_rpcs.get(source)
        if rpc is None:
            raise ValueError(f"source '{source}' is not in the allowlist")
        return schema, rpc

    async def retrieve(
        self,
        query: str,
        schema: str | None = None,
        table: str | None = None,
        k: int | None = None,
    ) -> list[RetrievedDoc]:
        settings = self._settings
        schema, rpc = self._resolve(schema, table)
        k = k or settings.match_count

        vector = await self._embeddings.aembed_query(query)
        resp = await self._client.post(
            f"{settings.supabase_url}/rest/v1/rpc/{rpc}",
            headers={
                "apikey": settings.supabase_api_key,
                "Authorization": f"Bearer {settings.supabase_api_key}",
                "Content-Type": "application/json",
                "Content-Profile": schema,  # route the RPC to the right schema
                "Accept-Profile": schema,
            },
            json={"query_embedding": vector, "match_count": settings.candidate_pool},
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            return []

        contents = [r["content"] for r in rows]
        cosine = _min_max([float(r.get("similarity", 0.0)) for r in rows])
        lexical = _min_max(bm25_scores(query, contents))
        combined = [
            settings.cosine_weight * c + settings.bm25_weight * lx
            for c, lx in zip(cosine, lexical)
        ]

        docs = [
            RetrievedDoc(
                id=str(r["id"]),
                content=r["content"],
                score=round(score, 4),
                source_filename=r.get("source_filename"),
                section_title=r.get("section_title"),
            )
            for r, score in zip(rows, combined)
        ]
        docs.sort(key=lambda d: d.score, reverse=True)
        return docs[:k]
