"""Groundedness + recall eval harness over the real corpus.

The honest version of "quality gates": runs ground-truth questions through the full
agentic graph and asserts the answers are graded grounded and recall the known facts.
Skipped automatically when secrets/config are absent (e.g. in CI).
"""

import os

import pytest

_HAVE_ENV = all(
    os.getenv(k) for k in ("SUPABASE_URL", "SUPABASE_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY")
)
pytestmark = pytest.mark.skipif(
    not _HAVE_ENV,
    reason="requires SUPABASE_URL + SUPABASE_API_KEY + ANTHROPIC_API_KEY + OPENAI_API_KEY",
)

GROUND_TRUTH = [
    {
        "question": "What fraud case did Steve flag at Grainger, and at what dollar value?",
        "must_include": ["1.7", "fraud"],
    },
    {
        "question": "How many SKU-site combinations did the KeepStock pipeline process weekly?",
        "must_include": ["100k", "sku"],
    },
]


@pytest.mark.asyncio
async def test_groundedness_and_recall():
    import httpx

    from app.graph import build_graph
    from app.nodes import Nodes
    from app.retriever import HybridRetriever

    async with httpx.AsyncClient(timeout=30.0) as client:
        graph = build_graph(Nodes(HybridRetriever(client)))
        for case in GROUND_TRUTH:
            state = {
                "question": case["question"],
                "original_question": case["question"],
                "retrieval_passes": 0,
                "generation_passes": 0,
            }
            result = await graph.ainvoke(state)
            answer = result.get("answer", "").lower()
            assert result.get("grounded") is True, f"ungrounded answer: {case['question']}"
            for token in case["must_include"]:
                assert token.lower() in answer, f"missing '{token}' for: {case['question']}"
