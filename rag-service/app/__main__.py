"""CLI entry point for lightweight, zero-idle-RAM invocation.

The MCP server spawns this per call (`python -m app`), writes a JSON payload to
stdin, reads the JSON result from stdout, and the process exits — so RAM is used
only while a query is actually running. Same graph/core as the FastAPI app.

Input  (stdin): {"question": "...", "table": "...", "match_count": 5}
                (or a bare question string, or a single argv question)
Output (stdout): {"answer": "...", "documents": [...], "grounded": true, ...}
"""

import asyncio
import json
import sys

import httpx

from .graph import build_graph
from .nodes import Nodes
from .retriever import HybridRetriever


async def _run(payload: dict) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        graph = build_graph(Nodes(HybridRetriever(client)))
        state = {
            "question": payload["question"],
            "original_question": payload["question"],
            "schema": payload.get("schema"),
            "table": payload.get("table"),
            "match_count": payload.get("match_count"),
            "retrieval_passes": 0,
            "generation_passes": 0,
        }
        final = await graph.ainvoke(state)
        return {
            "answer": final.get("answer", ""),
            "documents": [d.model_dump() for d in final.get("documents", [])],
            "retrieval_passes": final.get("retrieval_passes", 0),
            "generation_passes": final.get("generation_passes", 0),
            "grounded": final.get("grounded", False),
            "addresses_question": final.get("addresses_question", False),
            "grade_log": [e.model_dump() for e in final.get("grade_log", [])],
        }


def _read_payload() -> dict:
    raw = sys.stdin.read().strip() if not sys.stdin.isatty() else ""
    if raw:
        try:
            data = json.loads(raw)
            return data if isinstance(data, dict) else {"question": str(data)}
        except json.JSONDecodeError:
            return {"question": raw}  # treat plain stdin as the question
    if len(sys.argv) > 1:
        return {"question": sys.argv[1]}
    return {}


def main() -> None:
    payload = _read_payload()
    if not payload.get("question"):
        print(json.dumps({"error": "missing 'question' (stdin JSON or argv)"}))
        sys.exit(2)
    try:
        result = asyncio.run(_run(payload))
    except Exception as exc:  # emit a structured error the caller can parse
        print(json.dumps({"error": str(exc)}))
        sys.exit(1)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
