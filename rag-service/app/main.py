"""FastAPI app: streaming + sync agentic-RAG endpoints, token auth, PostgREST-backed."""

import json
from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse

from .config import get_settings
from .graph import build_graph
from .models import QueryRequest, QueryResponse
from .nodes import GraphState, Nodes
from .retriever import HybridRetriever


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.client = None
    app.state.graph = None
    app.state.startup_error = None
    try:
        if not settings.supabase_url:
            raise RuntimeError("SUPABASE_URL not configured")
        app.state.client = httpx.AsyncClient(timeout=30.0)
        app.state.graph = build_graph(Nodes(HybridRetriever(app.state.client)))
    except Exception as exc:  # health stays up even if config is incomplete
        app.state.startup_error = str(exc)
    try:
        yield
    finally:
        if app.state.client is not None:
            await app.state.client.aclose()


app = FastAPI(title="Agentic RAG Service", version="0.1.0", lifespan=lifespan)


async def require_token(authorization: str | None = Header(default=None)) -> None:
    settings = get_settings()
    if not settings.trust_token:
        return  # auth disabled (local dev)
    presented = (authorization or "").removeprefix("Bearer ").strip()
    if presented != settings.trust_token:
        raise HTTPException(status_code=401, detail="invalid or missing token")


def _initial_state(req: QueryRequest) -> GraphState:
    return {
        "question": req.question,
        "original_question": req.question,
        "schema": req.schema_name,
        "table": req.table,
        "match_count": req.match_count,
        "retrieval_passes": 0,
        "generation_passes": 0,
    }


def _require_graph(app: FastAPI):
    if app.state.graph is None:
        raise HTTPException(status_code=503, detail=f"graph unavailable: {app.state.startup_error}")
    return app.state.graph


@app.get("/health")
async def health() -> dict[str, object]:
    return {"status": "ok", "graph_ready": app.state.graph is not None}


@app.post("/query/sync", response_model=QueryResponse, dependencies=[Depends(require_token)])
async def query_sync(req: QueryRequest) -> QueryResponse:
    graph = _require_graph(app)
    final: GraphState = await graph.ainvoke(_initial_state(req))
    return QueryResponse(
        answer=final.get("answer", ""),
        documents=final.get("documents", []),
        retrieval_passes=final.get("retrieval_passes", 0),
        generation_passes=final.get("generation_passes", 0),
        grounded=final.get("grounded", False),
        addresses_question=final.get("addresses_question", False),
        grade_log=final.get("grade_log", []),
    )


@app.post("/query", dependencies=[Depends(require_token)])
async def query_stream(req: QueryRequest) -> StreamingResponse:
    graph = _require_graph(app)

    async def event_stream():
        async for update in graph.astream(_initial_state(req)):
            for node_name, node_state in update.items():
                payload: dict[str, object] = {"node": node_name}
                if "documents" in node_state:
                    payload["documents"] = len(node_state["documents"])
                if "answer" in node_state:
                    payload["answer"] = node_state["answer"]
                if "grounded" in node_state:
                    payload["grounded"] = node_state["grounded"]
                    payload["addresses_question"] = node_state.get("addresses_question")
                if "grade_log" in node_state:
                    # This node's grading verdicts + reasoning (the reducer delta,
                    # i.e. just this pass's events, not the accumulated log).
                    payload["grades"] = [e.model_dump() for e in node_state["grade_log"]]
                yield f"data: {json.dumps(payload)}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
