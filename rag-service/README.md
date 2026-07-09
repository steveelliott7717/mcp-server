# rag-service — Agentic RAG microservice (FastAPI + LangGraph)

A Python microservice that answers questions over the existing Supabase/pgvector
corpus with a **self-correcting** retrieval loop, and exposes it over HTTP so the
Node MCP server can call it as a tool.

It is deliberately **not** a replacement for the existing `semantic-search` Edge
Function. That function is strong *retrieval* (hybrid cosine + BM25 + LLM rerank) and
returns ranked passages. This service *reuses that idea as one node* and adds the two
things the current system doesn't have: **answer generation** and **answer
verification** (groundedness + on-topic checks with bounded retries).

## Data access: PostgREST only, no direct connection

The service reaches the database through the **PostgREST data-plane** (API key), not a
direct Postgres connection. That is a deliberate capability boundary: the service can
*call* the vector-search functions but holds **no DDL or write capability** — it cannot
create, alter, or drop anything, and the only SQL it can invoke is the fixed `match_*`
functions you author out-of-band. The agent driving the MCP server, in turn, only ever
reaches a `question -> answer` tool, never raw SQL. Containment is enforced by the shape
of the interface, not by trusting the model.

The `<=>` cosine ranking and top-k selection still happen **inside Postgres** (in the
`match_*` functions), so only the top candidates cross the wire — unlike the Edge
Function, which fetches all rows and scores cosine in application code.

## Architecture

```
   ┌──────────────────────┐   HTTP (SSE / JSON)   ┌──────────────────────────┐   PostgREST RPC    ┌──────────────────┐
   │  Node MCP server      │ ───────────────────▶ │  rag-service (FastAPI)    │ ─────────────────▶ │  Supabase        │
   │  tool: agentic_rag    │  POST /query          │   LangGraph agentic loop  │  /rest/v1/rpc/     │  match_* fn(s)   │
   │  (question in only)   │  POST /query/sync     │                          │  match_<table>     │  ORDER BY <=>    │
   └──────────────────────┘                       └──────────────────────────┘   (API key)        └──────────────────┘
```

## The agentic loop

```
retrieve ──▶ grade_documents ──▶ enough relevant docs?
                                   │  no  ──▶ transform_query ──▶ retrieve   (≤ MAX_RETRIEVAL_RETRIES)
                                   │  yes ──▶ generate ──▶ grade_generation ──▶ grounded & on-topic?
                                                                                 │ not grounded  ──▶ generate  (≤ MAX_GENERATION_RETRIES)
                                                                                 │ off-topic     ──▶ transform_query
                                                                                 │ grounded & on ──▶ END
```

- **retrieve** — calls the `match_*` PostgREST RPC (in-DB pgvector cosine top-k), then
  BM25 lexical re-rank in app, combined `0.55·cosine + 0.45·bm25` (mirrors the existing
  Edge Function's weighting). Query embedding uses `text-embedding-3-large`, the model
  the chunks were embedded with.
- **grade_documents** — an LLM grades each retrieved doc for relevance; irrelevant docs
  are dropped. If none survive, the query is rewritten and retrieval retried.
- **generate** — answers using only the retrieved context.
- **grade_generation** — an LLM checks the answer is *grounded* and *addresses the
  question*; ungrounded answers regenerate, off-topic ones re-retrieve — both bounded.

## Setup

1. **Author the vector-search functions** (DDL you run yourself — the service never does):
   ```bash
   psql "$YOUR_ADMIN_CONNECTION" -f sql/match_functions.sql
   # or paste sql/match_functions.sql into the Supabase SQL editor
   ```
   Ensure `professional_profile` is in Supabase → Settings → API → Exposed schemas.

2. **Configure and run:**
   ```bash
   cd rag-service
   cp .env.example .env          # fill SUPABASE_URL + SUPABASE_API_KEY + provider keys
   pip install -e ".[dev]"
   uvicorn app.main:app --reload # http://127.0.0.1:8000
   ```
   Or containerised: `docker compose up --build`.

### Endpoints

| Method | Path          | Purpose                                             |
|--------|---------------|-----------------------------------------------------|
| GET    | `/health`     | Liveness + whether the graph initialised            |
| POST   | `/query`      | Streams graph progress (SSE), then the final answer |
| POST   | `/query/sync` | Runs the graph and returns the full result as JSON  |

```bash
curl -N http://127.0.0.1:8000/query \
  -H "Authorization: Bearer $TRUST_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"question": "What fraud did Steve detect at Grainger?"}'
```

## Evaluation

`tests/test_eval.py` runs ground-truth questions through the full graph and asserts the
answers are graded grounded and recall the known facts. It runs locally (with config +
keys) and is skipped automatically in CI. The routing logic (`tests/test_decisions.py`)
and API surface (`tests/test_api.py`) run in CI with no secrets.

## Notes

- **Security / containment** — PostgREST-only (no DDL/writes), an allowlist of
  `schema.table -> match_*` sources, localhost bind, and a bearer-token gate. The MCP
  tool exposed to the agent must accept only `{question, table-from-allowlist,
  match_count}` — never freeform SQL — so the agent's reachable capability stays
  "retrieve and answer," nothing more.
- **Models are configurable** via env (`GENERATION_MODEL`, `GRADING_MODEL`,
  `EMBEDDING_MODEL`); defaults target current provider IDs. Verify exact model strings
  against your provider before deploying.
- **Why LangGraph here but not in the main MCP server?** The main server is driven by a
  capable model with a human in the loop, where open-ended reasoning beats a fixed
  graph. This service is the opposite context — an unsupervised, callable endpoint that
  benefits from a bounded, testable, regression-checkable control flow. The scaffolding
  earns its cost precisely because there's no human watching each answer.
```
