# MCP Server

Production agent backend enabling LLMs to query databases, execute workflows, automate email/calendar, and make decisions using RAG + evaluation scoring.

Built with Node.js/Express, backed by Supabase (PostgreSQL + pgvector), exposing 20+ tools over HTTP via the Model Context Protocol (MCP). Runs as a systemd service with CI/CD deployment and health monitoring.

## Architecture

```
LLM Client (Claude/GPT)
    ↓ JSON-RPC over HTTPS
MCP Server (Express, port 3000)
    ↓
┌───────────────┬──────────────┬─────────────────┐
│  Supabase     │  External    │  Browser        │
│  PostgreSQL   │  APIs        │  Automation     │
│  + pgvector   │  Gmail,      │  Playwright     │
│  6 schemas    │  Pushover    │  multi-step     │
└───────────────┴──────────────┴─────────────────┘
```

## MCP Tools

**CRUD** — `query_table`, `insert_data`, `update_data`, `delete_data`, `upsert_data`

**Schema Discovery** — `list_schemas`, `list_tables`, `list_columns`, `list_functions`, `list_views`, `list_triggers`, `list_rpcs`

**HTTP** — `http_fetch` with rate limiting, retries, exponential backoff, host allowlist

**Browser** — `browser_flow` (Playwright multi-step automation), screenshot, PDF, text extraction

**Email** — `send_email` via Gmail OAuth2 (draft/send modes, attachments, open tracking, follow-up events)

**Push Notifications** — `push_notify` via Pushover with priority categories

**Agentic RAG** — `agentic_rag_query`: self-correcting RAG answer over the professional-profile corpus, served by a Python/FastAPI/LangGraph companion service (see below)

## RAG Pipeline

Hybrid semantic search across multiple domains:

- **Embeddings**: OpenAI `text-embedding-3-large` (3072-dim)
- **Scoring**: 55% cosine similarity + 45% BM25 lexical matching
- **Boosts**: phrase match (15%), domain keywords (10%), section title (8%), schema tags (8%)
- **Reranking**: LLM-based relevance scoring via `gpt-4.1-mini` with JSON mode
- **Diversity**: max 60% results from single source
- **Gmail-specific**: recency decay (90-day half-life), thread expansion

Implemented as Supabase Edge Functions (Deno/TypeScript):
- `semantic-search` — general-purpose hybrid search
- `gmail-semantic-search` — email search with recency weighting
- `embed-chunk` — webhook-triggered embedding generation
- `screenplay-semantic-search` — screenplay element search
- `track_email_open` — pixel-based open tracking

## Agentic RAG Service (Python · FastAPI · LangGraph)

A companion microservice ([`rag-service/`](rag-service/)) that adds a **self-correcting** retrieval loop on top of the RAG pipeline above. Where the Edge Functions return ranked passages, this service returns a *verified answer*: it retrieves, grades the retrieved documents for relevance, generates, then checks the answer for groundedness and whether it addresses the question — rewriting the query or regenerating on failure, with bounded retries.

- **Framework**: LangGraph state machine (retrieve → grade documents → generate → grade generation, with conditional edges and retry ceilings)
- **API**: FastAPI with streaming (SSE) plus a per-call CLI entry point; called by the MCP server as the `agentic_rag_query` tool
- **Retrieval**: pgvector cosine top-k via a PostgREST RPC (in-DB), BM25 lexical re-rank in app (55/45) — no direct DB connection, so the service holds no DDL or write capability
- **Models**: Claude (generation + grading), OpenAI `text-embedding-3-large` (query embedding)
- **Quality**: pytest + ruff + mypy in CI, plus a groundedness/recall eval harness over the real corpus

## Database Schemas

| Schema | Purpose |
|--------|---------|
| `gmail` | Email ingestion, thread tracking, open tracking, semantic search |
| `calendar` | Events, recurring events, three notification types |
| `health` | Weight, meals, workouts, recipes, food inventory, daily snapshots |
| `finance` | Purchase tracking, recurring charges |
| `professional_profile` | Work experience, education, publications, consulting — 400+ RAG chunks |
| `genealogy` | Ancestry documents with embeddings |

## Automation Modules

Each module has its own cron scripts running via system crontab with `flock` locking:

**Gmail** — Ingest emails every 3 min, consolidate threads, push notifications for unread messages. OAuth2 with automatic token refresh.

**Calendar** — Day-of, before-event, and at-start push notifications. Handles both one-off and recurring events with instance generation.

**WG-Gesucht** — Apartment listing scraper with LLM-based evaluation (0–100 scoring rubric via `gpt-4o-mini`, JSON mode), auto-generated German inquiry messages with style variation.

**Poshmark** — Playwright-based closet sharing and sales notification monitoring.

**Facebook** — Marketplace listing ingestion and notifications.

**Embeddings** — Cron-based embedding generation for professional profile and genealogy documents with quota management and retry logic.

## LLM Integration Patterns

- **Structured outputs**: `response_format: { type: "json_object" }` across search, eval, scoring
- **Prompt engineering**: detailed evaluation rubrics, scoring guides, style variation, phrase prohibitions
- **Cost optimization**: quota detection with cooldown, inter-call delays, batch limits, input truncation
- **Idempotency**: content hashing (`md5`) prevents duplicate embedding generation

## Security

- Trust token authentication (`X-MCP-Trust` header) for write operations
- Read-only endpoint (`/sse-readonly`) with schema-scoped access
- URL token authentication for cron/service access
- Direct localhost bypass for internal cron calls
- Systemd hardening: `ProtectSystem=full`, `NoNewPrivileges`, `PrivateTmp`
- Host allowlist for outbound HTTP fetch
- Rate limiting per endpoint

## Stack

- **Runtime**: Node.js, Express
- **Database**: Supabase (PostgreSQL + pgvector)
- **Edge Functions**: Deno/TypeScript
- **Browser Automation**: Playwright + Chromium
- **Email**: Gmail API via googleapis + nodemailer
- **Push Notifications**: Pushover
- **Embeddings**: OpenAI `text-embedding-3-large`
- **Reranking**: OpenAI `gpt-4.1-mini`
- **Evaluation**: OpenAI `gpt-4o-mini`
- **Process Management**: systemd
- **Logging**: pino

### Companion service — [`rag-service/`](rag-service/)

Python · FastAPI · LangGraph · LangChain · Pydantic · pgvector (via PostgREST RPC) · Docker · pytest / ruff / mypy
