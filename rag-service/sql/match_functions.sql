-- Vector-search RPCs for the agentic RAG service.
--
-- Run this yourself (psql / Supabase SQL editor). It is the ONLY DDL the service
-- depends on, and it stays in your hands — the service authenticates with the
-- PostgREST API key and can call these functions but cannot create or alter them.
--
-- Each function does the pgvector cosine ranking + top-k selection IN THE DATABASE,
-- so only the top `match_count` rows are returned over the wire. `query_embedding`
-- is declared as bare `vector` (no fixed dimension) so it accepts whatever model
-- the table was embedded with.

create or replace function professional_profile.match_work_experience_chunks(
    query_embedding vector,
    match_count int default 25
)
returns table (
    id uuid,
    content text,
    source_filename text,
    section_title text,
    similarity float
)
language sql
stable
as $$
    select
        id,
        content,
        source_filename,
        section_title,
        1 - (embedding <=> query_embedding) as similarity
    from professional_profile.work_experience_chunks
    where embedding is not null
    order by embedding <=> query_embedding
    limit match_count;
$$;

-- If the professional_profile schema is not yet exposed to PostgREST, add it to
-- the "Exposed schemas" list (Supabase → Settings → API), or these RPCs won't be
-- reachable via /rest/v1/rpc/.
