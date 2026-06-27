import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY")!;
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

interface SearchRequest {
  query_text?: string;
  query?: string;
  match_count?: number;
  table_name?: string;
  schema?: string;
  parent_id?: string;
}

// ---------- Text utilities ----------
const STOP_WORDS = new Set([
  "the","a","an","and","or","of","to","in","on","for","with","at",
  "by","from","is","are","was","were","it","this","that","as","be",
  "has","have","had","will","would","can","could","should","about",
  "into","over","under","than","then","so","what","which","who",
  "how","when","where","why","all","each","every","both","few",
  "more","most","other","some","such","no","not","only","own",
  "same","just","also","now","your","my","our","their","his","her"
]);

function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .split(/\s+/)
    .filter((t) => t.length > 1 && !STOP_WORDS.has(t));
}

// ---------- LLM reranker ----------
async function rerankResults(queryText: string, results: any[]): Promise<any[]> {
  try {
    if (!OPENAI_API_KEY || results.length <= 1) return results;

    const docsText = results
      .map((r, idx) => {
        const snippet = typeof r.content === "string"
          ? r.content.length > 1200 ? r.content.slice(0, 1200) + "..." : r.content
          : "";
        return `[${idx}] id=${r.id}\n${snippet}`;
      })
      .join("\n\n");

    const prompt = `Rank these ${results.length} emails by relevance to the query.

Query: "${queryText}"

Emails:
${docsText}

Return JSON with a score (0-10) for EVERY email:
{
  "scores": [
    {"index": 0, "score": 8.5},
    {"index": 1, "score": 3.2}
  ]
}

You MUST return exactly ${results.length} scores.`.trim();

    const chatRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${OPENAI_API_KEY}` },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        temperature: 0,
        response_format: { type: "json_object" },
        messages: [{ role: "user", content: prompt }],
      }),
    });

    const data = await chatRes.json();
    const content = data?.choices?.[0]?.message?.content;
    if (!content) return results;

    const parsed = JSON.parse(content);
    if (!parsed.scores || parsed.scores.length !== results.length) return results;

    const scoreMap = new Map<number, number>();
    for (const s of parsed.scores) {
      if (typeof s.index === "number" && typeof s.score === "number")
        scoreMap.set(s.index, s.score);
    }

    return results
      .map((r, i) => ({ ...r, rerank_score: scoreMap.get(i) ?? 0 }))
      .sort((a, b) => b.rerank_score - a.rerank_score);
  } catch (err) {
    console.error("🔴 Reranker error:", err);
    return results;
  }
}

// ---------- Main Handler ----------
serve(async (req: Request) => {
  let parsed: SearchRequest;
  try {
    const text = await req.text();
    parsed = JSON.parse(text);
  } catch {
    return new Response(JSON.stringify({ error: "Invalid request JSON" }), { status: 400 });
  }

  const query_text = parsed.query_text || parsed.query || "";
  const match_count = parsed.match_count || 5;
  const table_name = parsed.table_name || "all_emails";
  const schema = parsed.schema || "gmail";
  const candidate_count = Math.max(match_count * 4, 20);

  if (!query_text) {
    return new Response(JSON.stringify({ error: "query_text is required" }), { status: 400 });
  }

  // Model autodetect from first row
  let modelName = "text-embedding-3-large";
  try {
    const modelRes = await fetch(
      `${SUPABASE_URL}/rest/v1/${table_name}?select=embedding_model&limit=1`,
      {
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          "Accept-Profile": schema,
        },
      }
    );
    const modelData = await modelRes.json();
    if (Array.isArray(modelData) && modelData[0]?.embedding_model)
      modelName = modelData[0].embedding_model;
  } catch {
    // fallback to default
  }

  try {
    // 1. Generate query embedding
    const embRes = await fetch("https://api.openai.com/v1/embeddings", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${OPENAI_API_KEY}` },
      body: JSON.stringify({ model: modelName, input: query_text }),
    });
    const embData = await embRes.json();
    const queryEmbedding: number[] | undefined = embData?.data?.[0]?.embedding;
    if (!queryEmbedding)
      return new Response(JSON.stringify({ error: "Failed to generate embedding" }), { status: 500 });

    // 2. pgvector search via generic RPC — returns only top N ids + similarity
    const rpcRes = await fetch(`${SUPABASE_URL}/rest/v1/rpc/match_emails_by_embedding`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      },
      body: JSON.stringify({
        p_schema: schema,
        p_table: table_name,
        query_embedding: queryEmbedding,
        match_count: candidate_count,
      }),
    });
    const vectorMatches: { id: string; similarity: number }[] = await rpcRes.json();
    if (!Array.isArray(vectorMatches) || vectorMatches.length === 0)
      return new Response(JSON.stringify({ results: [] }), { status: 200 });

    // 3. Fetch full rows for matched IDs
    const ids = vectorMatches.map((m) => m.id).join(",");
    const rowsRes = await fetch(
      `${SUPABASE_URL}/rest/v1/${table_name}?id=in.(${ids})&select=*`,
      {
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          "Accept-Profile": schema,
        },
      }
    );
    const rowsRaw: any[] = await rowsRes.json();
    if (!Array.isArray(rowsRaw))
      return new Response(JSON.stringify({ error: "Failed to fetch rows", detail: rowsRaw }), { status: 500 });

    // Merge similarity scores and normalise content
    const simMap = new Map(vectorMatches.map((m) => [m.id, m.similarity]));
    const candidates = rowsRaw
      .map((r) => ({
        ...r,
        similarity: simMap.get(r.id) ?? 0,
        content: typeof r.content === "string" ? r.content : "",
        embedding: undefined, // drop large vector from working set
      }))
      .sort((a, b) => b.similarity - a.similarity);

    // 4. BM25 lexical scoring over candidates
    const queryTokens = tokenize(query_text);
    const docsTokens = candidates.map((r) => tokenize(r.content));
    const N = docsTokens.length;
    const avgdl = docsTokens.reduce((sum, t) => sum + t.length, 0) / Math.max(1, N);
    const k = 1.5, b = 0.75;

    const df = new Map<string, number>();
    for (const toks of docsTokens)
      for (const term of new Set<string>(toks))
        df.set(term, (df.get(term) || 0) + 1);

    const bm25Scores = docsTokens.map((toks) => {
      const tf = new Map<string, number>();
      toks.forEach((t) => tf.set(t, (tf.get(t) || 0) + 1));
      let score = 0;
      for (const term of queryTokens) {
        const freq = tf.get(term) || 0;
        const dfTerm = df.get(term) || 0;
        const idf = Math.log((N - dfTerm + 0.5) / (dfTerm + 0.5) + 1);
        score += idf * ((freq * (k + 1)) / (freq + k * (1 - b + (b * toks.length) / avgdl)));
      }
      return score;
    });

    // 5. Recency boost (exponential decay, 90-day half-life)
    const now = Date.now();
    const RECENCY_WEIGHT = 0.08;
    const recencyBoosts = candidates.map((r) => {
      const d = r.gmail_date ? new Date(r.gmail_date).getTime() : 0;
      const ageDays = (now - d) / (1000 * 60 * 60 * 24);
      return Math.max(0, RECENCY_WEIGHT * Math.exp(-ageDays / 90));
    });

    // 6. Hybrid score: 70% semantic, 30% BM25, + recency
    const HYBRID_ALPHA = 0.7;
    const cosNorms = candidates.map((r) => (r.similarity + 1) / 2);
    const minBM = Math.min(...bm25Scores);
    const maxBM = Math.max(...bm25Scores);
    const bmNorms = maxBM === minBM
      ? bm25Scores.map(() => 0.5)
      : bm25Scores.map((s) => (s - minBM) / (maxBM - minBM));

    const scored = candidates.map((r, i) => ({
      ...r,
      hybrid_score: HYBRID_ALPHA * cosNorms[i] + (1 - HYBRID_ALPHA) * bmNorms[i] + recencyBoosts[i],
      cosine_similarity: candidates[i].similarity,
      bm25_score: bm25Scores[i],
    }));

    scored.sort((a, b) => b.hybrid_score - a.hybrid_score);
    let results = scored.slice(0, match_count);

    // 7. LLM rerank
    results = await rerankResults(query_text, results);

    // 8. Thread expansion from candidates pool
    const expanded: any[] = [];
    const seen = new Set<string>();
    for (const r of results) {
      expanded.push(r);
      seen.add(r.id);
      if (r.thread_id) {
        for (const t of candidates) {
          if (t.thread_id === r.thread_id && !seen.has(t.id)) {
            expanded.push({ ...t, is_thread_context: true });
            seen.add(t.id);
          }
        }
      }
    }

    const project = (r: any) => ({
      id: r.id,
      message_id: r.message_id ?? null,
      thread_id: r.thread_id ?? null,
      from_email: r.from_email ?? null,
      to_email: r.to_email ?? null,
      cc_email: r.cc_email ?? null,
      bcc_email: r.bcc_email ?? null,
      reply_to: r.reply_to ?? null,
      subject: r.subject ?? null,
      push_body: r.push_body ?? null,
      gmail_date: r.gmail_date ?? null,
      message_type: r.message_type ?? null,
      body_text: r.body_text ?? null,
      similarity: r.cosine_similarity ?? r.similarity ?? null,
      hybrid_score: r.hybrid_score ?? null,
      rerank_score: r.rerank_score ?? null,
      ...(r.is_thread_context ? { is_thread_context: true } : {}),
    });

    return new Response(JSON.stringify({ results: expanded.slice(0, match_count).map(project) }), { status: 200 });
  } catch (err: any) {
    console.error("💥 ERROR", err);
    return new Response(JSON.stringify({ error: err.message }), { status: 500 });
  }
});
