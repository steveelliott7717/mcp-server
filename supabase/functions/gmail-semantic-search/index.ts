import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY")!;
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// ---------- Types ----------
interface EmailRow {
  id: string;
  subject?: string;
  content: string;
  embedding: number[] | string;
  thread_id?: string;
  gmail_date?: string;
  from_email?: string;
  [key: string]: any;
}

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
async function rerankResults(
  queryText: string,
  results: any[]
): Promise<any[]> {
  try {
    if (!OPENAI_API_KEY || results.length <= 1) return results;

    const docsText = results
      .map((r, idx) => {
        const snippet =
          typeof r.content === "string"
            ? r.content.length > 1200
              ? r.content.slice(0, 1200) + "..."
              : r.content
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
        {"index": 1, "score": 3.2},
        {"index": 2, "score": 7.1}
      ]
    }

    You MUST return exactly ${results.length} scores.`.trim();

    const chatRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        temperature: 0,
        response_format: { type: "json_object" },
        messages: [
          { role: "user", content: prompt },
        ],
      }),
    });

    const data = await chatRes.json();
    const content = data?.choices?.[0]?.message?.content;
    if (!content) {
      console.warn("⚠️ Reranker failed, using hybrid scores");
      return results;
    }

    const parsed = JSON.parse(content);

    if (!parsed.scores || parsed.scores.length !== results.length) {
      console.error(`⚠️ Expected ${results.length} scores, got ${parsed.scores?.length || 0}`);
      return results;
    }

    const scoreMap = new Map<number, number>();
    for (const s of parsed.scores || []) {
      if (typeof s.index === "number" && typeof s.score === "number")
        scoreMap.set(s.index, s.score);
    }

    const scored = results.map((r, i) => ({
      ...r,
      rerank_score: scoreMap.get(i) ?? 0,
    }));

    scored.sort((a, b) => (b.rerank_score ?? 0) - (a.rerank_score ?? 0));
    return scored;
  } catch (err) {
    console.error("🔴 Reranker error:", err);
    return results;
  }
}

// ---------- Main Handler ----------
serve(async (req) => {
  let parsed: SearchRequest;

  try {
    const text = await req.text();
    parsed = JSON.parse(text);
  } catch {
    return new Response(JSON.stringify({ error: "Invalid request JSON" }), {
      status: 400,
    });
  }

  const query_text = parsed.query_text || parsed.query || "";
  const match_count = parsed.match_count || 5;
  const table_name = parsed.table_name || "all_emails";
  const schema = parsed.schema || "gmail";

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
          "Content-Profile": schema,
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
    // Generate query embedding
    const embRes = await fetch("https://api.openai.com/v1/embeddings", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({ model: modelName, input: query_text }),
    });
    const embData = await embRes.json();
    const queryEmbedding: number[] | undefined = embData?.data?.[0]?.embedding;
    if (!queryEmbedding)
      return new Response(
        JSON.stringify({ error: "Failed to generate embedding" }),
        { status: 500 }
      );

    // Fetch all rows
    const rowsRes = await fetch(`${SUPABASE_URL}/rest/v1/${table_name}?select=*`, {
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        "Accept-Profile": schema,
        "Content-Profile": schema,
      },
    });

    const rowsRaw: EmailRow[] = await rowsRes.json();

    const allRows: EmailRow[] = rowsRaw.map((r) => ({
      ...r,
      embedding:
        typeof r.embedding === "string"
          ? JSON.parse(r.embedding)
          : r.embedding,
      content: typeof r.content === "string" ? r.content : "",
    }));

    // Cosine similarity
    const dot = (a: number[], b: number[]) => a.reduce((sum, v, i) => sum + v * b[i], 0);
    const mag = (v: number[]) => Math.sqrt(dot(v, v));

    const similarities = allRows.map((r) =>
      Array.isArray(r.embedding)
        ? dot(r.embedding, queryEmbedding) / (mag(r.embedding) * mag(queryEmbedding))
        : -1
    );

    // BM25 lexical scoring
    const queryTokens = tokenize(query_text);
    const docsTokens = allRows.map((r) => tokenize(r.content));
    const N = docsTokens.length;
    const avgdl = docsTokens.reduce((sum, t) => sum + t.length, 0) / Math.max(1, N);
    const k = 1.5, b = 0.75;

    const df = new Map<string, number>();
    for (const toks of docsTokens) {
      for (const term of new Set<string>(toks)) {
        df.set(term, (df.get(term) || 0) + 1);
      }
    }

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

    // Recency boost (exponential decay, 90-day half-life)
    const now = Date.now();
    const RECENCY_WEIGHT = 0.08;
    const recencyBoosts = allRows.map((r) => {
      const d = r.gmail_date ? new Date(r.gmail_date).getTime() : 0;
      const ageDays = (now - d) / (1000 * 60 * 60 * 24);
      return Math.max(0, RECENCY_WEIGHT * Math.exp(-ageDays / 90));
    });

    // Hybrid score: 70% semantic, 30% BM25, + recency
    const HYBRID_ALPHA = 0.7;
    const cosNorms = similarities.map((s) => (s + 1) / 2);
    const minBM = Math.min(...bm25Scores);
    const maxBM = Math.max(...bm25Scores);
    const bmNorms =
      maxBM === minBM
        ? bm25Scores.map(() => 0.5)
        : bm25Scores.map((s) => (s - minBM) / (maxBM - minBM));

    const scored = allRows.map((r, i) => ({
      id: r.id,
      subject: r.subject,
      content: r.content,
      thread_id: r.thread_id,
      hybrid_score:
        HYBRID_ALPHA * cosNorms[i] + (1 - HYBRID_ALPHA) * bmNorms[i] + recencyBoosts[i],
      cosine_similarity: similarities[i],
      bm25_score: bm25Scores[i],
    }));

    scored.sort((a, b) => (b.hybrid_score ?? 0) - (a.hybrid_score ?? 0));
    let results = scored.slice(0, match_count);

    // LLM rerank top results
    results = await rerankResults(query_text, results);

    // Thread expansion: include sibling emails from same thread
    const expanded: any[] = [];
    for (const r of results) {
      expanded.push(r);
      const thread = allRows.filter(
        (t) => t.thread_id === allRows.find((x) => x.id === r.id)?.thread_id
      );
      for (const t of thread)
        if (t.id !== r.id)
          expanded.push({ id: t.id, content: t.content, is_thread_context: true });
    }

    return new Response(JSON.stringify({ results: expanded.slice(0, match_count) }), {
      status: 200,
    });
  } catch (err) {
    console.error("💥 ERROR", err);
    return new Response(JSON.stringify({ error: err.message }), { status: 500 });
  }
});
