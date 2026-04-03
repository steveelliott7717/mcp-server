import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY");
const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

// ---------- Text utilities for BM25 ----------
const STOP_WORDS = new Set([
  "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "at",
  "by", "from", "is", "are", "was", "were", "it", "this", "that", "as", "be",
  "has", "have", "had", "will", "would", "can", "could", "should", "about",
  "into", "over", "under", "than", "then", "so", "what", "which", "who",
  "how", "when", "where", "why", "all", "each", "every", "both", "few",
  "more", "most", "other", "some", "such", "no", "not", "only", "own",
  "same", "just", "also", "now", "your", "my", "our", "their", "his", "her"
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
  results: any[],
  parentIdField: string
): Promise<any[]> {
  try {
    if (!OPENAI_API_KEY) {
      console.warn("⚠️ [RERANK] No OPENAI_API_KEY; skipping rerank");
      return results;
    }
    if (!results || results.length <= 1) {
      console.log("ℹ️ [RERANK] 0 or 1 result; skipping rerank");
      return results;
    }

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

    const prompt = `
You are a reranking model. Score how relevant each document is to answering the user query.

User query:
"""${queryText}"""

Documents:
${docsText}

Instructions:
- For each document index [i], assign a relevance score from 0 to 10.
- 0 = completely unrelated, 10 = directly and strongly answers the query.
- Base your scores ONLY on how useful the document would be to answer the query.
- Return ONLY valid JSON of the form:

{
  "scores": [
    {"index": 0, "score": 7.5},
    {"index": 1, "score": 3.0}
  ]
}
`.trim();

    console.log("🧠 [RERANK] Calling OpenAI reranker (gpt-4.1-mini)...");
    const chatRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: "gpt-4.1-mini",
        temperature: 0,
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content: "You are a precise document relevance scorer for semantic search.",
          },
          { role: "user", content: prompt },
        ],
      }),
    });

    const chatData = await chatRes.json();
    const content = chatData?.choices?.[0]?.message?.content;
    if (!content) {
      console.error("❌ [RERANK] No content from OpenAI:", chatData);
      return results;
    }

    let parsed: any;
    try {
      parsed = JSON.parse(content);
    } catch (err) {
      console.error("❌ [RERANK] Failed to parse JSON from model:", err, content);
      return results;
    }

    const indexToScore = new Map<number, number>();
    for (const item of parsed.scores || []) {
      if (
        typeof item.index === "number" &&
        typeof item.score === "number" &&
        item.index >= 0 &&
        item.index < results.length
      ) {
        indexToScore.set(item.index, item.score);
      }
    }

    if (indexToScore.size === 0) {
      console.warn("⚠️ [RERANK] No valid scores returned; keeping original order");
      return results;
    }

    const scored = results.map((r, idx) => ({
      ...r,
      rerank_score: indexToScore.get(idx) ?? 0,
    }));

    scored.sort((a, b) => {
      const sa = typeof a.rerank_score === "number" ? a.rerank_score : 0;
      const sb = typeof b.rerank_score === "number" ? b.rerank_score : 0;
      if (sb !== sa) return sb - sa;
      return (b.hybrid_score ?? 0) - (a.hybrid_score ?? 0);
    });

    console.log(
      "[RERANK] Top reranked scores:",
      scored.slice(0, 5).map((r) => r.rerank_score)
    );
    return scored;
  } catch (err) {
    console.error("💥 [RERANK FATAL] Error during reranking:", err);
    return results;
  }
}

serve(async (req) => {
  console.log("🚀 [START] Semantic search request received");

  // STEP 0: Parse request body
  let bodyText = "";
  try {
    bodyText = await req.text();
  } catch (err) {
    console.error("❌ [ERROR] Could not read body text:", err);
    await new Promise((resolve) => setTimeout(resolve, 800));
    return new Response(JSON.stringify({ error: "Invalid body" }), { status: 400 });
  }

  let query_text = "";
  let match_count = 5;
  let table_name = "";
  let parent_id: string | null = null;
  let parent_ids: any[] | null = null;
  let parent_id_field = "experience_id";
  let schema = "public";
  let enable_diversity = true;

  try {
    const parsed = JSON.parse(bodyText);
    query_text = parsed.query_text || parsed.query;
    match_count = parsed.match_count || 5;
    table_name = parsed.table_name;
    schema = parsed.schema || "public";
    parent_id_field = parsed.parent_id_field || "experience_id";
    enable_diversity = parsed.enable_diversity !== false;

    parent_id = parsed.experience_id || parsed.parent_id || null;
    parent_ids = parsed.experience_ids || parsed.parent_ids || null;

    if (!table_name) {
      console.error("❌ [ERROR] Missing table_name in request");
      return new Response(
        JSON.stringify({ error: "Missing required field: table_name" }),
        { status: 400 }
      );
    }

    console.log("✅ [DEBUG] Parsed request:", {
      query_text,
      match_count,
      table_name,
      parent_id,
      parent_ids,
      parent_id_field,
      schema,
      enable_diversity,
    });
  } catch (err) {
    console.error("❌ [ERROR] JSON parse failed:", err);
    await new Promise((resolve) => setTimeout(resolve, 800));
    return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400 });
  }

  // Auto-detect embedding model from table
  let modelName = "text-embedding-3-small";
  try {
    const sampleRes = await fetch(
      `${SUPABASE_URL}/rest/v1/${table_name}?select=embedding_model&limit=1`,
      {
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY!,
          Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          "Accept-Profile": schema,
          "Content-Profile": schema,
        },
      }
    );
    const sampleData = await sampleRes.json();
    if (Array.isArray(sampleData) && sampleData[0]?.embedding_model) {
      const detected = sampleData[0].embedding_model;
      if (["text-embedding-3-small", "text-embedding-3-large"].includes(detected)) {
        modelName = detected;
        console.log(`✅ [MODEL DETECTION] Detected model: ${modelName}`);
      }
    }
  } catch (err: any) {
    console.warn(`⚠️ [MODEL DETECTION] Failed: ${err.message}`);
  }

  try {
    // Step 1: Generate query embedding
    console.log("🧠 [STEP 1] Requesting embedding from OpenAI...");
    const embeddingRes = await fetch("https://api.openai.com/v1/embeddings", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({ model: modelName, input: query_text }),
    });

    const embeddingData = await embeddingRes.json();
    const query_embedding = embeddingData?.data?.[0]?.embedding;

    if (!query_embedding) {
      console.error("❌ [ERROR] Failed to generate embedding:", embeddingData);
      await new Promise((resolve) => setTimeout(resolve, 800));
      return new Response(
        JSON.stringify({ error: "Embedding generation failed", details: embeddingData }),
        { status: 500 }
      );
    }
    console.log(`✅ [STEP 1 COMPLETE] Embedding generated (${query_embedding.length} dims)`);

    // Step 2: Fetch rows from Supabase with optional parent ID filter(s)
    let queryUrl = `${SUPABASE_URL}/rest/v1/${table_name}?select=*`;

    if (parent_id) {
      queryUrl += `&${parent_id_field}=eq.${parent_id}`;
    } else if (parent_ids && Array.isArray(parent_ids)) {
      const idList = parent_ids
        .map((item) => (typeof item === "object" ? item.id : item))
        .join(",");
      queryUrl += `&${parent_id_field}=in.(${idList})`;
    }

    const searchRes = await fetch(queryUrl, {
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY!,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        "Accept-Profile": schema,
        "Content-Profile": schema,
      },
    });

    const rowsRaw = await searchRes.json();
    if (!Array.isArray(rowsRaw)) {
      console.error("❌ [ERROR] Unexpected Supabase response:", rowsRaw);
      return new Response(
        JSON.stringify({ error: "Invalid response from Supabase", details: rowsRaw }),
        { status: 500 }
      );
    }

    console.log(`📦 [STEP 2 COMPLETE] Retrieved ${rowsRaw.length} rows`);

    if (rowsRaw.length === 0) {
      return new Response(JSON.stringify({ results: [] }), { status: 200 });
    }

    // Step 3: Parse embeddings
    const allRows = rowsRaw.map((row, idx) => {
      let parsedEmbedding = row.embedding;

      if (typeof row.embedding === "string") {
        const cleaned = row.embedding.replace(/[\[\]{}]/g, "");
        const parts = cleaned.split(",");
        parsedEmbedding = parts.map((v: string) => parseFloat(v.trim()));
      }

      return {
        ...row,
        embedding: parsedEmbedding,
        content: typeof row.content === "string" ? row.content : row.content ?? "",
      };
    });

    // Step 4: Cosine similarity
    const dot = (a: number[], b: number[]) => a.reduce((sum, v, i) => sum + v * b[i], 0);
    const mag = (v: number[]) => Math.sqrt(dot(v, v));

    const similarities: number[] = allRows.map((row) =>
      Array.isArray(row.embedding) && row.embedding.length === query_embedding.length
        ? dot(row.embedding, query_embedding) / (mag(row.embedding) * mag(query_embedding))
        : -1
    );

    // Step 5: BM25 lexical scoring
    const queryLower = query_text.toLowerCase().trim();
    const queryTokens = tokenize(query_text);
    const uniqueQueryTerms = Array.from(new Set(queryTokens));
    const queryTermSet = new Set(uniqueQueryTerms);
    const queryWords = queryLower.split(/\s+/).filter((w) => w.length > 2 && !STOP_WORDS.has(w));

    const docsTokens = allRows.map((row) => tokenize(row.content));
    const N = docsTokens.length;
    const docLengths = docsTokens.map((toks) => toks.length || 1);
    const avgdl = docLengths.reduce((sum, len) => sum + len, 0) / Math.max(N, 1);

    let bm25Scores = new Array(N).fill(0);

    if (uniqueQueryTerms.length > 0) {
      const df = new Map<string, number>();
      for (let i = 0; i < N; i++) {
        const docSet = new Set(docsTokens[i]);
        for (const term of uniqueQueryTerms) {
          if (docSet.has(term)) df.set(term, (df.get(term) || 0) + 1);
        }
      }

      const k = 1.5, b = 0.75;

      for (let i = 0; i < N; i++) {
        const tokens = docsTokens[i];
        const len = docLengths[i];
        const tf = new Map<string, number>();
        for (const t of tokens) {
          if (queryTermSet.has(t)) tf.set(t, (tf.get(t) || 0) + 1);
        }

        let score = 0;
        for (const term of uniqueQueryTerms) {
          const freq = tf.get(term) || 0;
          if (freq === 0) continue;
          const dfTerm = df.get(term) || 0;
          const idf = Math.log((N - dfTerm + 0.5) / (dfTerm + 0.5) + 1);
          score += idf * ((freq * (k + 1)) / (freq + k * (1 - b + (b * len) / avgdl)));
        }
        bm25Scores[i] = score;
      }
    }

    // Phrase boost
    const PHRASE_BOOST = 0.15;
    const phraseBoosts = allRows.map((row) => {
      const contentLower = (row.content || "").toLowerCase();
      let maxBoost = 0;
      for (let phraseLen = Math.min(4, queryWords.length); phraseLen >= 2; phraseLen--) {
        for (let i = 0; i <= queryWords.length - phraseLen; i++) {
          const phrase = queryWords.slice(i, i + phraseLen).join(" ");
          if (phrase.length > 5 && contentLower.includes(phrase)) {
            maxBoost = Math.max(maxBoost, PHRASE_BOOST * (0.4 + phraseLen * 0.15));
          }
        }
      }
      return maxBoost;
    });

    // Domain keyword boost
    const KEYWORD_BOOST = 0.10;
    const keywordBoosts = allRows.map((row) => {
      const keywords: string[] = row.domain_keywords || [];
      if (!keywords.length) return 0;
      const keywordsLower = keywords.map((k) => k.toLowerCase());
      let matchScore = 0;
      for (const queryWord of queryWords) {
        if (keywordsLower.some((k) => k.includes(queryWord) || queryWord.includes(k)))
          matchScore += 0.025;
      }
      for (const keyword of keywordsLower) {
        if (queryLower.includes(keyword) || keyword.includes(queryLower))
          matchScore += 0.04;
      }
      return Math.min(matchScore, KEYWORD_BOOST);
    });

    // Section title boost
    const TITLE_BOOST = 0.08;
    const titleBoosts = allRows.map((row) => {
      const title = (row.section_title || "").toLowerCase();
      if (!title) return 0;
      let matchScore = 0;
      for (const queryWord of queryWords) {
        if (title.includes(queryWord)) matchScore += 0.025;
      }
      if (queryWords.length >= 2) {
        for (let i = 0; i < queryWords.length - 1; i++) {
          if (title.includes(queryWords[i] + " " + queryWords[i + 1])) matchScore += 0.04;
        }
      }
      return Math.min(matchScore, TITLE_BOOST);
    });

    // Schema tags boost
    const TAG_BOOST = 0.08;
    const tagBoosts = allRows.map((row) => {
      const tags: string[] = row.schema_tags || [];
      if (!tags.length) return 0;
      const tagsNormalized = tags.map((t) => t.toLowerCase().replace(/_/g, " "));
      let matchScore = 0;
      for (const tag of tagsNormalized) {
        for (const queryWord of queryWords) {
          if (tag.includes(queryWord)) matchScore += 0.02;
        }
        const queryNormalized = queryLower.replace(/\s+/g, " ");
        if (queryNormalized.includes(tag) || tag.includes(queryNormalized)) matchScore += 0.04;
      }
      return Math.min(matchScore, TAG_BOOST);
    });

    // Step 6: Hybrid scoring (55% semantic, 45% BM25 + boosts)
    const cosNorms = similarities.map((s) => (s + 1) / 2);
    let minBM = Math.min(...bm25Scores);
    let maxBM = Math.max(...bm25Scores);
    if (!isFinite(minBM)) minBM = 0;
    if (!isFinite(maxBM)) maxBM = 0;
    const bmNorms =
      maxBM === minBM
        ? bm25Scores.map(() => 0.5)
        : bm25Scores.map((s) => (s - minBM) / (maxBM - minBM));

    const HYBRID_ALPHA = 0.55;

    const allResults = allRows.map((row, idx) => ({
      id: row.id,
      content: row.content,
      hybrid_score:
        HYBRID_ALPHA * cosNorms[idx] +
        (1 - HYBRID_ALPHA) * bmNorms[idx] +
        phraseBoosts[idx] +
        keywordBoosts[idx] +
        titleBoosts[idx] +
        tagBoosts[idx],
      similarity: similarities[idx],
      bm25_score: bm25Scores[idx],
    }));

    allResults.sort((a, b) => (b.hybrid_score ?? 0) - (a.hybrid_score ?? 0));

    // Step 6.5: Source diversity (when no parent filter)
    let candidateResults = allResults;
    if (enable_diversity && !parent_id) {
      const MAX_PER_SOURCE = Math.ceil(match_count * 0.6);
      const sourceCounts = new Map<string, number>();
      const diverseResults: typeof allResults = [];

      for (const result of allResults) {
        const source = allRows.find((r) => r.id === result.id)?.source_filename || "unknown";
        const count = sourceCounts.get(source) || 0;
        if (count < MAX_PER_SOURCE) {
          diverseResults.push(result);
          sourceCounts.set(source, count + 1);
          if (diverseResults.length >= match_count * 2) break;
        }
      }
      candidateResults = diverseResults;
    }

    // Step 7: Per-parent chunk limits or global slice
    let results: typeof allResults;
    if (
      parent_ids &&
      Array.isArray(parent_ids) &&
      parent_ids.some((item) => typeof item === "object" && "match_count" in item)
    ) {
      results = [];
      const parentConfigs = parent_ids.map((item) =>
        typeof item === "object"
          ? { id: item.id, match_count: item.match_count || match_count }
          : { id: item, match_count }
      );

      for (const config of parentConfigs) {
        const parentResults = candidateResults
          .filter((r) => {
            const fullRow = allRows.find((ar) => ar.id === r.id);
            return fullRow && fullRow[parent_id_field] === config.id;
          })
          .slice(0, config.match_count);
        results.push(...parentResults);
      }

      results.sort((a, b) => (b.hybrid_score ?? 0) - (a.hybrid_score ?? 0));
    } else {
      results = candidateResults.slice(0, match_count);
    }

    // Step 8: LLM rerank
    results = await rerankResults(query_text, results, parent_id_field);

    // Step 9: Return minimal response (id + content only)
    const minimalResults = results.map((r) => ({ id: r.id, content: r.content }));

    console.log(`✅ [COMPLETE] Returning ${minimalResults.length} results`);

    await new Promise((resolve) => setTimeout(resolve, 800));
    return new Response(
      JSON.stringify({
        results: minimalResults,
        meta: { total_candidates: allRows.length, returned_count: minimalResults.length },
      }),
      { status: 200 }
    );
  } catch (err: any) {
    console.error("💥 [FATAL ERROR]", err);
    await new Promise((resolve) => setTimeout(resolve, 800));
    return new Response(JSON.stringify({ error: err.message }), { status: 500 });
  }
});
