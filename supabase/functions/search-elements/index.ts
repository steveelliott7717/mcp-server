import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY")!;
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

interface SearchRequest {
  query_text: string;
  script_id?: number;
  scene_id?: number;
  type?: string | string[];
  character_id?: number;
  mentioned_character_id?: number;
  match_count?: number;
}

interface ElementRow {
  id: number;
  scene_id: number;
  type: string;
  content: string;
  character_id: number | null;
  parenthetical: string | null;
  dialogue_modifier: string | null;
  position: number;
  mentioned_character_ids: number[] | null;
  embedding: number[] | string;
}

function dot(a: number[], b: number[]): number {
  return a.reduce((sum, v, i) => sum + v * b[i], 0);
}

function cosine(a: number[], b: number[]): number {
  const mag = (v: number[]) => Math.sqrt(dot(v, v));
  return dot(a, b) / (mag(a) * mag(b));
}

serve(async (req) => {
  try {
    const body: SearchRequest = await req.json();
    const {
      query_text,
      script_id,
      scene_id,
      type: typeFilter,
      character_id,
      mentioned_character_id,
      match_count = 10,
    } = body;

    if (!query_text) {
      return new Response(JSON.stringify({ error: "query_text is required" }), { status: 400 });
    }

    // Step 1: Embed the query
    const embRes = await fetch("https://api.openai.com/v1/embeddings", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: "text-embedding-3-large",
        input: query_text,
      }),
    });

    const embData = await embRes.json();
    const queryEmbedding: number[] | undefined = embData?.data?.[0]?.embedding;
    if (!queryEmbedding) {
      return new Response(JSON.stringify({ error: "Failed to generate query embedding" }), { status: 500 });
    }

    // Step 2: Build filter params for PostgREST
    const params = new URLSearchParams();
    params.set("select", "id,scene_id,type,content,character_id,parenthetical,dialogue_modifier,position,mentioned_character_ids,embedding");
    params.set("embedding", "not.is.null");

    if (script_id)     params.set("script_id", `eq.${script_id}`);
    if (scene_id)      params.set("scene_id", `eq.${scene_id}`);
    if (character_id)  params.set("character_id", `eq.${character_id}`);

    if (typeFilter) {
      if (Array.isArray(typeFilter)) {
        params.set("type", `in.(${typeFilter.join(",")})`);
      } else {
        params.set("type", `eq.${typeFilter}`);
      }
    }

    if (mentioned_character_id) {
      params.set("mentioned_character_ids", `cs.{${mentioned_character_id}}`);
    }

    // Step 3: Fetch filtered rows
    const rowsRes = await fetch(
      `${SUPABASE_URL}/rest/v1/script_elements?${params.toString()}`,
      {
        headers: {
          "apikey": SUPABASE_SERVICE_ROLE_KEY,
          "Authorization": `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          "Accept-Profile": "screenplay",
        },
      }
    );

    const rows: ElementRow[] = await rowsRes.json();
    if (!Array.isArray(rows) || rows.length === 0) {
      return new Response(JSON.stringify({ results: [] }), { status: 200 });
    }

    // Step 4: Cosine similarity on filtered set
    const scored = rows
      .map((r) => {
        const emb = typeof r.embedding === "string" ? JSON.parse(r.embedding) : r.embedding;
        const score = Array.isArray(emb) ? cosine(emb, queryEmbedding) : -1;
        return {
          id: r.id,
          scene_id: r.scene_id,
          type: r.type,
          content: r.content,
          character_id: r.character_id,
          parenthetical: r.parenthetical,
          dialogue_modifier: r.dialogue_modifier,
          position: r.position,
          mentioned_character_ids: r.mentioned_character_ids,
          similarity: score,
        };
      })
      .filter((r) => r.similarity > 0)
      .sort((a, b) => b.similarity - a.similarity)
      .slice(0, match_count);

    return new Response(JSON.stringify({ results: scored }), { status: 200 });

  } catch (err) {
    console.error("[ERROR]", err);
    return new Response(JSON.stringify({ error: (err as Error).message }), { status: 500 });
  }
});
