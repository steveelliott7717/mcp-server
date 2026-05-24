import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY")!;
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

async function sha256(text: string): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, "0")).join("");
}

serve(async (req) => {
  try {
    const payload = await req.json();
    console.log("[Webhook Triggered]", JSON.stringify(payload).slice(0, 300));

    const schema: string = payload?.schema;
    const table: string = payload?.table;
    const record = payload?.record;
    const eventType: string = payload?.type; // INSERT | UPDATE

    if (!schema || !table || !record) {
      console.error("[ERROR] Missing schema, table, or record");
      return new Response(JSON.stringify({ error: "Missing schema, table, or record" }), { status: 400 });
    }

    const id = record.id;
    const content = record.content;

    console.log(`[START] ${schema}.${table} id=${id} event=${eventType}`);

    if (!id || !content) {
      console.error("[ERROR] Missing id or content");
      return new Response(JSON.stringify({ error: "Missing id or content" }), { status: 400 });
    }

    // On UPDATE: skip if embedded_at is still set — means content didn't change
    // (mark_for_reembed trigger clears embedded_at when content changes)
    if (eventType === "UPDATE" && record.embedded_at) {
      console.log(`[SKIP] id=${id} already embedded, content unchanged`);
      return new Response(JSON.stringify({ skipped: true }), { status: 200 });
    }

    // Step 1: Generate embedding
    console.log("[STEP 1] Requesting OpenAI embedding...");
    const openaiRes = await fetch("https://api.openai.com/v1/embeddings", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: "text-embedding-3-large",
        input: content.slice(0, 8000),
      }),
    });

    const openaiData = await openaiRes.json();
    console.log(`[STEP 1 STATUS] ${openaiRes.status} hasEmbedding=${!!openaiData?.data?.[0]?.embedding}`);
    if (!openaiRes.ok || !openaiData?.data?.[0]?.embedding) {
      console.error("[ERROR] OpenAI embedding failed:", openaiData);
      return new Response(JSON.stringify({ error: "Embedding failed", details: openaiData }), { status: 500 });
    }

    const embedding = openaiData.data[0].embedding;
    console.log(`[STEP 1 ✅] Embedding generated: ${embedding.length} dims`);

    // Step 2: Patch row
    const contentHash = await sha256(content);
    const patchUrl = `${SUPABASE_URL}/rest/v1/${table}?id=eq.${id}`;
    console.log("[STEP 2] Patching:", patchUrl);

    const updateRes = await fetch(patchUrl, {
      method: "PATCH",
      headers: {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json",
        "Content-Profile": schema,
        "Prefer": "return=minimal",
      },
      body: JSON.stringify({
        embedding,
        embedding_model: "text-embedding-3-large",
        embedded_at: new Date().toISOString(),
        content_hash: contentHash,
      }),
    });

    const updateStatus = updateRes.status;
    const updateText = await updateRes.text();
    console.log(`[STEP 2 STATUS] ${updateStatus} body=${updateText.slice(0, 200)}`);
    if (!updateRes.ok) {
      console.error("[ERROR] Supabase update failed:", updateStatus, updateText);
      return new Response(JSON.stringify({ error: "Supabase update failed", status: updateStatus, details: updateText }), { status: 500 });
    }

    console.log(`[STEP 2 ✅] Updated ${schema}.${table} id=${id}`);
    return new Response(JSON.stringify({ success: true }), { status: 200 });

  } catch (err) {
    console.error("[FATAL ERROR]", err);
    return new Response(JSON.stringify({ error: (err as Error).message }), { status: 500 });
  }
});
