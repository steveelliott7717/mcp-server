import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY")!;
const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

serve(async (req) => {
  try {
    // Parse the webhook payload
    const payload = await req.json();
    console.log("[Webhook Triggered] Full payload:", JSON.stringify(payload));

    // Supabase database webhooks send the changed row under payload.record
    const record = payload?.record;
    if (!record) {
      console.error("[ERROR] Missing 'record' field in webhook payload");
      return new Response(
        JSON.stringify({ error: "Missing 'record' in payload" }),
        { status: 400 },
      );
    }

    const id = record.id;
    const content = record.content;

    console.log("[START] Processing publication_chunk:", id);
    console.log("[CONTENT PREVIEW]:", (content ?? "").slice(0, 120));

    if (!id || !content) {
      console.error("[ERROR] Missing id or content:", { id, content });
      return new Response(JSON.stringify({ error: "Missing id or content" }), {
        status: 400,
      });
    }

    // --- Step 1: Generate embedding from OpenAI ---
    console.log("[STEP 1] Requesting OpenAI embedding...");
    const openaiResponse = await fetch("https://api.openai.com/v1/embeddings", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: "text-embedding-3-large",
        input: content,
      }),
    });

    const openaiData = await openaiResponse.json();
    if (!openaiResponse.ok || !openaiData?.data?.[0]?.embedding) {
      console.error("[ERROR] OpenAI embedding failed:", openaiData);
      return new Response(
        JSON.stringify({
          error: "Embedding generation failed",
          details: openaiData,
        }),
        { status: 500 },
      );
    }

    const embedding = openaiData.data[0].embedding;
    console.log("[STEP 1 ✅] Embedding generated successfully:", {
      length: embedding.length,
    });

    // --- Step 2: Patch Supabase row with embedding ---
    const patchUrl = `${SUPABASE_URL}/rest/v1/professional_profile.publication_chunks?id=eq.${id}`;
    console.log("[STEP 2] Updating publication_chunk row:", patchUrl);

    const updateResponse = await fetch(patchUrl, {
      method: "PATCH",
      headers: {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ embedding }),
    });

    const updateText = await updateResponse.text();
    if (!updateResponse.ok) {
      console.error("[ERROR] Failed to update Supabase row:", updateText);
      return new Response(
        JSON.stringify({
          error: "Supabase update failed",
          details: updateText,
        }),
        { status: 500 },
      );
    }

    console.log("[STEP 2 ✅] Supabase row updated successfully:", { id });

    // --- Step 3: Return success response ---
    console.log("[COMPLETE] Embedding process finished successfully for:", id);
    return new Response(JSON.stringify({ success: true }), { status: 200 });
  } catch (err) {
    console.error("[FATAL ERROR]", err);
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500,
    });
  }
});
