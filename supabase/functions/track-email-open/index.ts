import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

serve(async (req) => {
  const url = new URL(req.url);
  const tag = url.searchParams.get("tag");

  if (!tag) {
    return new Response("Missing tag", { status: 400 });
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
  const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const client = createClient(supabaseUrl, serviceKey, {
    db: { schema: "gmail" },
  });

  const now = new Date().toISOString();
  console.log("🟢 Processing pixel for tag:", tag);

  try {
    // Mark opened_at on first open
    const { error: emailError } = await client
      .from("all_emails")
      .update({ opened_at: now, updated_at: now })
      .eq("tracked_tag", tag)
      .is("opened_at", null);

    if (emailError) {
      console.error("❌ all_emails update failed:", emailError);
    }

    // Increment open_count
    const { data: rows, error: fetchError } = await client
      .from("all_emails")
      .select("open_count")
      .eq("tracked_tag", tag)
      .limit(1);

    if (!fetchError && rows?.length) {
      const currentCount = rows[0].open_count || 0;
      const { error: incError } = await client
        .from("all_emails")
        .update({ open_count: currentCount + 1, updated_at: now })
        .eq("tracked_tag", tag);

      if (incError) console.error("⚠️ Increment failed:", incError);
    }

    // Also mark sent_emails if present
    const { error: sentError } = await client
      .from("sent_emails")
      .update({ opened_at: now })
      .eq("tag", tag)
      .is("opened_at", null);

    if (sentError) {
      console.log("ℹ️ sent_emails update (may not exist):", sentError.message);
    }

    console.log("✅ Pixel processed for:", tag);

    // Return transparent 1x1 GIF pixel
    const pixel = new Uint8Array([
      71, 73, 70, 56, 57, 97, 1, 0, 1, 0, 128, 0, 0,
      0, 0, 0, 255, 255, 255, 33, 249, 4, 1, 0, 0,
      1, 0, 44, 0, 0, 0, 0, 1, 0, 1, 0, 0, 2, 2,
      68, 1, 0, 59,
    ]);

    return new Response(pixel, {
      headers: {
        "Content-Type": "image/gif",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
        "Access-Control-Allow-Origin": "*",
      },
    });
  } catch (err) {
    console.error("❌ Unexpected error:", err);
    return new Response("Internal Error", { status: 500 });
  }
});
