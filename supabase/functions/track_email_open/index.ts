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
  const thirtyMinAgo = new Date(Date.now() - 30 * 60 * 1000).toISOString();
  console.log("🟢 Processing pixel for tag:", tag);

  try {
    // --- Read current state ---
    const { data: rows, error: fetchError } = await client
      .from("all_emails")
      .select("open_count, opened_at, opened_at_2, opened_at_3")
      .eq("tracked_tag", tag)
      .limit(1);

    if (fetchError || !rows?.length) {
      console.log("ℹ️ Could not read row (tag missing):", tag);
    } else {
      const row = rows[0];
      const currentCount = row.open_count || 0;
      const patch: Record<string, unknown> = { updated_at: now };

      // --- Mark opened_at_1/2/3 in order, each written only once ---
      if (!row.opened_at) {
        patch.opened_at = now;
        console.log("✅ Set opened_at for:", tag);
      } else if (!row.opened_at_2) {
        patch.opened_at_2 = now;
        console.log("✅ Set opened_at_2 for:", tag);
      } else if (!row.opened_at_3) {
        patch.opened_at_3 = now;
        console.log("✅ Set opened_at_3 for:", tag);
      }

      // --- Increment open_count (deduplicated: max once per 30 min, cap at 20) ---
      // Use most recent open timestamp, not updated_at (which changes on every write)
      const lastOpen = row.opened_at_3 || row.opened_at_2 || row.opened_at || "";
      if (currentCount < 20 && lastOpen < thirtyMinAgo) {
        patch.open_count = currentCount + 1;
        console.log("🔢 Incremented open_count to", currentCount + 1, "for:", tag);
      } else {
        console.log("⏭️ Skipped increment (within 30min window or cap reached) for:", tag);
      }

      const { error: updateError } = await client
        .from("all_emails")
        .update(patch)
        .eq("tracked_tag", tag);

      if (updateError) console.error("❌ all_emails update failed:", updateError);
    }

    // --- Also mark sent_emails if present ---
    const { error: sentError } = await client
      .from("sent_emails")
      .update({ opened_at: now })
      .eq("tag", tag)
      .is("opened_at", null);

    if (sentError) {
      console.log("ℹ️ sent_emails update (may not exist):", sentError.message);
    }

    console.log("✅ Pixel processed for:", tag);

    // --- Transparent GIF ---
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
