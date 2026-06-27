// Reads listing_description from finance.marketplace_listings for all active Poshmark listings
// and pushes each one to Poshmark via poshmark_edit_listing.
// Usage: node sync_descriptions_to_poshmark.mjs

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import { callTool } from "./mcp.js";

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function parseRows(res) {
    const content = res?.content?.[0] || res?.result?.content?.[0];
    if (!content) return [];
    if (content.json) {
        const j = content.json;
        if (Array.isArray(j)) return j;
        if (Array.isArray(j.rows)) return j.rows;
    }
    if (content.type === "text" && content.text) {
        try {
            const p = JSON.parse(content.text);
            if (Array.isArray(p)) return p;
            if (Array.isArray(p.rows)) return p.rows;
        } catch {}
    }
    return [];
}

console.log("Fetching Poshmark listings from DB...");
const res = await callTool("query_table", {
    schema: "finance",
    table: "marketplace_listings",
    select: ["id", "listing_title", "listing_description", "platform_listing_id"],
    where: {
        platform: { eq: "poshmark" },
        status: { eq: "active" },
        platform_listing_id: { not_null: true },
        listing_description: { not_null: true },
    },
    limit: 200,
});

const listings = parseRows(res);
console.log(`Found ${listings.length} listings to sync\n`);

if (!listings.length) {
    console.log("Nothing to do.");
    process.exit(0);
}

let success = 0, failed = 0;

for (const row of listings) {
    const listing_url = `https://poshmark.com/listing/item-${row.platform_listing_id}`;
    process.stdout.write(`[${row.id}] ${row.listing_title?.slice(0, 50)}... `);
    try {
        const result = await callTool("poshmark_edit_listing", {
            listing_url,
            description: row.listing_description,
        });
        const content = result?.content?.[0] || result?.result?.content?.[0];
        const ok = content?.json?.ok ?? (typeof content?.text === "string" && content.text.includes("ok"));
        if (ok) {
            console.log("✅");
            success++;
        } else {
            console.log("⚠️  unexpected:", JSON.stringify(content ?? result ?? null).slice(0, 120));
            failed++;
        }
    } catch (err) {
        console.log(`❌ ${err.message}`);
        failed++;
    }
    // Pause between listings to avoid hammering Poshmark
    await sleep(4000);
}

console.log(`\nDone: ${success} succeeded, ${failed} failed`);
