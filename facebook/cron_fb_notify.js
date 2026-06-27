// /opt/supabase-mcp/custom/facebook/cron_fb_notify.js
// Sends Pushover notifications for new inbound Facebook Marketplace messages

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import { callTool } from "./mcp.js";

/* -------------------------------------------------------------------------- */
/* Helpers                                                                     */
/* -------------------------------------------------------------------------- */

function parseToolResponse(res) {
    const content = res?.content?.[0] || res?.result?.content?.[0];
    if (!content) return null;

    if (content.type === "text" && content.text) {
        try {
            const parsed = JSON.parse(content.text);
            if (Array.isArray(parsed)) return parsed;
            if (Array.isArray(parsed.rows)) return parsed.rows;
            if (Array.isArray(parsed.data)) return parsed.data;
            return parsed;
        } catch { return null; }
    }

    if (content.json) {
        const j = content.json;
        if (Array.isArray(j)) return j;
        if (Array.isArray(j.rows)) return j.rows;
        if (Array.isArray(j.data)) return j.data;
        return j;
    }

    return null;
}

/* -------------------------------------------------------------------------- */
/* Main                                                                        */
/* -------------------------------------------------------------------------- */

async function notifyFacebookMessages() {
    const runId = Date.now().toString(36);
    console.log(`[${new Date().toISOString()}] [${runId}] Starting Facebook notifications`);

    let rows = [];
    try {
        const res = await callTool("query_table", {
            schema: "finance",
            table: "fb_messages",
            select: ["id", "fb_message_id", "fb_conversation_id", "from_name", "body", "listing_id"],
            where: {
                is_from_me: { eq: false },
                notified_at: { eq: null },
            },
            orderBy: { column: "sent_at", ascending: true },
            limit: 50,
        });
        rows = parseToolResponse(res) || [];
        console.log(`[${runId}] Found ${rows.length} unnotified messages`);
    } catch (err) {
        console.error(`[${runId}] Query failed: ${err.message}`);
        process.exitCode = 1;
        return;
    }

    if (!rows.length) {
        console.log(`[${runId}] No unnotified messages`);
        return;
    }

    // Fetch listing titles for any linked listing_ids
    const listingIds = [...new Set(rows.map(r => r.listing_id).filter(Boolean))];
    const listingTitleMap = new Map();
    if (listingIds.length) {
        try {
            const listingRes = await callTool("query_table", {
                schema: "finance",
                table: "marketplace_listings",
                select: ["id", "listing_title"],
                where: { id: { in: listingIds } },
            });
            for (const l of parseToolResponse(listingRes) || []) {
                listingTitleMap.set(l.id, l.listing_title);
            }
        } catch { /* non-critical */ }
    }

    let notified = 0;
    for (const row of rows) {
        const preview = (row.body || "(no text)").slice(0, 120);
        const fromLabel = row.from_name || "Unknown";
        const listingTitle = row.listing_id ? listingTitleMap.get(row.listing_id) : null;
        const listingLabel = listingTitle
            ? ` — ${listingTitle.slice(0, 40)}`
            : " — No listing linked";

        try {
            await callTool("notify_push", {
                provider: "pushover",
                category: "facebook_marketplace",
                title: `${listingTitle ? listingTitle.slice(0, 40) : "No listing linked"} — ${fromLabel}`,
                message: `${preview}\n\nThread: ${row.fb_conversation_id}`,
                no_log: true,
            });
            console.log(`[${runId}] Push sent for ${row.fb_message_id}`);

            await callTool("update_data", {
                schema: "finance",
                table: "fb_messages",
                pk: "id",
                where: { fb_message_id: row.fb_message_id },
                data: { notified_at: new Date().toISOString() },
            });

            notified++;
            await new Promise((r) => setTimeout(r, 250));
        } catch (err) {
            console.error(`[${runId}] Failed for ${row.fb_message_id}: ${err.message}`);
        }
    }

    console.log(`[${new Date().toISOString()}] [${runId}] Done: ${notified}/${rows.length} notifications sent`);
    process.exitCode = 0;
}

notifyFacebookMessages().catch((err) => {
    console.error(`Fatal: ${err.message}`);
    process.exitCode = 1;
});
