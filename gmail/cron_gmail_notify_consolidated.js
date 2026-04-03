// /opt/supabase-mcp/custom/gmail/cron_gmail_notify_consolidated.js
// 📲 Gmail Notification Cron (Consolidated Version)
// ✅ Queries gmail.all_emails directly for unnotified replies
// ✅ Cleans snippets
// ✅ Includes tracked tag for traceability
// ✅ Updates notified_at in all_emails

import { callTool } from "./mcp.js";

/* -------------------------------------------------------------------------- */
/* 🧠 Utility Functions                                                       */
/* -------------------------------------------------------------------------- */

/** Parse MCP tool response safely */
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
        } catch {
            return null;
        }
    }

    if (content.json) {
        const j = content.json;
        if (Array.isArray(j)) return j;
        if (Array.isArray(j.rows)) return j.rows;
        if (Array.isArray(j.data)) return j.data;
        return j;
    }

    if (typeof content === "object" && !content.type) {
        if (Array.isArray(content.rows)) return content.rows;
        if (Array.isArray(content.data)) return content.data;
        return content;
    }

    return null;
}
/** Clean Gmail snippet text for push notifications */
function cleanSnippet(snippet = "") {
    return snippet
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/&amp;/g, "&")
        .replace(/On .*wrote:.*/is, "")
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 120);
}

/* -------------------------------------------------------------------------- */
/* 🚀 Main Notification Function                                             */
/* -------------------------------------------------------------------------- */

async function notifyNewReplies() {
    const runId = Date.now().toString(36);
    console.log(`[${new Date().toISOString()}] 📲 [${runId}] Starting notifications`);

    // 1️⃣ Query all_emails for unnotified replies to tracked emails
    let rows = [];
    try {
        const res = await callTool("query_table", {
            schema: "gmail",
            table: "all_emails",
            select: ["message_id", "from_email", "snippet", "tracked_tag", "subject", "gmail_date"],
            where: {
                is_reply_to_tracked: { eq: true },
                notified_at: { eq: null }
            },
            orderBy: { column: "gmail_date", ascending: false },
            limit: 100,
        });

        rows = parseToolResponse(res) || [];
        console.log(`[${runId}] Found ${rows.length} unnotified replies`);
    } catch (err) {
        console.error(`[${runId}] ❌ Query failed: ${err.message}`);
        process.exitCode = 1;
        return;
    }

    if (!rows.length) {
        console.log(`[${runId}] ✅ No unnotified replies`);
        return;
    }

    // 2️⃣ Send notification for each reply
    let notified = 0;
    for (const r of rows) {
        const messageId = r.message_id;
        const cleaned = cleanSnippet(r.snippet || "");
        const tagLabel = r.tracked_tag ? ` [${r.tracked_tag}]` : "";
        const fromName = r.from_email?.split("@")[0] || "Unknown Sender";

        try {
            // Send Pushover notification
            await callTool("notify_push", {
                provider: "pushover",
                category: "gmail",
                title: `📩 Reply — ${fromName}${tagLabel}`,
                message: cleaned || r.subject || "(No content)",
                no_log: true,
            });
            console.log(`[${runId}] 📲 Push sent for ${messageId}${tagLabel}`);

            // Update notified_at timestamp in all_emails
            await callTool("update_data", {
                schema: "gmail",
                table: "all_emails",
                pk: "id",
                where: { message_id: messageId },
                data: {
                    notified_at: new Date().toISOString(),
                },
            });
            console.log(`[${runId}] ✅ Marked notified for ${messageId}${tagLabel}`);

            notified++;

            // Rate limiting
            await new Promise((r) => setTimeout(r, 250));
        } catch (err) {
            console.error(`[${runId}] ❌ Notification failed for ${messageId}${tagLabel}: ${err.message}`);
        }
    }

    console.log(`[${new Date().toISOString()}] ✅ [${runId}] Notifications complete: ${notified}/${rows.length} sent`);
    process.exitCode = 0;
}

/* -------------------------------------------------------------------------- */
/* 🏁 CLI Entrypoint                                                          */
/* -------------------------------------------------------------------------- */

notifyNewReplies().catch((err) => {
    console.error(`[notifyNewReplies] Fatal error: ${err.message}`);
    process.exitCode = 1;
});