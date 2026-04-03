// /opt/supabase-mcp/custom/gmail/cron_gmail_pull_consolidated.js
// 📥 Gmail Reply Pull (Consolidated Version)
// ✅ Queries gmail.all_emails directly for tracked emails
// ✅ Checks threads for new replies
// ✅ Reconciles same-batch replies missed during ingestion
// ✅ Updates last_checked_at in all_emails

import { callTool } from "./mcp.js";

const MY_EMAIL = "your-email@example.com";

/* -------------------------------------------------------------------------- */
/* 🧠 Utility Functions                                                       */
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
function parseHttpFetchResponse(res) {
    const content = res?.content?.[0] || res?.result?.content?.[0];
    if (!content) return null;
    if (content.type === "text" && content.text) {
        try {
            const parsed = JSON.parse(content.text);
            return parsed?.data ?? parsed;
        } catch { return null; }
    }
    if (content.json) return content.json?.data ?? content.json;
    return null;
}

/* -------------------------------------------------------------------------- */
/* 🚀 Main Gmail Reply Pull Logic                                            */
/* -------------------------------------------------------------------------- */

async function pullGmailReplies() {
    const runId = Date.now().toString(36);
    console.log(`[${new Date().toISOString()}] 🔄 [${runId}] Starting Gmail reply check`);

    const fifteenMinsAgo = new Date(Date.now() - 15 * 60 * 1000).toISOString();

    // 1️⃣ Find tracked emails that need checking
    let rows = [];
    try {
        const trackedRes = await callTool("query_table", {
            schema: "gmail",
            table: "all_emails",
            select: ["message_id", "thread_id", "tracked_tag", "last_checked_at", "subject"],
            where: {
                is_tracked: { eq: true },
                tracking_active: { eq: true },
            },
            orderBy: { column: "gmail_date", ascending: false },
            limit: 200,
        });

        const rawRows = parseToolResponse(trackedRes) || [];

        // Filter those that haven't been checked recently
        rows = rawRows.filter((r) => {
            if (!r.thread_id) return false;
            const checked = r.last_checked_at;
            if (!checked || checked === null || checked === "null" || checked === "") return true;
            const checkedTime = new Date(checked).getTime();
            const cutoffTime = new Date(fifteenMinsAgo).getTime();
            return isFinite(checkedTime) && checkedTime < cutoffTime;
        });

        console.log(`[${runId}] Found ${rows.length} tracked emails to check (out of ${rawRows.length} total tracked)`);
    } catch (err) {
        console.error(`[${runId}] ❌ Query failed: ${err.message}`);
        return;
    }

    if (!rows.length) {
        console.log(`[${runId}] ✅ No emails need checking at this time`);
    }

    let newRepliesFound = 0;

    // 2️⃣ Inspect each tracked thread for new messages
    for (const email of rows) {
        const { message_id, thread_id, tracked_tag } = email;

        try {
            const res = await callTool("http_fetch", {
                url: `https://gmail.googleapis.com/gmail/v1/users/me/threads/${thread_id}?format=metadata&metadataHeaders=From&metadataHeaders=Subject&metadataHeaders=Date`,
                method: "GET",
                provider: "gmail",
                response_type: "json",
            });

            const json = parseHttpFetchResponse(res) || {};
            const msgs = json.messages || [];
            // 🏷️ Refresh label state for all messages in the thread
            if (Array.isArray(msgs) && msgs.length > 0) {
                for (const m of msgs) {
                    const mid = m.id;
                    const labelIds = m.labelIds || [];
                    const isRead = !labelIds.includes("UNREAD");
                    const isStarred = labelIds.includes("STARRED");
                    const isImportant = labelIds.includes("IMPORTANT");

                    try {
                        await callTool("update_data", {
                            schema: "gmail",
                            table: "all_emails",
                            pk: "id",
                            where: { message_id: mid },
                            data: {
                                is_read: isRead,
                                is_starred: isStarred,
                                is_important: isImportant,
                                labels: labelIds,
                                updated_at: new Date().toISOString(),
                            },
                        });
                        console.log(`[${runId}] 🏷️ Updated label state for ${mid} → read:${isRead} starred:${isStarred} important:${isImportant}`);
                    } catch (err) {
                        console.warn(`[${runId}] ⚠️ Failed to update label state for ${mid}: ${err.message}`);
                    }
                }
            }


            console.log(`[${runId}] Thread ${thread_id.slice(0, 8)} (${tracked_tag}): ${msgs.length} messages`);

            if (!msgs.length) {
                console.log(`[${runId}] ⚠️ No messages returned for thread ${thread_id}`);
                continue;
            }

            // 3️⃣ Examine each message in the thread
            for (const msg of msgs) {
                const msgId = msg.id;
                const headers = Object.fromEntries((msg.payload?.headers || []).map((h) => [h.name, h.value]));
                const from = headers["From"] || "";

                // Skip own messages
                if (!msgId || from.toLowerCase().includes(MY_EMAIL.toLowerCase())) continue;

                // Check if this message already exists
                const existing = await callTool("query_table", {
                    schema: "gmail",
                    table: "all_emails",
                    select: ["message_id", "is_reply_to_tracked", "notified_at"],
                    where: { message_id: { eq: msgId } },
                    limit: 1,
                });

                const existingMsg = parseToolResponse(existing)?.[0];

                if (!existingMsg) {
                    console.log(`[${runId}] 🆕 New message ${msgId} in tracked thread (will be ingested on next sync)`);
                    newRepliesFound++;
                } else if (existingMsg.is_reply_to_tracked && !existingMsg.notified_at) {
                    console.log(`[${runId}] 📬 Unnotified reply ${msgId} found (will be picked up by notify cron)`);
                    newRepliesFound++;
                }
            }

            // 4️⃣ Update last_checked_at for the tracked email
            await callTool("update_data", {
                schema: "gmail",
                table: "all_emails",
                pk: "id",
                where: { message_id: message_id },
                data: { last_checked_at: new Date().toISOString() },
            });
            console.log(`[${runId}] ⏱️ Updated last_checked_at for ${tracked_tag}`);

            await new Promise((r) => setTimeout(r, 150)); // gentle rate limit
        } catch (err) {
            console.error(`[${runId}] ❌ Error processing thread ${thread_id}: ${err.message}`);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* 🧩 Reconciliation: flag replies that arrived in same ingestion batch   */
    /* ---------------------------------------------------------------------- */
    try {
        // Step 1: Fetch all tracked thread_ids
        const trackedThreadsRes = await callTool("query_table", {
            schema: "gmail",
            table: "all_emails",
            select: ["thread_id"],
            where: { is_tracked: { eq: true } },
            limit: 500,
        });

        const trackedThreads = (parseToolResponse(trackedThreadsRes) || [])
            .map((r) => r.thread_id)
            .filter(Boolean);

        if (trackedThreads.length === 0) {
            console.log(`[${runId}] ℹ️ No tracked threads found for reconciliation`);
        } else {
            // Step 2: Get IDs of rows to update
            const rowsToUpdateRes = await callTool("query_table", {
                schema: "gmail",
                table: "all_emails",
                select: ["id"],
                where: {
                    thread_id: { in: trackedThreads },
                    from_email: { neq: MY_EMAIL },
                    is_reply_to_sent: false,
                },
                limit: 500,
            });

            const idsToUpdate = (parseToolResponse(rowsToUpdateRes) || [])
                .map(r => r.id)
                .filter(Boolean);

            if (idsToUpdate.length > 0) {
                // Step 3: Update by ID
                await callTool("update_data", {
                    schema: "gmail",
                    table: "all_emails",
                    pk: "id",
                    where: { id: { in: idsToUpdate } },
                    data: { is_reply_to_sent: true, is_reply_to_tracked: true },
                });
                console.log(`[${runId}] 🔄 Reconciled ${idsToUpdate.length} replies in ${trackedThreads.length} tracked threads`);
            } else {
                console.log(`[${runId}] ℹ️ No replies to reconcile`);
            }
        }
    } catch (err) {
        console.error(`[${runId}] ⚠️ Reconciliation update failed: ${err.message}`);
    }

    console.log(`[${new Date().toISOString()}] ✅ [${runId}] Reply check complete: ${newRepliesFound} new/unnotified replies found`);
}

/* -------------------------------------------------------------------------- */
/* 🏁 CLI Entrypoint                                                          */
/* -------------------------------------------------------------------------- */
pullGmailReplies().catch((err) => {
    console.error(`[pullGmailReplies] Fatal error: ${err.message}`);
    process.exitCode = 1;
});
