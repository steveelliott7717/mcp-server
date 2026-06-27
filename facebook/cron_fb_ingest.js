// /opt/supabase-mcp/custom/facebook/cron_fb_ingest.js
// Ingests Facebook Marketplace conversations and messages into finance.fb_messages

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import { callTool } from "./mcp.js";

const MY_PAGE_ID = "1116933241495616";

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

async function ingestFacebookMessages() {
    const runId = Date.now().toString(36);
    const startedAt = new Date().toISOString();
    console.log(`[${startedAt}] [${runId}] Starting Facebook ingest`);

    let inserted = 0, skipped = 0, errors = 0;

    try {
        // 1. Fetch all conversations
        const convRes = await callTool("facebook_messages", {
            action: "list_conversations",
            page_id: MY_PAGE_ID,
            limit: 100,
        });

        const convData = parseToolResponse(convRes);
        const conversations = convData?.conversations || [];

        if (!conversations.length) {
            console.log(`[${runId}] No conversations found`);
            return;
        }

        console.log(`[${runId}] Found ${conversations.length} conversations`);

        for (const conv of conversations) {
            const fbConversationId = conv.id;
            const snippet = conv.snippet || "";
            const threadUpdatedAt = conv.updated_time ? new Date(conv.updated_time).toISOString() : null;
            const participants = conv.participants?.data || [];
            const participantNames = participants.map((p) => p.name);
            const participantIds = participants.map((p) => p.id);

            // Check if any existing row in this thread has a listing_id
            let existingListingId = null;
            try {
                const listingCheck = await callTool("query_table", {
                    schema: "finance",
                    table: "fb_messages",
                    select: ["listing_id"],
                    where: {
                        fb_conversation_id: { eq: fbConversationId },
                        listing_id: { not_null: true },
                    },
                    limit: 1,
                });
                existingListingId = parseToolResponse(listingCheck)?.[0]?.listing_id || null;
            } catch { /* no existing rows yet */ }

            // 2. Fetch messages in this thread
            let messages = [];
            try {
                const threadRes = await callTool("facebook_messages", {
                    action: "get_thread",
                    conversation_id: fbConversationId,
                    limit: 100,
                });
                const threadData = parseToolResponse(threadRes);
                messages = threadData?.messages || [];
            } catch (err) {
                console.error(`[${runId}] Failed to fetch thread ${fbConversationId}: ${err.message}`);
                errors++;
                continue;
            }

            if (!messages.length) continue;

            // 3. Get existing message IDs for this thread to skip
            const messageIds = messages.map((m) => m.id);
            let existingIds = new Set();
            try {
                const existingRes = await callTool("query_table", {
                    schema: "finance",
                    table: "fb_messages",
                    select: ["fb_message_id"],
                    where: { fb_message_id: { in: messageIds } },
                });
                const existing = parseToolResponse(existingRes) || [];
                existingIds = new Set(existing.map((r) => r.fb_message_id));
            } catch { /* proceed without skip check */ }

            console.log(`[${runId}] Thread ${fbConversationId}: ${messages.length} messages, ${existingIds.size} existing`);

            // 4. Insert new messages
            for (const msg of messages) {
                if (existingIds.has(msg.id)) {
                    skipped++;
                    continue;
                }

                const isFromMe = msg.from?.id === MY_PAGE_ID;

                try {
                    await callTool("insert_data", {
                        schema: "finance",
                        table: "fb_messages",
                        data: {
                            fb_message_id: msg.id,
                            fb_conversation_id: fbConversationId,
                            listing_id: existingListingId,
                            from_name: msg.from?.name || null,
                            from_fb_id: msg.from?.id || null,
                            body: msg.message || null,
                            sent_at: msg.created_time ? new Date(msg.created_time).toISOString() : null,
                            is_from_me: isFromMe,
                            thread_snippet: snippet,
                            thread_participant_names: participantNames,
                            thread_participant_ids: participantIds,
                            thread_updated_at: threadUpdatedAt,
                            source: "facebook_api",
                        },
                    });
                    inserted++;
                    console.log(`[${runId}] Inserted: [${isFromMe ? "ME" : msg.from?.name}] ${(msg.message || "").slice(0, 60)}`);
                } catch (err) {
                    console.error(`[${runId}] Failed to insert ${msg.id}: ${err.message}`);
                    errors++;
                }
            }

            // 5. Update thread metadata on existing rows
            try {
                await callTool("update_data", {
                    schema: "finance",
                    table: "fb_messages",
                    pk: "id",
                    where: { fb_conversation_id: { eq: fbConversationId } },
                    data: {
                        thread_snippet: snippet,
                        thread_updated_at: threadUpdatedAt,
                        updated_at: new Date().toISOString(),
                    },
                });
            } catch { /* non-critical */ }

            await new Promise((r) => setTimeout(r, 200));
        }

        console.log(`[${runId}] Done: ${inserted} inserted, ${skipped} skipped, ${errors} errors`);
    } catch (err) {
        console.error(`[${runId}] Fatal: ${err.message}`);
    }
}

if (import.meta.url === `file://${process.argv[1]}`) {
    ingestFacebookMessages()
        .then(() => process.exit(0))
        .catch((e) => { console.error(e); process.exit(1); });
}
