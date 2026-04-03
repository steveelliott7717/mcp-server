// 📥 Gmail Ingestion Cron — Final Version
// ✅ Properly preserves tracking data set by tool_send_email
// ✅ Only activates tracking for SENT emails (not drafts)
// ✅ Uses conditional logic to avoid overwriting tracked emails

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import { callTool } from "./mcp.js";

/* -------------------------------------------------------------------------- */
/* 🧠 Helpers                                                                 */
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
/** Parse http_fetch tool response — returns the .data field */
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

/** Decode Gmail base64-encoded content */
function decodeBase64(data) {
    try {
        return Buffer.from(data.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
    } catch {
        return "";
    }
}

/** Extract plain text and HTML recursively */
function extractParts(parts, body = { text: "", html: "" }) {
    for (const part of parts) {
        if (part.mimeType === "text/plain" && part.body?.data) {
            body.text += decodeBase64(part.body.data);
        } else if (part.mimeType === "text/html" && part.body?.data) {
            body.html += decodeBase64(part.body.data);
        } else if (part.parts) {
            extractParts(part.parts, body);
        }
    }
    return body;
}

/** Count attachments recursively */
function countAttachments(parts) {
    let count = 0;
    for (const part of parts) {
        if (part.filename && part.filename.length > 0) count++;
        if (part.parts) count += countAttachments(part.parts);
    }
    return count;
}

/** Map Gmail labels to categories */
function extractCategory(labelIds) {
    const map = {
        CATEGORY_PERSONAL: "PRIMARY",
        CATEGORY_SOCIAL: "SOCIAL",
        CATEGORY_PROMOTIONS: "PROMOTIONS",
        CATEGORY_UPDATES: "UPDATES",
        CATEGORY_FORUMS: "FORUMS",
    };
    for (const [label, cat] of Object.entries(map)) if (labelIds.includes(label)) return cat;
    return null;
}

/** Generate embedding with retries */
async function generateEmbedding(text, retries = 3) {
    const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

    if (!OPENAI_API_KEY) {
        console.error("[EMBED] OPENAI_API_KEY not found in environment");
        return null;
    }

    for (let i = 0; i < retries; i++) {
        try {
            const res = await callTool("http_fetch", {
                url: "https://api.openai.com/v1/embeddings",
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "Authorization": `Bearer ${OPENAI_API_KEY}`,
                },
                body: JSON.stringify({
                    model: "text-embedding-3-large",
                    input: text.slice(0, 8000),
                }),
                response_type: "json",
            });

            const parsed = parseHttpFetchResponse(res);

            let vector = null;
            if (parsed?.data?.[0]?.embedding) {
                vector = parsed.data[0].embedding;
            } else if (Array.isArray(parsed?.[0]?.embedding)) {
                vector = parsed[0].embedding;
            } else if (Array.isArray(parsed?.embedding)) {
                vector = parsed.embedding;
            }

            if (Array.isArray(vector) && vector.length === 3072) {
                console.log("[EMBED] ✅ Successfully generated embedding");
                return vector;
            }

            console.error("[EMBED] ❌ Vector dimension mismatch. Expected 3072, got:", vector?.length || "null");
            return null;

        } catch (err) {
            console.error(`[EMBED RETRY ${i + 1}/${retries}] ${err.message}`);
            if (i < retries - 1) {
                await new Promise((r) => setTimeout(r, 500 * (i + 1)));
            }
        }
    }
    return null;
}

/* -------------------------------------------------------------------------- */
/* 🚀 Main Gmail Ingestion Function                                           */
/* -------------------------------------------------------------------------- */

export async function ingestGmailMessages() {
    const runId = Date.now().toString(36);
    const startedAt = new Date().toISOString();
    console.log(`[${startedAt}] 📨 [${runId}] Starting Gmail ingestion`);

    let inserted = 0, skipped = 0, errors = 0, embedded = 0, tracking_preserved = 0, followup_events_created = 0;

    try {
        console.log(`[${runId}] Fetching latest Gmail date from database...`);
        const latest = await callTool("query_table", {
            schema: "gmail",
            table: "all_emails",
            select: ["gmail_date"],
            orderBy: { column: "gmail_date", ascending: false },
            limit: 1,
        });
        const latestRows = parseToolResponse(latest) || [];
        let afterDate = null;
        if (Array.isArray(latestRows) && latestRows[0]?.gmail_date) {
            afterDate = new Date(latestRows[0].gmail_date);
            console.log(`[${runId}] Latest Gmail date: ${afterDate.toISOString()}`);
        } else {
            console.log(`[${runId}] Initial sync (no existing emails).`);
        }

        let query = "in:inbox OR in:sent";
        query += ` newer_than:2d`;

        console.log(`[${runId}] Gmail query: "${query}"`);
        const allMessages = [];
        let nextPageToken = null, page = 0;

        do {
            page++;
            const listUrl = `https://gmail.googleapis.com/gmail/v1/users/me/messages?maxResults=100&q=${encodeURIComponent(query)}${nextPageToken ? `&pageToken=${nextPageToken}` : ""}`;
            console.log(`[${runId}] Fetching page ${page}...`);

            const listRes = await callTool("http_fetch", {
                provider: "gmail",
                url: listUrl,
                method: "GET",
                response_type: "json",
            });
            const listData = parseHttpFetchResponse(listRes) || {};

            if (!listData.messages?.length) break;
            allMessages.push(...listData.messages);
            nextPageToken = listData.nextPageToken || null;
            console.log(`[${runId}] Page ${page}: ${listData.messages.length} fetched (total ${allMessages.length})`);
        } while (nextPageToken);

        if (allMessages.length === 0) {
            console.log(`[${runId}] ✅ No new messages found.`);
            return await logSync(runId, startedAt, { inserted, skipped, errors, embedded, tracking_preserved, total: 0 });
        }

        console.log(`[${runId}] Found ${allMessages.length} Gmail messages to process.`);

        const messageIds = allMessages.map((m) => m.id);
        const existingRes = await callTool("query_table", {
            schema: "gmail",
            table: "all_emails",
            select: ["message_id", "source", "body_html", "embedding"],
            where: { message_id: { in: messageIds } },
        });
        const existing = parseToolResponse(existingRes) || [];
        const existingMap = new Map(existing.map((r) => [r.message_id, r]));
        console.log(`[${runId}] ${existingMap.size} existing, ${messageIds.length - existingMap.size} new`);

        for (const msg of allMessages) {
            const id = msg.id;
            const existingStub = existingMap.get(id);

            // Skip only if complete (has body content and came from gmail_api)
            if (existingStub && existingStub.source === "gmail_api" && existingStub.body_html && existingStub.embedding) {
                skipped++;
                continue;
            }

            // If it's a stub from tool_send_email, process it to get full data
            if (existingStub && existingStub.source === "tool_send_email") {
                console.log(`[${runId}] 🔄 Processing tool_send_email stub: ${id}`);
            }

            try {
                const res = await callTool("http_fetch", {
                    provider: "gmail",
                    url: `https://gmail.googleapis.com/gmail/v1/users/me/messages/${id}?format=full`,
                    method: "GET",
                    response_type: "json",
                });

                const data = parseHttpFetchResponse(res) || {};
                if (!data.id) {
                    console.error(`[${runId}] ⚠️ Invalid message ${id}`);
                    errors++;
                    continue;
                }

                const headers = Object.fromEntries((data.payload?.headers || []).map((h) => [h.name.toLowerCase(), h.value]));
                const labelIds = data.labelIds || [];
                const threadId = data.threadId;
                const subject = headers["subject"] || "(no subject)";
                const date = headers["date"] || new Date().toISOString();
                const body = { text: "", html: "" };
                if (data.payload?.parts) extractParts(data.payload.parts, body);
                else if (data.payload?.body?.data) body.text = decodeBase64(data.payload.body.data);

                const inReplyToRaw = headers["in-reply-to"] || null;
                const inReplyTo = inReplyToRaw ? inReplyToRaw.replace(/[<>]/g, "").trim() : null;

                const referencesRaw = headers["references"] || "";
                const referenceIds = referencesRaw
                    .split(/\s+/)
                    .map(id => id.replace(/[<>]/g, "").trim())
                    .filter(Boolean);

                if (threadId && !referenceIds.includes(threadId)) {
                    referenceIds.unshift(threadId);
                }

                const agentTag = headers["x-agent-tag"] || null;
                const isTracked = agentTag !== null;

                const followupEnabled = headers["x-followup-enabled"] === "true";
                const followupDays = parseInt(headers["x-followup-days"] || "7", 10);
                const followupTime = headers["x-followup-time"] || "16:00:00";

                const messageType = labelIds.includes("SENT")
                    ? "sent"
                    : labelIds.includes("DRAFT")
                        ? "draft"
                        : "inbox";

                let isReplyToTracked = false;
                let replyToTag = null;

                if (messageType !== "sent" && threadId) {
                    try {
                        const trackedInThread = await callTool("query_table", {
                            schema: "gmail",
                            table: "all_emails",
                            select: ["tracked_tag", "message_id"],
                            where: {
                                thread_id: { eq: threadId },
                                is_tracked: { eq: true }
                            },
                            limit: 1
                        });

                        const tracked = parseToolResponse(trackedInThread)?.[0];
                        if (tracked) {
                            isReplyToTracked = true;
                            replyToTag = tracked.tracked_tag;
                            console.log(`[${runId}] 📧 Reply detected to tracked email ${replyToTag}`);
                        }
                    } catch (trackErr) {
                        console.warn(`[${runId}] ⚠️ Could not check tracked status: ${trackErr.message}`);
                    }
                }

                // 🔥 Check for existing record with tracking data
                let existingRecord = null;
                try {
                    const checkRes = await callTool("query_table", {
                        schema: "gmail",
                        table: "all_emails",
                        select: [
                            "id",
                            "tracked_tag",
                            "is_tracked",
                            "tracking_active",
                            "is_reply_to_tracked",
                            "is_reply_to_sent",
                            "opened_at",
                            "open_count",
                            "followup_enabled"
                        ],
                        where: { message_id: { eq: id } },
                        limit: 1,
                    });
                    const existing = parseToolResponse(checkRes)?.[0];
                    if (existing) {
                        existingRecord = existing;
                        console.log(`[${runId}] 🔍 Found existing record for ${id}`);
                        if (existing.is_tracked) {
                            console.log(`[${runId}] 🛡️ PROTECTED: Existing tracking data found - ${existing.tracked_tag}`);
                        }
                    }
                } catch (err) {
                    // No existing record
                }

                // 🔥 If record exists with tracking data, ONLY update non-tracking fields
                if (existingRecord && existingRecord.is_tracked) {
                    console.log(`[${runId}] 🔄 Updating non-tracking fields only for tracked email ${id}`);

                    // Update only Gmail API data fields, preserve ALL tracking fields
                    await callTool("update_data", {
                        schema: "gmail",
                        table: "all_emails",
                        pk: "id",
                        where: { message_id: id },
                        data: {
                            subject: subject,  // ← ADD THIS (decoded subject)
                            body_text: body.text || null,  // ← ADD THIS
                            body_html: body.html || null,  // ← ADD THIS
                            snippet: data.snippet || "",
                            labels: labelIds,
                            is_read: !labelIds.includes("UNREAD"),
                            is_starred: labelIds.includes("STARRED"),
                            is_important: labelIds.includes("IMPORTANT"),
                            category: extractCategory(labelIds),
                            has_attachments: data.payload?.parts?.some((p) => p.filename) || false,
                            attachment_count: countAttachments(data.payload?.parts || []),
                            raw_payload: data,
                            updated_at: new Date().toISOString(),

                            // ✅ Special case: If was a draft and now is sent, activate tracking
                            ...(messageType === "sent" && !existingRecord.tracking_active ? {
                                tracking_active: true,
                                message_type: "sent"
                            } : {})
                        },
                    });

                    tracking_preserved++;
                    console.log(`[${runId}] ✅ Updated metadata for tracked email: ${subject.slice(0, 60)} [PRESERVED: ${existingRecord.tracked_tag}]`);

                    // ✅ Create follow-up event if sent with follow-up enabled (check for duplicates first)
                    if (messageType === "sent" && existingRecord?.followup_enabled === true) {
                        try {

                            // Re-extract followup parameters from headers
                            const followupDays = parseInt(headers["x-followup-days"] || "7", 10);
                            const followupTime = headers["x-followup-time"] || "16:00:00";

                            // Check if follow-up event already exists
                            const existingEvent = await callTool("query_table", {
                                schema: "calendar",
                                table: "events",
                                select: ["id"],
                                where: {
                                    description: { like: `%${id}%` }
                                },
                                limit: 1
                            });

                            const hasEvent = parseToolResponse(existingEvent)?.length > 0;

                            if (hasEvent) {
                                console.log(`[${runId}] ⏭️ Follow-up event already exists for ${id}, skipping`);
                            } else {
                                console.log(`[${runId}] 📅 Creating follow-up calendar event (${followupDays} days)`);

                                await createFollowupEvent({
                                    runId,
                                    messageId: id,
                                    threadId,
                                    subject,
                                    to: headers["to"] || "",
                                    from: headers["from"] || "",
                                    trackingTag: agentTag,
                                    followupDays,
                                    followupTime,
                                    bodyText: body.text || "",
                                    senderTemplate: headers["x-sender-template"] || "basic",
                                    signatureTemplate: headers["x-signature-template"] || "basic"
                                });
                                followup_events_created++;
                            }
                        } catch (err) {
                            console.error(`[${runId}] ⚠️ Failed to create/check follow-up event: ${err.message}`);
                        }
                    }

                    // Continue to embedding step
                    const dbId = existingRecord.id;
                    if (!existingStub.embedding) {  // ← ADD THIS CHECK
                        await embedEmail(runId, dbId, subject, body);
                        embedded++;
                    }

                    await new Promise((r) => setTimeout(r, 100));
                    continue;
                }

                // 🔥 For NEW records, create with appropriate tracking state
                const record = {
                    message_id: id,
                    thread_id: threadId,
                    from_email: headers["from"] || "",
                    to_email: headers["to"] || "",
                    cc_email: headers["cc"] || null,
                    bcc_email: headers["bcc"] || null,
                    reply_to: headers["reply-to"] || null,
                    subject,
                    snippet: data.snippet || "",
                    gmail_date: new Date(date).toISOString(),
                    message_type: messageType,
                    body_text: body.text || null,
                    body_html: body.html || null,
                    in_reply_to: inReplyTo,
                    reference_ids: referenceIds,
                    is_reply_to_sent: isReplyToTracked,

                    // ✅ Tracking logic: Only activate for SENT emails with X-Agent-Tag
                    tracked_tag: agentTag || replyToTag,
                    is_tracked: isTracked,
                    tracking_active: isTracked && messageType === "sent", // Only active if SENT
                    is_reply_to_tracked: isReplyToTracked,
                    followup_enabled: followupEnabled,

                    labels: labelIds,
                    is_read: !labelIds.includes("UNREAD"),
                    is_starred: labelIds.includes("STARRED"),
                    is_important: labelIds.includes("IMPORTANT"),
                    category: extractCategory(labelIds),
                    has_attachments: data.payload?.parts?.some((p) => p.filename) || false,
                    attachment_count: countAttachments(data.payload?.parts || []),
                    source: "gmail_api",
                    raw_payload: data,
                };

                const insertRes = await callTool("upsert_data", {
                    schema: "gmail",
                    table: "all_emails",
                    data: record,
                    on_conflict: "message_id",
                    pk: "message_id",
                    returning: "representation",
                });

                const parsed = parseToolResponse(insertRes);
                let dbId = null;
                if (parsed?.rows && Array.isArray(parsed.rows) && parsed.rows[0]?.id) {
                    dbId = parsed.rows[0].id;
                } else if (Array.isArray(parsed) && parsed[0]?.id) {
                    dbId = parsed[0].id;
                }

                inserted++;
                const trackingLabel = isTracked && messageType === "sent" ? ` [TRACKED: ${agentTag}]` :
                    isTracked && messageType === "draft" ? ` [DRAFT TAG: ${agentTag}]` :
                        isReplyToTracked ? ` [REPLY TO: ${replyToTag}]` : "";
                console.log(`[${runId}] ✅ Upserted: ${subject.slice(0, 60)}${trackingLabel} (${id})`);

                // ✅ Create follow-up event for new sent emails with follow-up enabled
                if (messageType === "sent" && record.followup_enabled === true) {
                    try {
                        // Check if follow-up event already exists
                        const existingEvent = await callTool("query_table", {
                            schema: "calendar",
                            table: "events",
                            select: ["id"],
                            where: {
                                description: { like: `%${id}%` }
                            },
                            limit: 1
                        });

                        const hasEvent = parseToolResponse(existingEvent)?.length > 0;

                        if (hasEvent) {
                            console.log(`[${runId}] ⏭️ Follow-up event already exists for ${id}, skipping`);
                        } else {
                            console.log(`[${runId}] 📅 Creating follow-up calendar event (${followupDays} days)`);

                            await createFollowupEvent({
                                runId,
                                messageId: id,
                                threadId,
                                subject,
                                to: headers["to"] || "",
                                from: headers["from"] || "",
                                trackingTag: agentTag,
                                followupDays,
                                followupTime,
                                bodyText: body.text || "",
                                senderTemplate: headers["x-sender-template"] || "basic",
                                signatureTemplate: headers["x-signature-template"] || "basic"
                            });
                            followup_events_created++;
                        }
                    } catch (err) {
                        console.error(`[${runId}] ⚠️ Failed to create/check follow-up event: ${err.message}`);
                    }
                }

                // Embedding
                if (dbId) {
                    if (!existingStub || !existingStub.embedding) {  // ← ADD THIS
                        await embedEmail(runId, dbId, subject, body);
                        embedded++;
                    }
                }

                await new Promise((r) => setTimeout(r, 100));
            } catch (err) {
                console.error(`[${runId}] ❌ Error processing ${id}: ${err.message}`);
                errors++;
            }
        }

        const summary = { inserted, skipped, errors, embedded, tracking_preserved, followup_events_created, total: allMessages.length };
        await logSync(runId, startedAt, summary);

        console.log(`[${runId}] ✅ Sync complete: ${inserted} new, ${embedded} embedded, ${tracking_preserved} tracking preserved, ${followup_events_created} follow-ups created, ${skipped} skipped, ${errors} errors.`);
        return summary;
    } catch (err) {
        console.error(`[${runId}] ❌ Fatal: ${err.message}`);
        await logSync(runId, startedAt, { inserted, skipped, errors, embedded, tracking_preserved, total: 0, error_message: err.message });
    }
}

/* -------------------------------------------------------------------------- */
/* 📅 Helper: Create Follow-Up Calendar Event                                 */
/* -------------------------------------------------------------------------- */
async function createFollowupEvent({
    runId,
    messageId,
    threadId,
    subject,
    to,
    from,
    trackingTag,
    followupDays,
    followupTime,
    bodyText,
    senderTemplate,
    signatureTemplate
}) {
    // Calculate follow-up date
    const now = new Date();
    const followupDate = new Date(now);
    followupDate.setDate(followupDate.getDate() + followupDays);

    // Format as YYYY-MM-DDTHH:MM:SS in Chicago time, then convert to UTC
    const dateStr = followupDate.toISOString().split('T')[0];
    const chicagoDateTime = new Date(`${dateStr}T${followupTime}-06:00`); // CST offset
    const startTimeUTC = chicagoDateTime.toISOString();

    const followupTitle = `Follow-up: ${subject}`;
    const followupDescription = `**Original Email Context**
Sent: ${now.toISOString()}
To: ${to}
From: ${from}
Subject: ${subject}

**Thread Information**
Thread ID: ${threadId}
Message ID: ${messageId}
Tracking Tag: ${trackingTag || 'none'}

**Reply Instructions for GPT**
To send a follow-up in this thread, use:

\`\`\`json
{
  "name": "send_email",
  "arguments": {
    "to": "${to}",
    "subject": "Re: ${subject}",
    "body": "[Your follow-up message here]",
    "thread_id": "${threadId}",
    "in_reply_to": "${messageId}",
    "sender_template": "${senderTemplate}",
    "signature_template": "${signatureTemplate}",
    "track": true,
    "mode": "send"
  }
}
\`\`\`

**Original Message Preview**
${bodyText.substring(0, 500)}${bodyText.length > 500 ? '...' : ''}

**Suggested Follow-up Actions**
- Check if reply received (search thread_id: ${threadId})
- Send gentle reminder if no response
- Provide additional information if needed
`.trim();

    const eventRes = await callTool("insert_data", {
        schema: "calendar",
        table: "events",
        data: {
            title: followupTitle,
            description: followupDescription,
            start_time: startTimeUTC,
            end_time: null,
            location: `Email: ${to}`,
            notify_before_event: false,
            notify_on_the_day: false,
            notify_at_start: true,
            created_at: now.toISOString(),
            updated_at: now.toISOString(),
        },
        returning: "representation",
    });

    const parsed = parseToolResponse(eventRes);
    const eventId = parsed?.[0]?.id || parsed?.rows?.[0]?.id;

    if (eventId) {
        console.log(`[${runId}] ✅ Follow-up event created (ID: ${eventId}) for ${followupDays} days from now`);
    }

    return eventId;
}

/* -------------------------------------------------------------------------- */
/* 🧠 Helper: Embed Email                                                     */
/* -------------------------------------------------------------------------- */
async function embedEmail(runId, dbId, subject, body) {
    function cleanEmailText(text = "") {
        return text
            .replace(/\r?\n/g, " ")
            .replace(/<[^>]+>/g, " ")
            .replace(/\b(unsubscribe|view in browser|confidentiality notice|do not reply|powered by|sent from my iphone)\b.*/gi, "")
            .replace(/--+\s*forwarded message\s*--+.*/gi, "")
            .replace(/on\s+\w{3,9}\s+\d{1,2}.*wrote:.*/gi, "")
            .replace(/\s{2,}/g, " ")
            .trim();
    }

    const rawText = [subject, body.text || body.html?.replace(/<[^>]+>/g, "")]
        .filter(Boolean)
        .join(" — ");

    const cleanedText = cleanEmailText(rawText).slice(0, 8000);

    if (cleanedText.length > 0) {
        const vector = await generateEmbedding(cleanedText);
        if (Array.isArray(vector) && vector.length === 3072) {
            try {
                await callTool("update_data", {
                    schema: "gmail",
                    table: "all_emails",
                    pk: "id",
                    where: { id: dbId },
                    data: {
                        embedding: vector,
                        embedding_model: "text-embedding-3-large",
                        embedded_at: new Date().toISOString(),
                        content: cleanedText
                    },
                });
                console.log(`[${runId}] 🧠 Embedded clean text: ${subject.slice(0, 60)}`);
            } catch (err) {
                console.error(`[${runId}] ❌ Embedding update failed: ${err.message}`);
            }
        }
    }
}

/* -------------------------------------------------------------------------- */
/* 🧾 Logging Function                                                       */
/* -------------------------------------------------------------------------- */
async function logSync(runId, startedAt, summary) {
    try {
        await callTool("insert_data", {
            schema: "gmail",
            table: "sync_logs",
            data: {
                run_id: runId,
                run_started_at: startedAt,
                inserted: summary.inserted,
                skipped: summary.skipped,
                errors: summary.errors,
                total: summary.total,
                completed_at: new Date().toISOString(),
                error_message: summary.error_message || null,
            },
        });
        console.log(`[${runId}] 🧾 Sync log written.`);
    } catch (err) {
        console.error(`[${runId}] ⚠️ Failed to log sync: ${err.message}`);
    }
    return summary;
}

/* -------------------------------------------------------------------------- */
/* 🏁 CLI Runner                                                             */
/* -------------------------------------------------------------------------- */
if (import.meta.url === `file://${process.argv[1]}`) {
    ingestGmailMessages()
        .then((r) => {
            console.log("Final result:", r);
            process.exit(0);
        })
        .catch((e) => {
            console.error("Fatal:", e);
            process.exit(1);
        });
}