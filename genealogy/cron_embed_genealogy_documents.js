// /opt/supabase-mcp/custom/genealogy/cron_embed_genealogy_documents.js
// 🧠 Genealogy Documents Embedding Cron
// ✅ Embeds documents where embedding IS NULL
// ✅ Runs every 1-2 minutes for near-instant re-embedding

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import fs from "fs";
import crypto from "crypto";
import { callTool } from "../gmail/mcp.js";

const QUOTA_FLAG = '/tmp/openai_quota_exceeded';

/* -------------------------------------------------------------------------- */
/* 🧠 Helpers                                                                 */
/* -------------------------------------------------------------------------- */

/** Parse http_fetch tool response — returns the .data field (the actual HTTP response body) */
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

            // parsed is the OpenAI response body (or error string on non-OK responses)
            let errorObj = parsed?.error;
            if (!errorObj && typeof parsed === 'string') {
                try { errorObj = JSON.parse(parsed)?.error; } catch {}
            }
            if (errorObj?.code === 'insufficient_quota' || errorObj?.type === 'insufficient_quota') {
                fs.writeFileSync(QUOTA_FLAG, new Date().toISOString());
                throw new Error('OPENAI_QUOTA_EXCEEDED');
            }

            let vector = null;
            if (parsed?.data?.[0]?.embedding) {
                vector = parsed.data[0].embedding;
            } else if (Array.isArray(parsed?.[0]?.embedding)) {
                vector = parsed[0].embedding;
            } else if (Array.isArray(parsed?.embedding)) {
                vector = parsed.embedding;
            }

            if (Array.isArray(vector) && vector.length === 3072) {
                return vector;
            }

            console.error("[EMBED] ❌ Vector dimension mismatch. Expected 3072, got:", vector?.length || "null");
            return null;

        } catch (err) {
            if (err.message === 'OPENAI_QUOTA_EXCEEDED') throw err;
            console.error(`[EMBED RETRY ${i + 1}/${retries}] ${err.message}`);
            if (i < retries - 1) {
                await new Promise((r) => setTimeout(r, 500 * (i + 1)));
            }
        }
    }
    return null;
}

/* -------------------------------------------------------------------------- */
/* 🚀 Main Embedding Function                                                */
/* -------------------------------------------------------------------------- */

async function embedGenealogyDocuments() {
    const runId = Date.now().toString(36);
    const startedAt = new Date().toISOString();

    if (fs.existsSync(QUOTA_FLAG) && (Date.now() - fs.statSync(QUOTA_FLAG).mtimeMs) < 60 * 60 * 1000) {
        console.log(`[${startedAt}] ⛔ [${runId}] OpenAI quota flag active — skipping run`);
        return { embedded: 0, errors: 0, total: 0 };
    }

    console.log(`[${startedAt}] 🧠 [${runId}] Starting genealogy documents embedding check`);

    let embedded = 0, errors = 0;

    try {
        // Find documents where embedding IS NULL AND content IS NOT NULL
        const res = await callTool("query_table", {
            schema: "genealogy",
            table: "documents",
            pk: "id",
            select: ["id", "content", "doc_title", "doc_type"],
            where: {
                embedding: { eq: null },
                content: { neq: null }
            },
            orderBy: [{ column: "created_at", ascending: true }],  // Process oldest first
            limit: 50,  // Process 50 at a time
        });

        const documents = parseToolResponse(res) || [];

        if (documents.length === 0) {
            console.log(`[${runId}] ✅ No documents need embedding`);
            return { embedded, errors, total: 0 };
        }

        console.log(`[${runId}] 📋 Found ${documents.length} documents needing embeddings`);

        for (const doc of documents) {
            try {
                const { id, content, doc_title, doc_type } = doc;

                console.log(`[${runId}] 🧠 Embedding: "${doc_title?.slice(0, 50) || 'untitled'}" (${doc_type || 'unknown type'})...`);

                const vector = await generateEmbedding(content);

                if (Array.isArray(vector) && vector.length === 3072) {
                    await callTool("update_data", {
                        schema: "genealogy",
                        table: "documents",
                        pk: "id",
                        where: { id: id },
                        data: {
                            embedding: vector,
                            embedding_model: "text-embedding-3-large",
                            embedded_at: new Date().toISOString(),
                            content_hash: crypto.createHash('md5').update(content).digest('hex'),
                        },
                    });

                    embedded++;
                    console.log(`[${runId}] ✅ Embedded: "${doc_title?.slice(0, 50)}" (ID: ${id})`);
                } else {
                    errors++;
                    console.error(`[${runId}] ❌ Failed to embed: "${doc_title}" (ID: ${id})`);
                }

                // Rate limit: 150ms between embeddings
                await new Promise((r) => setTimeout(r, 150));

            } catch (err) {
                if (err.message === 'OPENAI_QUOTA_EXCEEDED') {
                    console.error(`[${runId}] ⛔ OpenAI quota exceeded — aborting batch`);
                    break;
                }
                console.error(`[${runId}] ❌ Error embedding document ID ${doc.id}: ${err.message}`);
                errors++;
            }
        }

        console.log(`[${runId}] ✅ Embedding complete: ${embedded} embedded, ${errors} errors, ${documents.length} total`);
        return { embedded, errors, total: documents.length };

    } catch (err) {
        console.error(`[${runId}] ❌ Fatal: ${err.message}`);
        return { embedded, errors, total: 0 };
    }
}

/* -------------------------------------------------------------------------- */
/* 🏁 CLI Entrypoint                                                          */
/* -------------------------------------------------------------------------- */
embedGenealogyDocuments()
    .then((r) => {
        console.log(`[RESULT] ${JSON.stringify(r)}`);
        process.exit(0);
    })
    .catch((e) => {
        console.error(`[FATAL] ${e.message}`);
        process.exit(1);
    });