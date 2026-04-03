// /opt/supabase-mcp/custom/consulting/cron_embed_consulting_chunks.js
// 🧠 Consulting Chunks Embedding Cron
// ✅ Embeds chunks where embedding IS NULL
// ✅ Runs every 1-2 minutes for near-instant re-embedding

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import { callTool } from "../gmail/mcp.js";

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
            return parsed.data || parsed;
        } catch {
            return null;
        }
    }

    if (content.json) return content.json.data || content.json;
    if (typeof content === "object" && !content.type) return content.data || content;
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

            const parsed = parseToolResponse(res);

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

async function embedConsultingChunks() {
    const runId = Date.now().toString(36);
    const startedAt = new Date().toISOString();
    console.log(`[${startedAt}] 🧠 [${runId}] Starting consulting chunks embedding check`);

    let embedded = 0, errors = 0;

    try {
        // Find chunks where embedding IS NULL
        const res = await callTool("query_table", {
            schema: "professional_profile",
            table: "consulting_chunks",
            select: ["id", "content", "section_title", "chunk_index"],
            where: {
                embedding: { eq: null }
            },
            limit: 100,  // Process 100 at a time
        });

        const chunks = parseToolResponse(res) || [];

        if (chunks.length === 0) {
            console.log(`[${runId}] ✅ No chunks need embedding`);
            return { embedded, errors, total: 0 };
        }

        console.log(`[${runId}] Found ${chunks.length} chunks needing embeddings`);

        for (const chunk of chunks) {
            try {
                const { id, content, section_title, chunk_index } = chunk;

                console.log(`[${runId}] 🧠 Embedding chunk ${chunk_index}: ${section_title?.slice(0, 40) || 'untitled'}...`);

                const vector = await generateEmbedding(content);

                if (Array.isArray(vector) && vector.length === 3072) {
                    await callTool("update_data", {
                        schema: "professional_profile",
                        table: "consulting_chunks",
                        where: { id: { eq: id } },
                        data: {
                            embedding: vector,
                            embedding_model: "text-embedding-3-large",
                            embedded_at: new Date().toISOString(),
                        },
                    });

                    embedded++;
                    console.log(`[${runId}] ✅ Embedded chunk ${chunk_index}`);
                } else {
                    errors++;
                    console.error(`[${runId}] ❌ Failed to embed chunk ${chunk_index}`);
                }

                // Rate limit: 150ms between embeddings
                await new Promise((r) => setTimeout(r, 150));

            } catch (err) {
                console.error(`[${runId}] ❌ Error embedding chunk: ${err.message}`);
                errors++;
            }
        }

        console.log(`[${runId}] ✅ Embedding complete: ${embedded} embedded, ${errors} errors`);
        return { embedded, errors, total: chunks.length };

    } catch (err) {
        console.error(`[${runId}] ❌ Fatal: ${err.message}`);
        return { embedded, errors, total: 0 };
    }
}

/* -------------------------------------------------------------------------- */
/* 🏁 CLI Entrypoint                                                          */
/* -------------------------------------------------------------------------- */
embedConsultingChunks()
    .then((r) => {
        console.log(`[RESULT] ${JSON.stringify(r)}`);
        process.exit(0);
    })
    .catch((e) => {
        console.error(`[FATAL] ${e.message}`);
        process.exit(1);
    });