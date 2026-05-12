// /opt/supabase-mcp/custom/wg/cron_wg_messages.mjs
// Checks WG-Gesucht inbox, saves all messages to wg_messages table,
// and push-notifies on new received messages.

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import path from "node:path";
import { fileURLToPath } from "node:url";
import { callTool } from "./mcp.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_FILE = path.join(__dirname, "session.json");

const PLAYWRIGHT_EXECUTABLE = "/opt/supabase-mcp/custom/ms-playwright/chromium_headless_shell-1217/chrome-headless-shell-linux64/chrome-headless-shell";
const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";
const NACHRICHTEN_URL = "https://www.wg-gesucht.de/nachrichten.html";

function log(msg) { console.log(`[wg_messages] ${msg}`); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function normalizeTitle(value = "") {
    return String(value)
        .toLowerCase()
        .replace(/[\u2010-\u2015]/g, "-")
        .replace(/[^\p{L}\p{N}\s-]+/gu, " ")
        .replace(/\s+/g, " ")
        .trim();
}

function extractListingIdFromUrl(url = "") {
    const match = String(url).match(/\.([0-9]{6,})\.html/i);
    return match ? match[1] : null;
}

function parseRelativeTime(str) {
    if (!str) return null;
    const s = str.trim();
    const now = new Date();
    const m = s.match(/vor\s+(\d+)\s+(Minute[n]?|Stunde[n]?|Tag[e]?|Woche[n]?)/i);
    if (m) {
        const n = parseInt(m[1]);
        const unit = m[2].toLowerCase();
        if (unit.startsWith("minute")) now.setMinutes(now.getMinutes() - n);
        else if (unit.startsWith("stunde")) now.setHours(now.getHours() - n);
        else if (unit.startsWith("tag")) now.setDate(now.getDate() - n);
        else if (unit.startsWith("woche")) now.setDate(now.getDate() - n * 7);
        return now.toISOString();
    }
    const today = s.match(/heute[,\s]+(\d{1,2}):(\d{2})/i);
    if (today) { now.setHours(+today[1], +today[2], 0, 0); return now.toISOString(); }
    const yesterday = s.match(/gestern[,\s]+(\d{1,2}):(\d{2})/i);
    if (yesterday) { now.setDate(now.getDate() - 1); now.setHours(+yesterday[1], +yesterday[2], 0, 0); return now.toISOString(); }
    const full = s.match(/(\d{2})\.(\d{2})\.(\d{4})[,\s]+(\d{2}):(\d{2})/);
    if (full) return new Date(`${full[3]}-${full[2]}-${full[1]}T${full[4]}:${full[5]}:00`).toISOString();
    return null;
}

async function getExistingMessageIds() {
    const res = await callTool("query_table", {
        schema: "finance",
        table: "wg_messages",
        select: "message_id",
        limit: 2000,
    });
    const content = res?.result?.content || res?.content || [];
    const text = content[0]?.text;
    if (!text) return new Set();
    let parsed; try { parsed = JSON.parse(text); } catch { return new Set(); }
    const rows = parsed.rows ?? parsed.data ?? (Array.isArray(parsed) ? parsed : []);
    return new Set(rows.map(r => r.message_id));
}

const GENERIC_TITLE_RE = /^(\d-?zimmer-?wohnung|wohnung|apartment|studio|zimmer|1-zimmer-wohnung|2-zimmer-wohnung|3-zimmer-wohnung)$/i;

async function fetchListingCandidates() {
    const res = await callTool("query_table", {
        schema: "finance",
        table: "wg_gesucht_listings",
        select: "listing_id,title,district,message_sent,message_sent_at",
        limit: 500,
    });
    const content = res?.result?.content || res?.content || [];
    const text = content[0]?.text;
    if (!text) return [];
    let parsed; try { parsed = JSON.parse(text); } catch { return []; }
    return parsed.rows ?? parsed.data ?? (Array.isArray(parsed) ? parsed : []);
}

function resolveListingId(adTitle, adUrl, approxTime, candidates) {
    // 1. Exact listing ID extracted from URL — most reliable
    const urlId = extractListingIdFromUrl(adUrl);
    if (urlId) return { listingId: urlId, source: "url-id" };

    if (!adTitle || !candidates.length) return { listingId: null, source: null };

    const needle = normalizeTitle(adTitle);
    const needleWords = needle.split(/\s+/).filter(w => w.length > 4);

    const scored = candidates.map(row => {
        if (!row.title) return null;
        const hay = normalizeTitle(row.title);
        let score = 0;
        let source = "";

        if (needle === hay) {
            score = 100; source = "exact-title";
        } else if (needle.length > 20 && (needle.includes(hay.slice(0, 24)) || hay.includes(needle.slice(0, 24)))) {
            score = 60; source = "substring-title";
        } else if (needleWords.length > 0) {
            const hayWords = hay.split(/\s+/);
            const overlap = needleWords.filter(w => hayWords.some(hw => hw.includes(w))).length;
            const ratio = overlap / needleWords.length;
            if (ratio >= 0.4) { score = Math.round(ratio * 50); source = "word-overlap"; }
        }

        if (score === 0) return null;

        // Prefer listings where a message was actually sent
        if (row.message_sent) score += 20;

        // Prefer listings whose sent time is close to the thread's approximate time
        if (approxTime && row.message_sent_at) {
            const diffH = Math.abs(new Date(approxTime) - new Date(row.message_sent_at)) / 3600000;
            if (diffH < 1) score += 30;
            else if (diffH < 6) score += 15;
            else if (diffH < 24) score += 5;
        }

        return { listing_id: row.listing_id, title: row.title, score, source };
    }).filter(Boolean).sort((a, b) => b.score - a.score);

    if (!scored.length) return { listingId: null, source: null };

    const best = scored[0];

    // Refuse to guess when title is generic and runner-up is close in score
    if (scored.length > 1 && scored[1].score >= best.score - 10) {
        const norm = normalizeTitle(best.title).replace(/\s+/g, "-");
        if (GENERIC_TITLE_RE.test(norm)) {
            log(`  listing_id ambiguous for "${adTitle}" — leaving null`);
            return { listingId: null, source: "ambiguous" };
        }
    }

    return { listingId: best.listing_id, source: `${best.source}(score=${best.score})` };
}

async function backfillConversationListing(thread, listingId) {
    if (!listingId || !thread?.conversationId) return;
    try {
        const existing = await callTool("query_table", {
            schema: "finance",
            table: "wg_messages",
            select: "message_id,listing_id",
            where: { conversation_id: { op: "eq", value: thread.conversationId } },
            limit: 100,
        });
        const content = existing?.result?.content || existing?.content || [];
        const text = content[0]?.text;
        let parsed; try { parsed = JSON.parse(text || "{}"); } catch { parsed = {}; }
        const rows = parsed.rows ?? parsed.data ?? (Array.isArray(parsed) ? parsed : []);
        const targets = rows.filter(r => !r.listing_id).map(r => ({ message_id: r.message_id, listing_id: listingId }));
        if (!targets.length) return;

        await callTool("update_data", {
            schema: "finance",
            table: "wg_messages",
            pk: ["message_id"],
            data: targets,
        });
        log(`  → Backfilled listing_id ${listingId} for ${targets.length} message(s) in conversation ${thread.conversationId}`);
    } catch (e) {
        log(`  → Backfill skipped: ${e.message}`);
    }
}

async function notify(senderName, preview) {
    const apiToken = process.env.PUSHOVER_TOKEN_WG || process.env.PUSHOVER_API_TOKEN;
    const userKey  = process.env.PUSHOVER_USER_KEY;
    if (!apiToken || !userKey) {
        log("  → Push skipped: PUSHOVER_TOKEN_WG / PUSHOVER_USER_KEY not set");
        return;
    }
    try {
        await callTool("notify_push", {
            provider: "pushover",
            api_token: apiToken,
            user_key: userKey,
            title: `WG reply: ${senderName}`,
            body: preview.slice(0, 200),
        });
        log(`  → Push notification sent`);
    } catch (e) {
        log(`  → Push notification failed: ${e.message}`);
    }
}

async function main() {
    log(`Starting at ${new Date().toISOString()}`);

    process.env.PLAYWRIGHT_BROWSERS_PATH = "/opt/supabase-mcp/custom/ms-playwright";
    const { chromium } = await import("playwright");

    const browser = await chromium.launch({
        headless: true,
        executablePath: PLAYWRIGHT_EXECUTABLE,
        args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
    });

    try {
        const context = await browser.newContext({
            storageState: SESSION_FILE,
            userAgent: USER_AGENT,
            viewport: { width: 1280, height: 900 },
        });
        const page = await context.newPage();

        const existingIds = await getExistingMessageIds();
        log(`Loaded ${existingIds.size} known message IDs`);

        // ── 1. Parse thread list ──────────────────────────────────────
        await page.goto(NACHRICHTEN_URL, { waitUntil: "domcontentloaded", timeout: 30000 });
        if (/cuba\.html/i.test(page.url()) || /login|signin/i.test(page.url())) {
            throw new Error("Session expired or rate limited");
        }

        const threads = await page.evaluate(() =>
            [...document.querySelectorAll(".conversation_list_item:not(.conversation_list_teaser)")].map(el => {
                const panel = el.querySelector("[data-conversation_id]");
                const linkEl = el.querySelector("a.link-conversation-list");
                const adLink = el.querySelector('.conversations_list_ad_title a[href*=".html"]');
                // Strip the #anchor so we load from the top of the conversation
                const url = linkEl?.href?.replace(/#.*$/, "") || null;
                return {
                    conversationId: panel?.dataset?.conversation_id || null,
                    url,
                    senderName: el.querySelector(".list_item_public_name")?.textContent?.trim() || "",
                    adTitle: el.querySelector(".conversations_list_ad_title")?.textContent?.trim() || "",
                    adUrl: adLink?.href?.replace(/#.*$/, "") || null,
                    timeText: el.querySelector("[class*='time'], [class*='date']")?.textContent?.trim() || "",
                };
            })
        );

        log(`Found ${threads.length} conversation(s)`);
        if (!threads.length) { log("Nothing to process"); return; }

        const listingCandidates = await fetchListingCandidates();
        log(`Loaded ${listingCandidates.length} listing candidates for matching`);

        let newReceived = 0;

        // ── 2. Process each conversation ──────────────────────────────
        for (const thread of threads) {
            if (!thread.conversationId || !thread.url) continue;
            log(`[${thread.conversationId}] ${thread.senderName} — "${thread.adTitle.slice(0, 60)}"`);

            const approxTime = parseRelativeTime(thread.timeText);
            let { listingId, source } = resolveListingId(thread.adTitle, thread.adUrl, approxTime, listingCandidates);
            if (listingId) log(`  listing_id matched: ${listingId} via ${source}`);

            await page.goto(thread.url, { waitUntil: "domcontentloaded", timeout: 20000 });
            await sleep(600);

            if (!listingId) {
                const threadAdUrl = await page.evaluate(() => {
                    const adArea = document.querySelector('.conversation_ad_title, [class*="conversation_ad"], .panel-heading');
                    if (adArea) {
                        const link = adArea.querySelector('a[href*=".html"]');
                        if (link && /\.\d{6,}\.html/i.test(link.href)) return link.href.replace(/#.*$/, "");
                    }
                    const link = [...document.querySelectorAll('a[href*=".html"]')]
                        .find(a => /\.\d{6,}\.html$/i.test(new URL(a.href, location.href).pathname)
                                && !/nachrichten|login|cuba|mein-wg/i.test(a.href));
                    return link?.href?.replace(/#.*$/, "") || null;
                });
                ({ listingId, source } = resolveListingId(thread.adTitle, threadAdUrl, approxTime, listingCandidates));
                if (listingId) log(`  listing_id resolved from thread: ${listingId} via ${source}`);
            }

            await backfillConversationListing(thread, listingId);

            // Parse all messages in the conversation
            const messages = await page.evaluate(() =>
                [...document.querySelectorAll(".last_message_selector")].map(wrapper => {
                    const msgDiv = wrapper.querySelector(".message");
                    if (!msgDiv) return null;
                    const isSent = msgDiv.classList.contains("my_message");
                    const msgId = msgDiv.id?.replace("last_message_id_", "") || null;
                    const body = msgDiv.querySelector(".message_content, .message_text")?.textContent?.trim() || "";
                    const timeEl = wrapper.querySelector("[class*='timestamp'], time");
                    const timeText = timeEl?.textContent?.replace(/\s+/g, " ").trim() || "";
                    return { msgId, isSent, body, timeText };
                }).filter(Boolean)
            );

            log(`  ${messages.length} message(s) in thread`);

            for (const msg of messages) {
                const messageId = `${thread.conversationId}_${msg.msgId || messages.indexOf(msg)}`;
                if (existingIds.has(messageId)) continue;

                const direction = msg.isSent ? "sent" : "received";
                const sentAt = parseRelativeTime(msg.timeText) || new Date().toISOString();

                const row = {
                    conversation_id: thread.conversationId,
                    message_id: messageId,
                    direction,
                    sender_name: msg.isSent ? "Steven Elliott" : thread.senderName,
                    message_body: msg.body.slice(0, 4000),
                    sent_at: sentAt,
                    notified: false,
                };
                if (listingId) row.listing_id = listingId;

                try {
                    await callTool("insert_data", {
                        schema: "finance",
                        table: "wg_messages",
                        data: row,
                    });
                    existingIds.add(messageId);
                    log(`  → Saved ${direction} message ${messageId}`);

                    if (direction === "received") {
                        newReceived++;
                        await notify(thread.senderName, msg.body);
                        // Mark notified
                        await callTool("update_data", {
                            schema: "finance",
                            table: "wg_messages",
                            pk: ["message_id"],
                            data: [{ message_id: messageId, notified: true }],
                        });
                    }
                } catch (e) {
                    // Duplicate key = already saved, skip silently
                    if (/duplicate|unique/i.test(e.message)) {
                        existingIds.add(messageId);
                    } else {
                        log(`  → Insert error: ${e.message}`);
                    }
                }
            }

            await sleep(800);
        }

        log(`Done — ${newReceived} new received message(s) at ${new Date().toISOString()}`);

    } finally {
        await browser.close();
    }
}

main().catch(err => {
    log(`Fatal: ${err.message}`);
    process.exit(1);
});
