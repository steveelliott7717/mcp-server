// /opt/supabase-mcp/custom/poshmark/cron_poshmark_notify.js
// Scrapes Poshmark news (comments + orders), saves new items to
// finance.poshmark_activity, sends push notifications for each new item.

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import { chromium } from "playwright";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";
import { callTool } from "./mcp.js";

process.env.PLAYWRIGHT_BROWSERS_PATH = "/opt/supabase-mcp/custom/ms-playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_FILE = path.join(__dirname, "session.json");
const TABS = ["comment", "offers", "order"];

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function makeId(...parts) {
    return crypto.createHash("sha256").update(parts.join(":")).digest("hex").slice(0, 24);
}

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
        return j;
    }
    return null;
}

async function scrapeTab(page, tab) {
    await page.goto(`https://poshmark.com/news/${tab}`, { waitUntil: "networkidle", timeout: 30000 });

    const isLoggedOut = await page.$('a[href="/login"]');
    if (isLoggedOut) throw new Error("Session expired — run poshmark_login.js to refresh");

    try {
        await page.waitForSelector('a[data-et-name="feed_unit"]', { timeout: 10000 });
    } catch {
        return [];
    }

    return page.$$eval('a[data-et-name="feed_unit"]', (anchors) =>
        anchors.map(a => {
            const href = a.getAttribute("href") || "";
            const storyType = a.getAttribute("data-et-prop-story_type") || "";

            // Listing ID: long alphanumeric segment at end of /listing/... URL
            const listingIdMatch = href.match(/-([a-zA-Z0-9]{16,})$/);
            const listingId = listingIdMatch ? listingIdMatch[1] : "";

            // Order ID: /order/sales/ORDER_ID
            const orderIdMatch = href.match(/\/order\/sales\/([^/?]+)/);
            const orderId = orderIdMatch ? orderIdMatch[1] : "";

            // Listing title: try URL slug first, then thumbnail img alt as fallback
            let listingTitle = "";
            if (listingId && href.includes("/listing/")) {
                const slug = href.split("/listing/")[1] || "";
                listingTitle = slug.replace(/-[a-zA-Z0-9]{16,}$/, "").replace(/-/g, " ").trim();
            }
            if (!listingTitle || listingTitle === ":post title") {
                const imgEl = a.querySelector("img[alt]");
                listingTitle = imgEl ? (imgEl.getAttribute("alt") || "").trim() : "";
            }

            // Actor username
            const actorLink = a.querySelector('a[href*="/closet/"]');
            const username = actorLink
                ? (actorLink.getAttribute("href") || "").replace("/closet/", "").replace(/^\//, "")
                : "";

            // Message text: only the first <p> inside .news-feed__message (excludes timestamp <p>)
            const msgP = a.querySelector(".news-feed__message p:first-child");
            const rawText = msgP ? msgP.innerText.trim() : "";
            const content = rawText
                .replace(/^.*?(?:commented|liked|shared|offered|purchased)\s*/i, "")
                .replace(/^[""]/, "").replace(/[""]$/, "")
                .trim();

            // Null out Poshmark's ":post_title" / "post title" placeholder
            const cleanTitle = (listingTitle && !/^:?post[_\s]title$/i.test(listingTitle)) ? listingTitle : null;

            return { href, storyType, listingId, orderId, listingTitle: cleanTitle, username, content };
        })
    );
}

async function run() {
    const runId = Date.now().toString(36);
    console.log(`[${new Date().toISOString()}] 📬 [${runId}] Starting Poshmark notify run`);

    if (!fs.existsSync(SESSION_FILE)) {
        console.error(`[${runId}] ❌ No session file — run poshmark_login.js first`);
        process.exit(1);
    }

    const browser = await chromium.launch({
        headless: true,
        executablePath: "/opt/supabase-mcp/custom/ms-playwright/chromium_headless_shell-1217/chrome-headless-shell-linux64/chrome-headless-shell",
        args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
    });

    const context = await browser.newContext({
        storageState: SESSION_FILE,
        userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        viewport: { width: 1280, height: 800 },
    });

    await context.addInitScript(() => {
        Object.defineProperty(navigator, "webdriver", { get: () => false });
        window.chrome = { runtime: {} };
    });

    const page = await context.newPage();
    const scraped = [];
    let existingIds = new Set();

    try {
        // 1. Scrape news tabs
        for (const tab of TABS) {
            try {
                const items = await scrapeTab(page, tab);
                console.log(`[${runId}] ${tab}: ${items.length} item(s)`);
                for (const item of items) {
                    const st = item.storyType.toLowerCase();
                    const type = st.includes("comment") ? "comment"
                        : (st.includes("sale") || st.includes("order") || st.includes("purchase")) ? "order"
                        : (st.includes("offer") || st.includes("bid")) ? "offer"
                        : st.includes("like") ? "like"
                        : "other";

                    let listingTitle = item.listingTitle || null;
                    let content = item.content || null;
                    if (!listingTitle && content) {
                        // "on your listing "Title"" pattern (comments)
                        const m1 = content.match(/on your listing\s+[""']?(.+?)[""']?\s*$/i);
                        if (m1) {
                            listingTitle = m1[1].trim();
                            content = content.replace(/\s*on your listing\s+[""']?.+$/i, "").trim() || null;
                        }
                        // "offer on "Title" was sent" pattern (offers)
                        const m2 = !listingTitle && content?.match(/offer on\s+[""](.+?)[""]/i);
                        if (m2) listingTitle = m2[1].trim();
                    }

                    const idKey = item.orderId || item.listingId || item.href;
                    scraped.push({
                        poshmark_id: makeId(type, item.username, idKey, (item.content || "").slice(0, 80)),
                        type,
                        from_username: item.username || null,
                        listing_title: listingTitle,
                        listing_url: null, // filled in after listing page fetch
                        content,
                        _href: item.href, // temp: used for listing page fetch, not saved to DB
                    });
                }
            } catch (err) {
                console.error(`[${runId}] ❌ ${tab}: ${err.message}`);
            }
        }

        // 2. While browser is still open: find new items and fetch listing page title for each
        if (scraped.length) {
            const existingRes = await callTool("query_table", {
                schema: "finance",
                table: "poshmark_activity",
                select: ["poshmark_id"],
                limit: 500,
            });
            const allKnown = (parseToolResponse(existingRes) || []).map(r => r.poshmark_id);
            existingIds = new Set(allKnown);

            for (const item of scraped) {
                if (existingIds.has(item.poshmark_id)) continue; // already in DB
                if (item.listing_title) continue;               // already have a title
                if (!item._href?.includes("/listing/")) continue;

                try {
                    await page.goto(`https://poshmark.com${item._href}`, { waitUntil: "domcontentloaded", timeout: 15000 });
                    const h1 = await page.$eval("h1", el => el.innerText.trim()).catch(() => null);
                    if (h1) {
                        item.listing_title = h1;
                        console.log(`[${runId}] 🔗 Title from listing page: "${h1}"`);
                    }
                    item.listing_url = page.url().replace(/[?#].*$/, "");
                } catch (err) {
                    console.error(`[${runId}] ⚠️ Listing page fetch failed: ${err.message}`);
                }
                await sleep(500);
            }
        }

        await context.storageState({ path: SESSION_FILE });
    } finally {
        await browser.close();
    }

    if (!scraped.length) {
        console.log(`[${runId}] ✅ No items found`);
        return;
    }

    // Insert only NEW items — never touch existing rows (prevents notified_at overwrite)
    const poshmarkIds = scraped.map(i => i.poshmark_id);
    const newItems = scraped.filter(i => !existingIds.has(i.poshmark_id));
    for (const item of newItems) {
        const { _href, ...record } = item;
        try {
            await callTool("insert_data", {
                schema: "finance",
                table: "poshmark_activity",
                data: record,
            });
            console.log(`[${runId}] ➕ Inserted: ${record.type} from ${record.from_username} — "${record.listing_title || ''}"`);
        } catch (err) {
            console.error(`[${runId}] ❌ Insert failed ${item.poshmark_id}: ${err.message}`);
        }
    }

    // Find newly inserted rows: notified_at null AND poshmark_id is one we just inserted
    const insertedIds = new Set(newItems.map(i => i.poshmark_id));
    let unnotified = [];
    try {
        const res = await callTool("query_table", {
            schema: "finance",
            table: "poshmark_activity",
            select: ["id", "poshmark_id", "type", "from_username", "listing_title", "listing_url", "content"],
            where: { notified_at: { eq: null } },
            limit: 100,
        });
        const all = parseToolResponse(res) || [];
        unnotified = all.filter(r => insertedIds.has(r.poshmark_id));
    } catch (err) {
        console.error(`[${runId}] ❌ Query unnotified failed: ${err.message}`);
    }

    console.log(`[${runId}] ${unnotified.length} new item(s) to notify`);

    let notified = 0;
    for (const row of unnotified) {
        try {
            const label = row.type === "comment" ? "💬 Comment"
                : row.type === "order" ? "🛍️ Sale"
                : row.type === "offer" ? "💰 Offer"
                : row.type === "like" ? "❤️ Like"
                : "📌 Poshmark";
            const from = row.from_username || "Someone";
            const parts = [];
            if (row.content) parts.push(`"${row.content.slice(0, 100)}"`);
            if (row.listing_title) parts.push(row.listing_title.slice(0, 60));
            if (row.listing_url) parts.push(row.listing_url);
            const msg = parts.join("\n") || "(no content)";

            await callTool("notify_push", {
                provider: "pushover",
                category: "poshmark",
                title: `${label} — ${from}`,
                message: msg,
                no_log: true,
            });

            await callTool("update_data", {
                schema: "finance",
                table: "poshmark_activity",
                pk: "id",
                where: { poshmark_id: row.poshmark_id },
                data: { notified_at: new Date().toISOString() },
            });

            notified++;
            console.log(`[${runId}] ✅ Notified: ${row.type} from ${from}`);
            await sleep(250);
        } catch (err) {
            console.error(`[${runId}] ❌ Notify failed for ${row.poshmark_id}: ${err.message}`);
        }
    }

    console.log(`[${new Date().toISOString()}] ✅ [${runId}] Done: ${notified}/${unnotified.length} notified`);
}

run().catch(err => {
    console.error(`[FATAL] ${err.message}`);
    process.exit(1);
});
