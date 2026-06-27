// Scrapes all listing URLs from your Poshmark closet.
// Usage: node scrape_closet_urls.mjs
// Outputs JSON array of { listing_id, title, listing_url }
// and optionally upserts into finance.marketplace_listings if --upsert flag passed.

import { chromium } from "/opt/supabase-mcp/custom/node_modules/playwright/index.mjs";
import fs from "fs";
import { callTool } from "./mcp.js";

const SESSION_FILE = "/opt/supabase-mcp/custom/poshmark/session.json";
const EXECUTABLE = "/opt/supabase-mcp/custom/ms-playwright/chromium_headless_shell-1217/chrome-headless-shell-linux64/chrome-headless-shell";
const UPSERT = process.argv.includes("--upsert");

if (!fs.existsSync(SESSION_FILE)) {
    console.error("No session file — run poshmark_login.js first");
    process.exit(1);
}

const browser = await chromium.launch({
    headless: true,
    executablePath: EXECUTABLE,
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
});
const context = await browser.newContext({
    storageState: SESSION_FILE,
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    viewport: { width: 1280, height: 900 },
});
await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
    window.chrome = { runtime: {} };
});

const page = await context.newPage();

// Step 1: get username from profile nav
await page.goto("https://poshmark.com", { waitUntil: "networkidle", timeout: 30000 });
if (await page.$('a[href="/login"]')) {
    console.error("Session expired — run poshmark_login.js to refresh");
    await browser.close();
    process.exit(1);
}

const username = await page.evaluate(() => {
    // Try nav profile link
    const a = document.querySelector('a[href*="/closet/"]');
    if (a) return (a.getAttribute("href") || "").replace("/closet/", "").replace(/^\//, "").split("?")[0];
    return null;
});

if (!username) {
    console.error("Could not detect Poshmark username from nav");
    await browser.close();
    process.exit(1);
}

console.error(`Logged in as: ${username}`);
console.error(`Scraping closet: https://poshmark.com/closet/${username}`);

// Step 2: load closet and scroll to get all listings
await page.goto(`https://poshmark.com/closet/${username}`, { waitUntil: "networkidle", timeout: 30000 });

// Scroll until no new listings appear
let prevCount = 0;
for (let i = 0; i < 50; i++) {
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(1500);
    const count = await page.$$eval('a[href*="/listing/"]', els => els.length);
    if (count === prevCount) break;
    prevCount = count;
    console.error(`  Scroll ${i + 1}: ${count} listings visible`);
}

// Step 3: extract all listing links
const listings = await page.$$eval('a[href*="/listing/"]', anchors => {
    const seen = new Set();
    const results = [];
    for (const a of anchors) {
        const href = a.getAttribute("href") || "";
        if (!href.includes("/listing/")) continue;
        const url = href.startsWith("http") ? href : `https://poshmark.com${href}`;
        const cleanUrl = url.replace(/[?#].*$/, "");
        if (seen.has(cleanUrl)) continue;
        seen.add(cleanUrl);

        // Extract listing ID (24-char hex or long alphanumeric at end)
        const idMatch = cleanUrl.match(/([a-f0-9]{24})$/) || cleanUrl.match(/-([a-zA-Z0-9]{16,})$/);
        const listing_id = idMatch ? idMatch[1] : null;

        // Title from img alt or aria-label
        const img = a.querySelector("img");
        const title = (img?.getAttribute("alt") || a.getAttribute("aria-label") || "").trim() || null;

        if (listing_id) results.push({ listing_id, title, listing_url: cleanUrl });
    }
    return results;
});

await browser.close();

console.error(`\nFound ${listings.length} listings`);
console.log(JSON.stringify(listings, null, 2));

if (UPSERT && listings.length) {
    console.error("\nUpserting platform_listing_id into finance.marketplace_listings...");
    let updated = 0;
    for (const item of listings) {
        if (!item.title) continue;
        try {
            // Match by title and platform
            const res = await callTool("query_table", {
                schema: "finance",
                table: "marketplace_listings",
                select: ["id", "title", "platform_listing_id"],
                where: { platform: { eq: "poshmark" }, title: { ilike: item.title } },
                limit: 1,
            });
            const rows = res?.content?.[0]?.json?.rows || res?.content?.[0]?.json || [];
            const row = Array.isArray(rows) ? rows[0] : null;
            if (!row) { console.error(`  No match for: ${item.title}`); continue; }
            if (row.platform_listing_id === item.listing_id) { console.error(`  Already set: ${item.title}`); continue; }

            await callTool("update_data", {
                schema: "finance",
                table: "marketplace_listings",
                pk: "id",
                where: { id: { eq: row.id } },
                data: { platform_listing_id: item.listing_id },
            });
            updated++;
            console.error(`  ✅ Updated: ${item.title} → ${item.listing_id}`);
        } catch (err) {
            console.error(`  ❌ Failed for ${item.title}: ${err.message}`);
        }
    }
    console.error(`\nDone: ${updated} rows updated`);
}
