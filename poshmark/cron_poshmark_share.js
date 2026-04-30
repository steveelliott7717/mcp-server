// /opt/supabase-mcp/custom/poshmark/cron_poshmark_share.js
// Shares all listings in Poshmark closet to followers
// Runs twice daily via cron

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import { chromium } from "playwright";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

process.env.PLAYWRIGHT_BROWSERS_PATH = "/opt/supabase-mcp/custom/ms-playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_FILE = path.join(__dirname, "session.json");
const CLOSET_URL = "https://poshmark.com/closet/valuewearshop";

const MIN_DELAY_MS = 2000;
const MAX_DELAY_MS = 4000;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function jitter() { return MIN_DELAY_MS + Math.random() * (MAX_DELAY_MS - MIN_DELAY_MS); }

async function shareCloset() {
    const runId = Date.now().toString(36);
    const startedAt = new Date().toISOString();
    console.log(`[${startedAt}] 🛍️ [${runId}] Starting Poshmark share run`);

    if (!fs.existsSync(SESSION_FILE)) {
        console.error(`[${runId}] ❌ No session file at ${SESSION_FILE} — run poshmark_login.js first`);
        return { shared: 0, errors: 0, total: 0 };
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
    let shared = 0, errors = 0, total = 0;

    try {
        await page.goto(CLOSET_URL, { waitUntil: "domcontentloaded", timeout: 30000 });

        // Verify we're still logged in
        const isLoggedOut = await page.$('a[href="/login"]');
        if (isLoggedOut) {
            console.error(`[${runId}] ❌ Session expired — run poshmark_login.js to refresh`);
            return { shared: 0, errors: 0, total: 0 };
        }

        console.log(`[${runId}] 📄 Loaded closet`);

        const SHARE_BTN = '[data-et-name="share"][data-et-prop-location="listing_tile"]';
        await page.waitForSelector(SHARE_BTN, { timeout: 15000 });

        total = (await page.$$(SHARE_BTN)).length;
        console.log(`[${runId}] Found ${total} listings`);

        for (let i = 0; i < total; i++) {
            // Re-query fresh each iteration — Vue re-renders detach stale handles
            const btns = await page.$$(SHARE_BTN);
            if (i >= btns.length) break;
            const btn = btns[i];
            const listingId = await btn.getAttribute("data-et-prop-listing_id") || "unknown";

            try {
                // Scroll into view and hover to reveal share button
                await btn.scrollIntoViewIfNeeded();
                await sleep(300);
                await btn.hover();
                await sleep(400);

                // Click share button
                await btn.click({ timeout: 5000 });

                // Wait for modal
                await page.waitForSelector('[data-test="modal-container"]', { timeout: 8000 });

                // Click "To My Followers"
                const shareTarget = await page.$('a.internal-share__link[data-et-name="share_poshmark"]');
                const shareText = await shareTarget?.evaluate(el => el.innerText.trim());
                console.log(`[${runId}] 🖱️ Clicking: "${shareText}"`);
                await shareTarget.click({ timeout: 5000 });

                // Wait for modal to close
                await page.waitForSelector('[data-test="modal-container"]', { state: "hidden", timeout: 8000 });

                shared++;
                console.log(`[${runId}] ✅ [${i + 1}/${total}] Shared ${listingId.slice(-8)}`);

            } catch (err) {
                console.error(`[${runId}] ❌ [${i + 1}/${total}] listing ${listingId.slice(-8)}: ${err.message}`);
                // Close any stuck modal before continuing
                try { await page.keyboard.press("Escape"); await sleep(500); } catch {}
                errors++;
            }

            await sleep(jitter());
        }

        // Refresh session for next run
        await context.storageState({ path: SESSION_FILE });

    } catch (err) {
        console.error(`[${runId}] ❌ Fatal: ${err.message}`);
    } finally {
        await browser.close();
    }

    console.log(`[${new Date().toISOString()}] ✅ [${runId}] Done: ${shared}/${total} shared, ${errors} errors`);
    return { shared, errors, total };
}

shareCloset()
    .then(r => { console.log(`[RESULT] ${JSON.stringify(r)}`); process.exit(0); })
    .catch(err => { console.error(`[FATAL] ${err.message}`); process.exit(1); });
