// Usage: node wg/test_send_dryrun.mjs <listing-url>
// Navigates to the listing, clicks "Nachricht senden", fills the textarea,
// takes a screenshot, then exits WITHOUT submitting.

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_FILE = path.join(__dirname, "session.json");
const SCREENSHOT = path.join(__dirname, "send_dryrun.png");

const URL_ARG = process.argv[2] || "https://www.wg-gesucht.de/1-zimmer-wohnungen-in-Leipzig-Gruenau-Nord.13409417.html";
const DUMMY_MESSAGE = `Hallo,

ich bin sehr interessiert an Ihrer Wohnung.

[DRY RUN — message not sent]

Viele Grüße
Steven Elliott`;

process.env.PLAYWRIGHT_BROWSERS_PATH = "/opt/supabase-mcp/custom/ms-playwright";
const { chromium } = await import("playwright");

const browser = await chromium.launch({
    headless: true,
    executablePath: "/opt/supabase-mcp/custom/ms-playwright/chromium_headless_shell-1217/chrome-headless-shell-linux64/chrome-headless-shell",
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
});

try {
    const context = await browser.newContext({
        storageState: SESSION_FILE,
        userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        viewport: { width: 1280, height: 900 },
    });
    const page = await context.newPage();

    console.log(`Navigating to: ${URL_ARG}`);
    await page.goto(URL_ARG, { waitUntil: "domcontentloaded", timeout: 30000 });
    console.log(`Final URL: ${page.url()}`);

    if (/cuba\.html/i.test(page.url())) {
        console.error("❌ Rate limited / captcha — re-login needed");
        await page.screenshot({ path: SCREENSHOT });
        process.exit(1);
    }
    if (/login|signin/i.test(page.url())) {
        console.error("❌ Session expired — redirected to login");
        await page.screenshot({ path: SCREENSHOT });
        process.exit(1);
    }

    // Dismiss cookie banner
    try { await page.click("#cmpwelcomebtnyes", { timeout: 3000 }); console.log("Dismissed cookie banner"); } catch {}

    // Find first visible "Nachricht senden" — WG-Gesucht renders multiple copies
    // (mobile sidebar, desktop sidebar, sticky) with CSS show/hide
    const allBtns = page.locator('a:has-text("Nachricht senden"), button:has-text("Nachricht senden")');
    const btnCount = await allBtns.count();
    let contactBtn = null;
    for (let i = 0; i < btnCount; i++) {
        if (await allBtns.nth(i).isVisible()) { contactBtn = allBtns.nth(i); break; }
    }
    if (!contactBtn) {
        console.error(`❌ No visible Nachricht senden button found (${btnCount} total in DOM)`);
        await page.screenshot({ path: SCREENSHOT });
        process.exit(1);
    }
    console.log("✅ Found visible contact button, clicking...");
    await contactBtn.click({ timeout: 10000 });

    // Wait for textarea
    const msgArea = page.locator([
        'textarea[name="message_body"]',
        'textarea[name="message"]',
        "#message-body",
        "textarea.contact-form-textarea",
        "textarea.form-control",
    ].join(", ")).first();

    await msgArea.waitFor({ state: "visible", timeout: 10000 });
    console.log("✅ Message textarea visible");

    await msgArea.fill(DUMMY_MESSAGE);
    console.log("✅ Filled textarea with dummy message");

    // Check submit button is present
    const submitBtn = page.locator([
        'button[type="submit"]:has-text("Senden")',
        'button:has-text("Nachricht absenden")',
        'button:has-text("Absenden")',
        'input[type="submit"]',
    ].join(", ")).first();
    const submitVisible = await submitBtn.isVisible().catch(() => false);
    console.log(submitVisible ? "✅ Submit button found" : "⚠️  Submit button NOT found (selectors may need adjustment)");

    await page.screenshot({ path: SCREENSHOT });
    console.log(`\n📸 Screenshot saved to ${SCREENSHOT}`);
    console.log("🛑 DRY RUN — did NOT click submit");

} finally {
    await browser.close();
}
