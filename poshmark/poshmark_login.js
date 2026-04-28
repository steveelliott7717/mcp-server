// /opt/supabase-mcp/custom/poshmark/poshmark_login.js
// Run once manually to save Poshmark session to session.json
// Usage: node poshmark_login.js

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import { chromium } from "playwright";
import path from "path";
import { fileURLToPath } from "url";
import readline from "readline";

function prompt(question) {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    return new Promise(resolve => rl.question(question, ans => { rl.close(); resolve(ans.trim()); }));
}

process.env.PLAYWRIGHT_BROWSERS_PATH = "/opt/supabase-mcp/custom/ms-playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_FILE = path.join(__dirname, "session.json");

const EMAIL = process.env.POSHMARK_EMAIL;
const PASSWORD = process.env.POSHMARK_PASSWORD;

if (!EMAIL || !PASSWORD) {
    console.error("POSHMARK_EMAIL and POSHMARK_PASSWORD must be set in .env");
    process.exit(1);
}

const browser = await chromium.launch({
    headless: true,
    executablePath: "/opt/supabase-mcp/custom/ms-playwright/chromium_headless_shell-1217/chrome-headless-shell-linux64/chrome-headless-shell",
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
});

const context = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    viewport: { width: 1280, height: 800 },
});

const page = await context.newPage();

try {
    await page.goto("https://poshmark.com/login", { waitUntil: "domcontentloaded", timeout: 30000 });
    console.log("Loaded login page");

    // Dismiss cookie banner if present
    try { await page.click('button:has-text("Ok")', { timeout: 3000 }); } catch {}

    await page.waitForSelector('input[placeholder="Username or Email"]', { timeout: 10000 });
    await page.fill('input[placeholder="Username or Email"]', EMAIL);
    await page.fill('input[type="password"]', PASSWORD);
    await page.click('button:has-text("Login")');

    // Handle phone verification if triggered
    try {
        await page.waitForSelector('.modal--in input', { timeout: 8000 });
        const code = await prompt("📱 Poshmark sent a verification code to your phone. Enter it here: ");
        await page.fill('.modal--in input', code);
        await page.click('.modal--in button:has-text("Done")');
    } catch {}

    // Wait for redirect away from login page
    await page.waitForURL(url => !url.toString().includes("/login"), { timeout: 20000 });
    console.log("Logged in successfully, current URL:", page.url());

    await context.storageState({ path: SESSION_FILE });
    console.log("✅ Session saved to", SESSION_FILE);
} catch (err) {
    console.error("❌ Login failed:", err.message);
    // Take a screenshot to see what happened
    await page.screenshot({ path: path.join(__dirname, "login_error.png") });
    console.log("Screenshot saved to login_error.png");
    process.exit(1);
} finally {
    await browser.close();
}
