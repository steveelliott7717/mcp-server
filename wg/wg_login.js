// /opt/supabase-mcp/custom/wg/wg_login.js
// Run once manually to save WG-Gesucht session to session.json
// Usage: node wg_login.js

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

const EMAIL = process.env.WG_EMAIL;
const PASSWORD = process.env.WG_PASSWORD;

if (!EMAIL || !PASSWORD) {
    console.error("WG_EMAIL and WG_PASSWORD must be set in .env");
    process.exit(1);
}

const browser = await chromium.launch({
    headless: true,
    executablePath: "/opt/supabase-mcp/custom/ms-playwright/chromium_headless_shell-1217/chrome-headless-shell-linux64/chrome-headless-shell",
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
});

const context = await browser.newContext({
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    viewport: { width: 1280, height: 900 },
});

const page = await context.newPage();

try {
    await page.goto("https://www.wg-gesucht.de/", { waitUntil: "domcontentloaded", timeout: 30000 });
    console.log("Loaded homepage:", page.url());

    // Accept cookie banner without navigating — use exact button text in German
    try {
        await page.click('#cmpwelcomebtnyes', { timeout: 4000 });
        console.log("Accepted cookies via #cmpwelcomebtnyes");
    } catch {
        try {
            // Click the consent button that stays on the same page
            await page.evaluate(() => {
                const btns = [...document.querySelectorAll('button')];
                const btn = btns.find(b => /akzeptieren|accept all/i.test(b.textContent));
                if (btn) btn.click();
            });
            console.log("Accepted cookies via evaluate");
        } catch {}
    }
    await page.waitForTimeout(800);

    // Click "Mein Konto" / "My Account" to open the login modal
    await page.click('a:has-text("Mein Konto"), a:has-text("My Account"), #header_login_link, .nav-login', { timeout: 8000 });
    console.log("Clicked login trigger");

    // Wait for the actual login form fields (form#login_basic)
    await page.waitForSelector('#login_email_username', { state: 'attached', timeout: 10000 });
    await page.waitForTimeout(600);
    console.log("Login modal in DOM");

    // Focus+type sends real key events without requiring Playwright visibility check
    await page.locator('#login_email_username').evaluate(el => el.focus());
    await page.keyboard.type(EMAIL, { delay: 40 });
    await page.locator('#login_password').evaluate(el => el.focus());
    await page.keyboard.type(PASSWORD, { delay: 40 });
    console.log("Filled email and password");

    // Login submit is an <input type="submit" id="login_submit">
    await page.locator('#login_submit').click({ force: true, timeout: 5000 });
    console.log("Submitted login form");

    // Handle 2FA — WG-Gesucht splits the code into 6 individual digit fields
    try {
        await page.waitForSelector('input[name="verification_code_1"]', { state: 'attached', timeout: 8000 });
        console.log("⚠️  2FA required — check your email (steveelliott7717@gmail.com) for a 6-digit code");
        const code = await prompt("📧 Enter the 6-digit code from your email: ");
        const digits = code.replace(/\D/g, '').split('');
        for (let i = 0; i < 6; i++) {
            const digit = digits[i] || '';
            await page.locator(`input[name="verification_code_${i + 1}"]`).evaluate(el => el.focus());
            await page.keyboard.type(digit, { delay: 60 });
        }
        // Submit — button is near #resend_verification_code
        await page.locator('button:has-text("Bestätigen")').first().click({ force: true });
        console.log("Submitted 2FA code");
        // Wait for the verification modal to disappear
        await page.waitForSelector('input[name="verification_code_1"]', { state: 'detached', timeout: 15000 });
        console.log("2FA accepted, modal closed");
    } catch {
        // No 2FA prompt — continue
    }

    // Allow page to settle then check login state. WG-Gesucht may return to the
    // homepage without rendering a logout link immediately after 2FA.
    await page.waitForTimeout(2000);
    const loginState = await page.evaluate(() => {
        const isVisible = (el) => {
            if (!el) return false;
            const style = window.getComputedStyle(el);
            const rect = el.getBoundingClientRect();
            return style.visibility !== 'hidden' &&
                style.display !== 'none' &&
                rect.width > 0 &&
                rect.height > 0;
        };

        const strongLoggedIn = !!(
            document.querySelector('a[href*="mein-wg-gesucht"]') ||
            document.querySelector('a[href*="my-wg-gesucht"]') ||
            document.querySelector('a[href*="logout"]') ||
            document.querySelector('a[href*="abmelden"]') ||
            document.querySelector('#logout_link') ||
            document.querySelector('.logout') ||
            document.querySelector('a[href*="/users/"]')
        );

        const loginVisible = isVisible(document.querySelector('#login_email_username')) ||
            isVisible(document.querySelector('#login_password')) ||
            isVisible(document.querySelector('#cu_email')) ||
            isVisible(document.querySelector('#cu_password'));
        const verificationVisible = isVisible(document.querySelector('input[name="verification_code_1"]'));

        return {
            strongLoggedIn,
            loginVisible,
            verificationVisible,
            accepted: strongLoggedIn || (!loginVisible && !verificationVisible),
        };
    });
    if (!loginState.accepted) {
        throw new Error(`Login did not reach an accepted post-auth state: ${JSON.stringify(loginState)}`);
    }
    console.log("Logged in successfully, URL:", page.url(), "| state:", JSON.stringify(loginState));

    await context.storageState({ path: SESSION_FILE });
    console.log("✅ Session saved to", SESSION_FILE);

} catch (err) {
    console.error("❌ Login failed:", err.message);
    await page.screenshot({ path: path.join(__dirname, "login_error.png") });
    console.log("Screenshot saved to login_error.png");
    process.exit(1);
} finally {
    await browser.close();
}
