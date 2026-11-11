// /opt/supabase-mcp/custom/alditalk/cron_alditalk_topup.js
//
// Reads the ALDI TALK Inland data allowance and books the free +1 GB top-up
// whenever it drops below the account's refill threshold.
//
// Design: the routine read goes through the portal's JSON BFF API — plain fetch,
// no browser, ~50 ms. Firefox is launched only for the two things that need it:
//   1. renewing the session when the API starts refusing us
//   2. clicking the top-up button
// That keeps the ~1.2 GB browser spike to roughly twice an hour instead of twelve.
//
// Notes earned the hard way:
//  * FIREFOX ONLY for the click. Playwright Chromium triggers an SMS OTP challenge
//    on the booking action from this host; Firefox booked silently until 2026-09-18,
//    when Firefox started getting it too. The code is now relayed from the iPhone
//    (Shortcuts automation → /shortcuts/alditalk-otp → .otp_inbox) and typed in.
//  * Login is never automated — ForgeRock fronts it with a proof-of-work challenge
//    behind Cloudflare Bot Management. session.json is seeded from browser cookies
//    (build_session.js) and the "Angemeldet bleiben" token renews it in place.
//  * API responses do NOT refresh the session cookies (only Cloudflare's __cf_bm).
//    Renewal happens via the portal page load's OAuth redirect, hence renewSession().
//  * The button self-guards: above the threshold it shows an explainer rather than
//    booking, so a spurious click cannot over-book. The top-up is free and unlimited
//    ("Unendlich nachbuchen": kostenlos ... so oft du willst).

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import { firefox } from "playwright";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { callTool } from "./mcp.js";

// Firefox lives in the default browser cache, not the repo-local path the
// Chromium-based crons use.
delete process.env.PLAYWRIGHT_BROWSERS_PATH;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_FILE = path.join(__dirname, "session.json");
const PORTAL = "https://www.alditalk-kundenportal.de";
const OVERVIEW_URL = `${PORTAL}/portal/auth/uebersicht/`;
const ACTION_BTN = 'one-usage-meter one-button[slot="action"]';
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0";

// From subscribedOffers[0].subscriptionId. Re-derivable from
// customer-master-data/v1/navigation-list?msisdn=<msisdn> if the contract changes.
const CONTRACT_ID = "32237690";
const OFFERS_URL = `${PORTAL}/scs/bff/scs-209-selfcare-dashboard-bff/selfcare-dashboard/v1/offers`
    + `?warningDays=28&contractId=${CONTRACT_ID}&productType=Mobile_Product_Offer`;

const DEFAULT_THRESHOLD_KB = 1048576;   // 1 GB, if the account doesn't state one

// Marker so a dead token pushes once, not every five minutes until re-seeded.
const DEAD_FLAG = path.join(__dirname, ".session_dead");
// When the current lapse began. A login redirect can clear by itself (26 min on
// 2026-11-06, token still valid), so only a lapse outlasting LAPSE_ALERT_MS pushes.
const LAPSE_SINCE = path.join(__dirname, ".session_lapse_since");
const LAPSE_ALERT_MS = 45 * 60 * 1000;
// Written by index-http.js /shortcuts/alditalk-otp. Codes are valid 2 minutes.
const OTP_INBOX = path.join(__dirname, ".otp_inbox");
const OTP_WAIT_MS = 100000;
const sleep = ms => new Promise(r => setTimeout(r, ms));

function cookieHeader() {
    const { cookies } = JSON.parse(fs.readFileSync(SESSION_FILE, "utf8"));
    return cookies
        .filter(c => c.domain.includes("kundenportal"))
        .map(c => `${c.name}=${c.value}`)
        .join("; ");
}

/** Fetch the offers payload. Returns null when the session is no longer accepted. */
async function fetchOffers() {
    const res = await fetch(OFFERS_URL, {
        headers: { "User-Agent": UA, Accept: "application/json", Cookie: cookieHeader() },
        redirect: "manual",
    });
    // An expired session redirects to the login host rather than returning 401.
    if (res.status >= 300 && res.status < 400) return null;
    if (!res.ok) return null;
    const body = await res.json().catch(() => null);
    return body && body.subscribedOffers ? body : null;
}

/**
 * Load the portal in Firefox. The page load runs the OAuth redirect, which the
 * remember-me token turns into a fresh session; the refreshed cookies are saved.
 * Optionally clicks the top-up button while we are there.
 */
async function withBrowser(fn) {
    const browser = await firefox.launch({ headless: false });
    try {
        const context = await browser.newContext({
            storageState: SESSION_FILE,
            viewport: { width: 1400, height: 1100 },
            locale: "de-DE",
            timezoneId: "Europe/Berlin",
        });
        const page = await context.newPage();
        await page.goto(OVERVIEW_URL, { waitUntil: "domcontentloaded", timeout: 60000 });
        await page.waitForLoadState("networkidle", { timeout: 30000 }).catch(() => { });
        await sleep(5000);

        if (page.url().includes("login.alditalk")) return { ok: false, reason: "session lapsed" };

        // Consent overlay intercepts pointer events; storageState carries no
        // localStorage, so the banner reappears on every run.
        await page.evaluate(() => {
            const root = document.querySelector("#usercentrics-root");
            if (!root) return;
            const btn = root.shadowRoot && [...root.shadowRoot.querySelectorAll("button")]
                .find(b => /akzeptieren|accept/i.test(b.innerText || ""));
            btn ? btn.click() : root.remove();
        });
        await sleep(2000);

        const result = fn ? await fn(page) : { ok: true };
        await context.storageState({ path: SESSION_FILE });
        return result;
    } finally {
        await browser.close();
    }
}

async function clickTopUp(page) {
    const btn = await page.$(ACTION_BTN);
    if (!btn) return { ok: false, reason: "+1 GB button not found" };

    await btn.scrollIntoViewIfNeeded();
    await sleep(500);
    const clickedAt = Date.now();
    await btn.click({ timeout: 10000 });
    await sleep(8000);

    // Since 2026-09-18 ALDI challenges this host's bookings with an SMS OTP (a
    // manual booking from a home browser is not challenged). The iPhone forwards
    // the SMS to /shortcuts/alditalk-otp, which drops the code in OTP_INBOX.
    const otp = await page.evaluate(() =>
        /OTP|Verification|verification code|Bestätigungscode/i.test(document.body.innerText || ""));
    if (!otp) return { ok: true };

    console.log("OTP challenge — waiting for the forwarded SMS code");
    const code = await waitForOtpCode(clickedAt);
    if (!code) {
        await page.screenshot({ path: path.join(__dirname, "otp_challenge.png") }).catch(() => { });
        return { ok: false, otp: true, reason: `OTP challenge — no code forwarded within ${OTP_WAIT_MS / 1000}s` };
    }

    const inputs = page.locator("input:visible");
    if (await inputs.count() >= code.length) {
        for (let i = 0; i < code.length; i++) await inputs.nth(i).fill(code[i]);
    } else {
        await inputs.first().click();
        await page.keyboard.type(code, { delay: 150 });
    }
    await sleep(1000);
    await page.getByText(/Verify OTP|Bestätigen/i).first().click({ timeout: 10000 });
    await sleep(10000);

    const still = await page.evaluate(() => /OTP Verification/i.test(document.body.innerText || ""));
    if (still) {
        await page.screenshot({ path: path.join(__dirname, "otp_challenge.png") }).catch(() => { });
        return { ok: false, otp: true, reason: "OTP code entered but popup still showing (rejected/expired?)" };
    }
    console.log("OTP code accepted");
    return { ok: true, viaOtp: true };
}

/** Poll the inbox for a code that arrived after the click. Consumes it. */
async function waitForOtpCode(clickedAt) {
    const deadline = clickedAt + OTP_WAIT_MS;
    while (Date.now() < deadline) {
        try {
            const { code, received_at } = JSON.parse(fs.readFileSync(OTP_INBOX, "utf8"));
            if (Date.parse(received_at) >= clickedAt && /^\d{4,8}$/.test(code)) {
                fs.unlinkSync(OTP_INBOX);
                return code;
            }
        } catch { /* no inbox yet */ }
        await sleep(2000);
    }
    return null;
}

async function run() {
    const runId = Date.now().toString(36);
    const started = new Date().toISOString();
    console.log(`[${started}] 📶 [${runId}] alditalk check`);

    let usedBrowser = false;

    try {
        let offers = await fetchOffers();

        if (!offers) {
            // Session no longer accepted — renew via a portal page load, then retry.
            console.log(`[${runId}] API refused — renewing session via browser`);
            usedBrowser = true;
            const renew = await withBrowser(null);
            if (!renew.ok) {
                console.error(`[${runId}] ❌ ${renew.reason}`);
                // A missing or unreadable timer starts over, so it can never block the alert for good.
                let since = fs.existsSync(LAPSE_SINCE) ? Date.parse(fs.readFileSync(LAPSE_SINCE, "utf8").trim()) : NaN;
                if (Number.isNaN(since)) {
                    since = Date.now();
                    fs.writeFileSync(LAPSE_SINCE, new Date(since).toISOString());
                }
                const lapsedMs = Date.now() - since;
                if (lapsedMs < LAPSE_ALERT_MS) {
                    console.error(`[${runId}] session lapsed for ${Math.round(lapsedMs / 60000)} min — top-ups paused, alert at ${LAPSE_ALERT_MS / 60000} min`);
                    return;
                }
                console.error(`[${runId}] SESSION DEAD — re-export browser cookies and rebuild session.json; top-ups are paused until then`);
                // The browser has been unable to renew for the whole grace period,
                // so the "Angemeldet bleiben" token has most likely expired. Push
                // once, then stay quiet.
                if (!fs.existsSync(DEAD_FLAG)) {
                    try {
                        const res = await callTool("notify_push", {
                            provider: "pushover",
                            category: "quick",
                            title: "📶 ALDI TALK remember-me expired",
                            message: "The 'Angemeldet bleiben' token was rejected. Log in again with the box ticked, "
                                + "re-capture the login.alditalk-kundenbetreuung.de Cookie header, and re-run build_session.js. "
                                + "Top-ups are paused until then.",
                            no_log: true,
                        });
                        // A rejected push comes back as a result, not a throw — only
                        // set the flag once the push really went out.
                        const text = res?.result?.content?.[0]?.text ?? res?.content?.[0]?.text ?? "";
                        if (res?.error || res?.result?.isError || !/"ok":\s*true/.test(text)) {
                            throw new Error((text || JSON.stringify(res?.error ?? res)).slice(0, 300));
                        }
                        fs.writeFileSync(DEAD_FLAG, new Date().toISOString());
                        console.error(`[${runId}] pushed expiry notification (once)`);
                    } catch (e) {
                        console.error(`[${runId}] notify failed: ${e.message}`);
                    }
                }
                return;
            }
            offers = await fetchOffers();
            if (!offers) throw new Error("API still refusing after session renewal");
        }

        // Session is healthy — clear any prior dead-token marker so the next
        // expiry notifies again, and restart the grace period.
        if (fs.existsSync(DEAD_FLAG)) { fs.unlinkSync(DEAD_FLAG); console.log(`[${runId}] session recovered — cleared dead flag`); }
        if (fs.existsSync(LAPSE_SINCE)) { fs.unlinkSync(LAPSE_SINCE); console.log(`[${runId}] session recovered — lapse timer cleared`); }

        const offer = offers.subscribedOffers[0];
        const inland = (offer.pack || []).find(p => p.balanceAttributeReference === "dataGrantAmount");
        if (!inland) throw new Error("dataGrantAmount not present in offers payload");

        const allocatedKb = Number(inland.allocated);
        const usedKb = Number(inland.used);
        const remainingKb = allocatedKb - usedKb;
        const remainingMb = Math.round(remainingKb / 1024);
        const thresholdKb = Number(offer.refillThresholdValueUid) || DEFAULT_THRESHOLD_KB;
        const balance = offers.totalBalance != null ? Number(offers.totalBalance) : null;
        const refillable = offer.isOnDemandRefillApplicable === true;

        console.log(`[${runId}] ${remainingMb} MB left of ${Math.round(allocatedKb / 1024 / 1024)} GB `
            + `| threshold ${Math.round(thresholdKb / 1024)} MB | Guthaben ${balance ?? "?"} € `
            + `| refillable ${refillable}`);

        let action = "check", toppedUp = null, afterMb = null;

        if (remainingKb < thresholdKb && refillable) {
            console.log(`[${runId}] below threshold — booking free +1 GB`);
            usedBrowser = true;
            action = "topup";

            const clicked = await withBrowser(clickTopUp);
            // No pause and no push on a missed OTP: the next run clicks again and
            // gets a fresh SMS, which the phone forwards once it is reachable.
            if (!clicked.ok) throw new Error(clicked.reason);

            // Confirm against the API rather than trusting the rounded gauge.
            await sleep(3000);
            const after = await fetchOffers();
            const afterInland = after && after.subscribedOffers[0].pack
                .find(p => p.balanceAttributeReference === "dataGrantAmount");
            if (afterInland) {
                afterMb = Math.round((Number(afterInland.allocated) - Number(afterInland.used)) / 1024);
                toppedUp = afterMb > remainingMb;
            }

            console.log(`[${runId}] ${toppedUp ? "✅ booked" : "⚠️ no change"}: ${remainingMb} MB → ${afterMb} MB`);
        }

        // No database — the log line is the record. One JSON object per run.
        console.log(`[${runId}] RESULT ${JSON.stringify({
            checked_at: new Date().toISOString(),
            data_remaining_mb: remainingMb,
            data_allocated_mb: Math.round(allocatedKb / 1024),
            threshold_mb: Math.round(thresholdKb / 1024),
            balance_eur: balance,
            action,
            topup_ok: toppedUp,
            data_after_mb: afterMb,
            used_browser: usedBrowser,
        })}`);
    } catch (err) {
        console.error(`[${runId}] ❌ ${err.message}`);
        process.exitCode = 1;
    } finally {
        console.log(`[${new Date().toISOString()}] [${runId}] done (browser=${usedBrowser})`);
    }
}

run();
