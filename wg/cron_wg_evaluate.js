// /opt/supabase-mcp/custom/wg/cron_wg_evaluate.js
// Evaluates un-scored WG-Gesucht listings and optionally auto-sends messages above threshold.

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import path from "node:path";
import { fileURLToPath } from "node:url";
import { callTool } from "./mcp.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_FILE = path.join(__dirname, "session.json");

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const SCORE_THRESHOLD = Number(process.env.WG_SCORE_THRESHOLD || 60);
const ENABLE_AUTO_MESSAGE = String(process.env.WG_ENABLE_AUTO_MESSAGE || "").toLowerCase() === "true";
const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

function log(msg) { console.log(`[wg_evaluate] ${msg}`); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

const PLAYWRIGHT_EXECUTABLE = "/opt/supabase-mcp/custom/ms-playwright/chromium_headless_shell-1217/chrome-headless-shell-linux64/chrome-headless-shell";

async function callOpenAI(messages, jsonMode = false) {
    const res = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${OPENAI_API_KEY}`,
        },
        body: JSON.stringify({
            model: "gpt-4o-mini",
            messages,
            ...(jsonMode ? { response_format: { type: "json_object" } } : {}),
            max_tokens: 600,
            temperature: 0.3,
        }),
    });
    if (!res.ok) throw new Error(`OpenAI API error: ${res.status} ${await res.text()}`);
    const json = await res.json();
    return json.choices[0].message.content;
}

function isStudentOnly(listing) {
    const text = [
        listing.title,
        listing.description_wohnung,
        listing.description_lage,
        listing.description_sonstiges,
    ].join(" ").toLowerCase();

    return text.includes("immatrikulationsbescheinigung") ||
        text.includes("studentenwohnheim") ||
        text.includes("studentenwohnanlage") ||
        text.includes("nur an studenten") ||
        text.includes("nur fuer studenten") ||
        text.includes("nur für studenten");
}

function isActuallyWG(listing) {
    const text = [listing.title, listing.description_wohnung, listing.description_lage, listing.description_sonstiges]
        .join(" ").toLowerCase();

    const hardWGPatterns = [
        /\bwg-zimmer\b/i,
        /\bzimmer in (?:einer |einem |der )?wg\b/i,
        /\bzimmer frei\b/i,
        /\bmitbewohner(?:in)?\b/i,
        /\bwg-leben\b/i,
        /\bwg-bewohner\b/i,
        /\brest der wg\b/i,
        /\bgemeinschaftsküche\b/i,
        /\bshared kitchen\b/i,
        /\broom in a shared\b/i,
        /\broom in shared\b/i,
        /\bzwischenmiete\b.*\bzimmer\b/i,
        /\bein möbliertes zimmer steht zur miete\b/i,
        /\bzimmer steht zur miete\b/i,
    ];

    return hardWGPatterns.some((pattern) => pattern.test(text));
}

function hasSoftWGSignals(listing) {
    const text = [listing.title, listing.description_wohnung, listing.description_lage, listing.description_sonstiges]
        .join(" ")
        .toLowerCase();

    const softWGPatterns = [
        /\bwg geeignet\b/i,
        /\b2er wg\b/i,
        /\b3er wg\b/i,
        /\bideal geeignet .* wg\b/i,
        /\bfür ein paar oder als .*wg\b/i,
    ];

    return softWGPatterns.some((pattern) => pattern.test(text));
}

function forbidsAnmeldung(listing) {
    const text = [listing.description_wohnung, listing.description_sonstiges]
        .join(" ").toLowerCase();
    return text.includes("keine anmeldung") ||
        text.includes("anmeldung nicht möglich") ||
        text.includes("anmeldung leider nicht") ||
        text.includes("no registration");
}

function isCommercial(listing) {
    const text = (listing.title ?? "").toLowerCase();
    return text.includes("gmbh") ||
        text.includes("immobilien") ||
        text.includes("vermietung");
}

function hasNoProperBed(listing) {
    const text = [listing.title, listing.description_wohnung, listing.description_sonstiges]
        .join(" ").toLowerCase();
    const nobed = text.includes("kein bett") || text.includes("ohne bett");
    const sofaOnly = text.includes("schlafsofa") && !(/\bbett\b/.test(text));
    return nobed || sofaOnly;
}

function isObviousReject(listing) {
    const today = new Date();
    const moveIn = new Date("2026-06-01");

    // Already expired
    if (listing.available_to) {
        const to = new Date(listing.available_to);
        if (to < today) return "expired";
    }

    // Available date is after June 10 (arrival date)
    if (listing.available_from) {
        const from = new Date(listing.available_from);
        if (from > new Date("2026-06-10")) return `available too late (${listing.available_from})`;
    }

    // Lease under 30 days
    if (listing.available_from && listing.available_to) {
        const from = new Date(listing.available_from);
        const to = new Date(listing.available_to);
        const days = (to - from) / (1000 * 60 * 60 * 24);
        if (days < 30) return `lease too short (${Math.round(days)} days)`;
    }

    // Total rent clearly over budget
    const warm = listing.total_rent ?? (listing.rent + (listing.additional_costs ?? 0));
    if (warm > 820) return `over budget (${warm}€ warm)`;

    return null;
}

async function evaluateListing(listing, flags = {}) {
    const prompt = `You are evaluating apartment listings on WG-Gesucht.de for a tenant with these requirements:

TENANT PROFILE:
- Arriving in Leipzig on June 10 2026, looking for 1–3+ months, ideally long-term
- Has a Sperrkonto (Blocked Account) — financial proof for international tenants
- Non-smoker, quiet and reliable
- Needs Anmeldung (Wohnungsgeberbestätigung — official registration at the address)
- Budget: up to 800€ warm (total including all costs)
- Prefers good public transport access and walkable neighborhoods

SCORING GUIDE (0–100):
- Availability: must be available by June 10 2026 (arrival date) — available before = good; after June 10 = already rejected by pre-filter
- Lease term: unlimited or 6+ months = best; 3–6 months = ok; under 2 months = heavy penalty
- Anmeldung: explicitly not allowed = heavy penalty (−25); if not mentioned = neutral, do NOT penalize
- Value: size-to-price ratio — e.g. 40m² at 550€ is great, 30m² at 750€ is poor
- Location: Gohlis, Connewitz, Plagwitz, Schleußig, Südvorstadt, Zentrum = bonus; outer suburbs = neutral
- Features: balcony, furnished, washing machine = small bonuses
- Description quality: vague or boilerplate-only = slight penalty
- "WG geeignet", "2er WG", or "3er WG" on a full apartment listing is only a small penalty, not a rejection
- Commercial listing (GmbH/Immobilien): harder to get, more competitive — apply −10 penalty
- Sleeping arrangement: no dedicated bed (Schlafsofa only, or explicit "kein Bett") = −8; furnished with a proper Bett mentioned = neutral${flags.commercial ? "\n\nFLAGS:\n- Commercial agency listing detected. Apply the −10 commercial penalty." : ""}${flags.noProperBed ? (flags.commercial ? "\n- No proper bed detected (Schlafsofa only or explicit 'kein Bett'). Apply the −8 sleeping arrangement penalty." : "\n\nFLAGS:\n- No proper bed detected (Schlafsofa only or explicit 'kein Bett'). Apply the −8 sleeping arrangement penalty.") : ""}${flags.softWG ? ((flags.commercial || flags.noProperBed) ? "\n- Soft WG wording detected ('WG geeignet' / roommate-suitable wording). Treat this as only a small penalty, not a rejection." : "\n\nFLAGS:\n- Soft WG wording detected ('WG geeignet' / roommate-suitable wording). Treat this as only a small penalty, not a rejection.") : ""}

LISTING:
${JSON.stringify({
    title: listing.title,
    district: listing.district,
    rent: listing.rent,
    additional_costs: listing.additional_costs,
    total_rent: listing.total_rent,
    size_m2: listing.size_m2,
    rooms: listing.rooms,
    available_from: listing.available_from,
    available_to: listing.available_to,
    building_type: listing.building_type,
    floor_level: listing.floor_level,
    furnished: listing.furnished,
    has_balcony: listing.has_balcony,
    has_washing_machine: listing.has_washing_machine,
    has_own_kitchen: listing.has_own_kitchen,
    minutes_on_foot: listing.minutes_on_foot,
    features: listing.features,
    description_wohnung: listing.description_wohnung?.slice(0, 400),
    description_lage: listing.description_lage?.slice(0, 300),
    description_sonstiges: listing.description_sonstiges?.slice(0, 200),
}, null, 2)}

Respond with JSON only:
{
  "score": <integer 0-100>,
  "reason": "<2-3 sentences explaining the score and main factors>",
  "highlights": "<1 short sentence about the best aspect — used in the personalized outreach message>"
}`;

    const text = await callOpenAI([{ role: "user", content: prompt }], true);
    return JSON.parse(text);
}

async function generateMessage(listing, highlights) {
    const district = listing.district || "Leipzig";
    const prompt = `Write a German apartment inquiry message for a WG-Gesucht listing.

LISTING: "${listing.title}" in ${district}, ${listing.size_m2}m², ${listing.rent}€/month
STANDOUT FEATURE: ${highlights}

Fill in {PERSONALIZED} in the template below with 1–2 natural German sentences that reference something specific about this listing (the neighborhood, the size, a feature, the price, etc.). Do NOT change any other part of the template.

Hallo,

ich bin sehr interessiert an Ihrer Wohnung in ${district}.

Da ich im Juni nach Leipzig ziehe, suche ich eine Wohnung für mehrere Monate (gerne auch länger). {PERSONALIZED}

Ich verfüge über ein Sperrkonto (Blocked Account) als gesicherte finanzielle Grundlage und kann entsprechende Nachweise gerne vorlegen. Ich bin ein ruhiger, zuverlässiger Mieter und Nichtraucher.

Ist eine Anmeldung (Wohnungsgeberbestätigung) möglich?

Gerne würde ich eine Besichtigung vereinbaren – auch online.

Viele Grüße
Steven Elliott

Output only the completed message text, nothing else.`;

    return await callOpenAI([{ role: "user", content: prompt }]);
}

async function sendMessageOnce(listing, messageText) {
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

        await page.goto(listing.url, { waitUntil: "domcontentloaded", timeout: 30000 });

        if (/cuba\.html/i.test(page.url())) {
            throw new Error("Rate limited / captcha detected on listing page");
        }
        if (/login|signin/i.test(page.url())) {
            throw new Error("Session expired — redirected to login");
        }

        // Dismiss cookie banner if present
        try { await page.click("#cmpwelcomebtnyes", { timeout: 3000 }); } catch {}

        // WG-Gesucht renders multiple copies of this button — pick the first visible one
        const allBtns = page.locator('a:has-text("Nachricht senden"), button:has-text("Nachricht senden")');
        const btnCount = await allBtns.count();
        let contactBtn = null;
        for (let i = 0; i < btnCount; i++) {
            if (await allBtns.nth(i).isVisible()) { contactBtn = allBtns.nth(i); break; }
        }
        if (!contactBtn) throw new Error(`Nachricht senden button not visible (${btnCount} in DOM)`);
        await contactBtn.click({ timeout: 10000 });

        // Wait for message textarea
        const msgArea = page.locator([
            'textarea[name="message_body"]',
            'textarea[name="message"]',
            "#message-body",
            "textarea.contact-form-textarea",
            "textarea.form-control",
        ].join(", ")).first();
        await msgArea.waitFor({ state: "visible", timeout: 10000 });
        await msgArea.fill(messageText);

        // Submit
        const submitBtn = page.locator([
            'button[type="submit"]:has-text("Senden")',
            'button:has-text("Nachricht absenden")',
            'button:has-text("Absenden")',
            'input[type="submit"]',
        ].join(", ")).first();
        await submitBtn.click({ timeout: 5000 });

        // Brief settle time then check for visible errors
        await page.waitForTimeout(3000);

        const errEl = await page.$(".alert-danger, .has-error");
        if (errEl) {
            const txt = await errEl.textContent();
            if (txt?.trim()) throw new Error(`Form error: ${txt.trim().slice(0, 200)}`);
        }
    } finally {
        await browser.close();
    }
}

async function sendMessage(listing, messageText, maxAttempts = 3) {
    let lastErr;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            await sendMessageOnce(listing, messageText);
            return;
        } catch (err) {
            lastErr = err;
            log(`  → Send attempt ${attempt}/${maxAttempts} failed: ${err.message}`);
            if (attempt < maxAttempts) await sleep(attempt * 2000);
        }
    }
    throw lastErr;
}

async function main() {
    log(`Starting at ${new Date().toISOString()}, threshold=${SCORE_THRESHOLD}`);

    if (!OPENAI_API_KEY) {
        log("Fatal: OPENAI_API_KEY not set");
        process.exit(1);
    }

    const result = await callTool("query_table", {
        schema: "finance",
        table: "wg_gesucht_listings",
        where: { evaluated: { op: "eq", value: false } },
        orderBy: { column: "scraped_at", ascending: true },
        limit: 30,
    });

    const text = result?.result?.content?.[0]?.text || result?.content?.[0]?.text || "{}";
    let parsed; try { parsed = JSON.parse(text); } catch { parsed = {}; }
    const rows = parsed?.rows ?? [];
    if (!rows.length) {
        log("No un-evaluated listings");
        return;
    }

    log(`Evaluating ${rows.length} listing(s)`);

    for (const listing of rows) {
        try {
            log(`[${listing.listing_id}] ${listing.title}`);

            const hardReject =
                isStudentOnly(listing) ? "student-only listing" :
                isActuallyWG(listing) ? "shared flat disguised as apartment" :
                forbidsAnmeldung(listing) ? "Anmeldung not allowed" :
                null;

            if (hardReject) {
                log(`  → Skipped (${hardReject})`);
                await callTool("update_data", {
                    schema: "finance",
                    table: "wg_gesucht_listings",
                    pk: ["listing_id"],
                    data: [{ listing_id: listing.listing_id, evaluated: true, score: 0, evaluation_notes: `Skipped: ${hardReject}` }],
                });
                continue;
            }

            const rejectReason = isObviousReject(listing);
            if (rejectReason) {
                log(`  → Skipped (${rejectReason})`);
                await callTool("update_data", {
                    schema: "finance",
                    table: "wg_gesucht_listings",
                    pk: ["listing_id"],
                    data: [{ listing_id: listing.listing_id, evaluated: true, score: 0, evaluation_notes: `Skipped: ${rejectReason}` }],
                });
                continue;
            }

            const evaluation = await evaluateListing(listing, {
                commercial: isCommercial(listing),
                noProperBed: hasNoProperBed(listing),
                softWG: hasSoftWGSignals(listing),
            });
            log(`  score=${evaluation.score} | ${evaluation.reason.slice(0, 100)}`);

            let messageText = null;
            let messageSent = false;
            let messageSentAt = null;

            if (evaluation.score >= SCORE_THRESHOLD) {
                if (ENABLE_AUTO_MESSAGE) {
                    log(`  → Above threshold, generating message...`);
                    messageText = await generateMessage(listing, evaluation.highlights ?? "");

                    try {
                        await sendMessage(listing, messageText);
                        messageSent = true;
                        messageSentAt = new Date().toISOString();
                        log(`  → Message sent`);
                        await sleep(4000 + Math.random() * 4000);
                    } catch (sendErr) {
                        log(`  → Send failed: ${sendErr.message}`);
                    }
                } else {
                    log(`  → Above threshold, auto-send disabled`);
                }
            }

            await callTool("update_data", {
                schema: "finance",
                table: "wg_gesucht_listings",
                pk: ["listing_id"],
                data: [{
                    listing_id: listing.listing_id,
                    evaluated: true,
                    score: evaluation.score,
                    evaluation_notes: evaluation.reason,
                    message_text: messageText,
                    message_sent: messageSent,
                    ...(messageSentAt ? { message_sent_at: messageSentAt } : {}),
                }],
            });

            await sleep(600);
        } catch (err) {
            log(`  → Error: ${err.message}`);
            await callTool("update_data", {
                schema: "finance",
                table: "wg_gesucht_listings",
                pk: ["listing_id"],
                data: [{
                    listing_id: listing.listing_id,
                    evaluated: true,
                    score: null,
                    evaluation_notes: `Evaluation error: ${err.message}`,
                }],
            });
        }
    }

    log(`Done at ${new Date().toISOString()}`);
}

main().catch(err => {
    log(`Fatal: ${err.message}`);
    process.exit(1);
});
