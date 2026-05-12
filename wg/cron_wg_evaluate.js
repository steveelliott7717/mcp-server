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
const NACHRICHTEN_URL = "https://www.wg-gesucht.de/nachrichten.html";

function log(msg) { console.log(`[wg_evaluate] ${msg}`); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

const PLAYWRIGHT_EXECUTABLE = "/opt/supabase-mcp/custom/ms-playwright/chromium_headless_shell-1217/chrome-headless-shell-linux64/chrome-headless-shell";

function parseToolRows(res) {
    const content = res?.result?.content || res?.content || [];
    const text = content[0]?.text;
    if (!text) return [];
    let parsed; try { parsed = JSON.parse(text); } catch { return []; }
    return parsed.rows ?? parsed.data ?? (Array.isArray(parsed) ? parsed : []);
}

function extractStreet(address = "") {
    const m = address.match(/^(.+?)\s+\d{4,5}/);
    return m ? m[1].trim() : null;
}

async function hasSentToContact(contactName, address) {
    if (!contactName) return false;
    const street = extractStreet(address || "");
    const where = {
        contact_name: { op: "ilike", value: contactName },
        message_sent: { op: "eq", value: true },
    };
    if (street) where.address = { op: "ilike", value: `%${street}%` };
    const res = await callTool("query_table", {
        schema: "finance",
        table: "wg_gesucht_listings",
        select: "listing_id",
        where,
        limit: 1,
    });
    return parseToolRows(res).length > 0;
}

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

function hasFurnitureTakeover(listing) {
    const text = normalizeUiText([
        listing.title,
        listing.description_wohnung,
        listing.description_lage,
        listing.description_sonstiges,
        ...(Array.isArray(listing.features) ? listing.features : []),
    ].join(" "));

    const patterns = [
        /\bmoebeluebernahme\b/,
        /\bmoebel uebernahme\b/,
        /\bmoebel.*uebernommen\b/,
        /\bmoebel.*uebernehmen\b/,
        /\bkueche.*uebernommen\b/,
        /\bkueche.*uebernehmen\b/,
        /\bablose\b/,
        /\babstandszahlung\b/,
        /\beinrichtung.*uebernehmen\b/,
        /\bgegenstaende.*uebernehmen\b/,
    ];

    return patterns.some((pattern) => pattern.test(text));
}

function hasMandatoryFurnitureTakeover(listing) {
    const text = normalizeUiText([
        listing.description_wohnung,
        listing.description_sonstiges,
    ].join(" "));

    return /pflicht\s*(zur\s*)?uebernahme|muss\s*uebernommen|ist\s*zu\s*uebernehmen|pflichtuebernahme|verpflichtend.*uebernehmen|uebernahme.*pflicht/.test(text);
}

function parseFurnitureCost(listing) {
    const text = [listing.description_wohnung || "", listing.description_sonstiges || ""].join(" ");
    const matches = [...text.matchAll(/(\d[\d.,]+)\s*[€.]/g)];
    const values = matches
        .map(m => parseFloat(m[1].replace(/\./g, "").replace(",", ".")))
        .filter(v => v >= 200 && v <= 15000);
    return values.length ? Math.max(...values) : null;
}

function normalizeUiText(value = "") {
    return String(value)
        .toLowerCase()
        .replace(/ä/g, "ae")
        .replace(/ö/g, "oe")
        .replace(/ü/g, "ue")
        .replace(/ß/g, "ss")
        .replace(/[\u2010-\u2015]/g, "-")
        .replace(/[^\p{L}\p{N}\s-]+/gu, " ")
        .replace(/\s+/g, " ")
        .trim();
}

function isPendingSendRetry(listing) {
    return ENABLE_AUTO_MESSAGE &&
        !listing.message_sent &&
        typeof listing.score === "number" &&
        listing.score >= SCORE_THRESHOLD &&
        typeof listing.message_text === "string" &&
        listing.message_text.trim().length > 0;
}

function isPermanentSendBlockError(error) {
    const text = String(error?.message ?? error ?? "").toLowerCase();
    return text.includes("aktuell kann keine nachricht an diesen nutzer") ||
        text.includes("kann keine nachricht an diesen nutzer") ||
        text.includes("send blocked by wg/recipient");
}

function isObviousReject(listing) {
    const today = new Date();
    const moveIn = new Date("2026-06-01");
    const warm = listing.total_rent ?? (listing.rent + (listing.additional_costs ?? 0));

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

    // Tiny studios are an automatic reject regardless of location
    if (listing.size_m2 && listing.size_m2 < 25) {
        return `too small (${listing.size_m2}m²)`;
    }

    // Total rent clearly over budget
    if (warm > 820) return `over budget (${warm}€ warm)`;

    // Mandatory furniture takeover with a stated cost over 1000€
    if (hasMandatoryFurnitureTakeover(listing)) {
        const cost = parseFurnitureCost(listing);
        if (cost && cost > 1000) return `mandatory furniture takeover (${cost}€)`;
    }

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
- Size: under 25m² is already rejected by pre-filter; 25–30m² should receive a noticeable penalty unless the price is exceptionally low
- Value: size-to-price ratio is a major factor — e.g. 40m² at 550€ is great, 30m² at 750€ is poor
- Location: central or well-connected neighborhoods get a small bonus, including Zentrum, Zentrum-Süd, Zentrum-Ost, Zentrum-West, Zentrum-Nord, Südvorstadt, Reudnitz, Volkmarsdorf, Neustadt-Neuschönefeld, Connewitz, and Plagwitz. Do not penalize grittier but central neighborhoods if transit access is good; outer suburbs = neutral
- Features: furnished, washing machine = small bonuses
- Furniture takeover: distinguish carefully — optional takeover (nice extras at negotiable price) is a small positive; MANDATORY takeover (Pflicht zur Übernahme, muss übernommen werden) is an additional upfront cost to the tenant, treat it as a moderate negative unless the items are genuinely essential (bed, kitchen) and no large fee is mentioned
- Description quality: vague or boilerplate-only = slight penalty
- "WG geeignet", "2er WG", or "3er WG" on a full apartment listing is only a small penalty, not a rejection
- Commercial listing (GmbH/Immobilien): harder to get, more competitive — apply −10 penalty
- Sleeping arrangement: no dedicated bed (Schlafsofa only, or explicit "kein Bett") = −8; furnished with a proper Bett mentioned = neutral${flags.commercial ? "\n\nFLAGS:\n- Commercial agency listing detected. Apply the −10 commercial penalty." : ""}${flags.noProperBed ? (flags.commercial ? "\n- No proper bed detected (Schlafsofa only or explicit 'kein Bett'). Apply the −8 sleeping arrangement penalty." : "\n\nFLAGS:\n- No proper bed detected (Schlafsofa only or explicit 'kein Bett'). Apply the −8 sleeping arrangement penalty.") : ""}${flags.softWG ? ((flags.commercial || flags.noProperBed) ? "\n- Soft WG wording detected ('WG geeignet' / roommate-suitable wording). Treat this as only a small penalty, not a rejection." : "\n\nFLAGS:\n- Soft WG wording detected ('WG geeignet' / roommate-suitable wording). Treat this as only a small penalty, not a rejection.") : ""}${flags.mandatoryFurnitureTakeover ? ((flags.commercial || flags.noProperBed || flags.softWG) ? `\n- MANDATORY furniture/kitchen takeover detected (Pflicht zur Übernahme). This is an additional upfront cost for the tenant — apply a moderate penalty. Do not treat as a furnished bonus.${flags.furnitureCost ? ` Stated cost: ${flags.furnitureCost}€.` : ""}` : `\n\nFLAGS:\n- MANDATORY furniture/kitchen takeover detected (Pflicht zur Übernahme). This is an additional upfront cost for the tenant — apply a moderate penalty. Do not treat as a furnished bonus.${flags.furnitureCost ? ` Stated cost: ${flags.furnitureCost}€.` : ""}`) : flags.furnitureTakeover ? ((flags.commercial || flags.noProperBed || flags.softWG) ? "\n- Optional furniture/kitchen takeover wording detected. Treat the listing as partially furnished / move-in-friendly even if the structured furnished field is false." : "\n\nFLAGS:\n- Optional furniture/kitchen takeover wording detected. Treat the listing as partially furnished / move-in-friendly even if the structured furnished field is false.") : ""}

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
    const styles = [
        "short and direct — 3 to 5 sentences total, no filler",
        "conversational — medium length, ask one specific question about the apartment",
        "warm and personal — mention something concrete from the listing description, vary paragraph structure",
    ];
    const style = styles[Math.floor(Math.random() * styles.length)];

    const prompt = `Write a German apartment inquiry message from Steven Elliott to a landlord on WG-Gesucht.

LISTING: "${listing.title}" in ${district}, ${listing.size_m2}m², ${listing.rent}€/month
HIGHLIGHTS: ${highlights}
STYLE FOR THIS MESSAGE: ${style}

REQUIRED FACTS TO INCLUDE (weave them in naturally — do not list them):
- Steven is moving to Leipzig in June and looking for a few months, ideally longer
- He has a Sperrkonto (Blocked Account) as financial proof
- He wants to ask about Anmeldung (Wohnungsgeberbestätigung) if not already obvious from the listing

RULES:
- Write the full message, ready to send — start with "Hallo," and end with "Steven Elliott"
- Use "Viele Grüße", "Beste Grüße", or "Mit freundlichen Grüßen" — vary it
- Do NOT use the phrase "ruhiger, zuverlässiger Mieter und Nichtraucher" — find a more natural way to convey reliability if needed, or omit it
- Do NOT use "besonders attraktiv" or other stock filler phrases
- Reference something specific from the listing — a feature, the neighborhood, the size, the price — not just the district name
- Vary sentence length and paragraph structure based on the style above
- Avoid generic emotional filler or imagined lifestyle language ("Es scheint ein schöner Ort zu sein", "perfekt für mich") unless directly supported by something concrete in the listing
- Sound like a real person, not a template

Output only the message text, nothing else.`;

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

        // Navigate directly to the contact form page (nachricht-senden/<listing-path>)
        const listingPath = listing.url.replace("https://www.wg-gesucht.de/", "");
        const contactUrl = `https://www.wg-gesucht.de/nachricht-senden/${listingPath}`;
        await page.goto(contactUrl, { waitUntil: "domcontentloaded", timeout: 30000 });

        if (/cuba\.html/i.test(page.url())) {
            throw new Error("Rate limited / captcha detected");
        }
        if (/login|signin/i.test(page.url())) {
            throw new Error("Session expired — redirected to login");
        }

        // Dismiss cookie banner if present
        try { await page.click("#cmpwelcomebtnyes", { timeout: 3000 }); } catch {}

        // Fill the message textarea
        const msgArea = page.locator("textarea").first();
        await msgArea.waitFor({ state: "visible", timeout: 10000 });
        await msgArea.fill(messageText);

        // Remove #sec_advice modal via JS — it overlays the submit button on this page too
        await page.evaluate(() => {
            const el = document.getElementById("sec_advice");
            if (el) { el.classList.remove("in"); el.style.display = "none"; }
            document.querySelectorAll(".modal-backdrop").forEach(b => b.remove());
            document.body.classList.remove("modal-open");
        });
        await page.waitForTimeout(300);

        // Click the Senden button — force:true as final safety net against any overlay
        const submitBtn = page.locator('button:has-text("Senden"), input[type="submit"]').first();
        await submitBtn.waitFor({ state: "visible", timeout: 5000 });
        await submitBtn.click({ timeout: 5000, force: true });

        await page.waitForTimeout(4000);
        await page.screenshot({ path: path.join(__dirname, "send_result.png") });

        const finalUrl = page.url();
        console.log(`[wg_evaluate]   → Post-submit URL: ${finalUrl}`);

        const errEl = await page.$(".alert-danger, .has-error");
        if (errEl) {
            const txt = await errEl.textContent();
            if (txt?.trim()) {
                const clean = txt.trim().slice(0, 200);
                if (clean.toLowerCase().includes("aktuell kann keine nachricht an diesen nutzer")) {
                    throw new Error(`Send blocked by WG/recipient: ${clean}`);
                }
                throw new Error(`Form error: ${clean}`);
            }
        }

        // Verify the new conversation is visible in the inbox before we mark this as sent.
        await page.goto(NACHRICHTEN_URL, { waitUntil: "domcontentloaded", timeout: 30000 });
        await page.waitForTimeout(3000);
        await page.screenshot({ path: path.join(__dirname, "send_inbox_check.png") });

        const inboxText = normalizeUiText(await page.locator("body").innerText());
        const titleProbe = normalizeUiText(listing.title).slice(0, 24);
        if (!titleProbe || !inboxText.includes(titleProbe)) {
            throw new Error(`Sent message not confirmed in inbox for title "${listing.title}"`);
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

            if (listing.contact_name && await hasSentToContact(listing.contact_name, listing.address)) {
                log(`  → Skipped: already contacted ${listing.contact_name}`);
                await callTool("update_data", {
                    schema: "finance",
                    table: "wg_gesucht_listings",
                    pk: ["listing_id"],
                    data: [{ listing_id: listing.listing_id, evaluated: true, score: 0, evaluation_notes: `Skipped: already contacted ${listing.contact_name}` }],
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

            if (isPendingSendRetry(listing)) {
                log(`  → Retrying previously generated message...`);

                try {
                    await sendMessage(listing, listing.message_text);
                    const messageSentAt = new Date().toISOString();
                    log(`  → Message sent`);

                    await callTool("update_data", {
                        schema: "finance",
                        table: "wg_gesucht_listings",
                        pk: ["listing_id"],
                        data: [{
                            listing_id: listing.listing_id,
                            evaluated: true,
                            message_sent: true,
                            message_sent_at: messageSentAt,
                        }],
                    });

                    await sleep(4000 + Math.random() * 4000);
                } catch (sendErr) {
                    if (isPermanentSendBlockError(sendErr)) {
                        log(`  → Send permanently blocked: ${sendErr.message}`);
                        await callTool("update_data", {
                            schema: "finance",
                            table: "wg_gesucht_listings",
                            pk: ["listing_id"],
                            data: [{
                                listing_id: listing.listing_id,
                                evaluated: true,
                                message_sent: false,
                                evaluation_notes: `Send blocked by WG/recipient: ${sendErr.message}`,
                            }],
                        });
                    } else {
                        log(`  → Send failed, will retry next run: ${sendErr.message}`);
                        await callTool("update_data", {
                            schema: "finance",
                            table: "wg_gesucht_listings",
                            pk: ["listing_id"],
                            data: [{
                                listing_id: listing.listing_id,
                                evaluated: false,
                                message_sent: false,
                            }],
                        });
                    }
                }

                continue;
            }

            const mandatoryFurniture = hasMandatoryFurnitureTakeover(listing);
            const evaluation = await evaluateListing(listing, {
                commercial: isCommercial(listing),
                noProperBed: hasNoProperBed(listing),
                softWG: hasSoftWGSignals(listing),
                furnitureTakeover: hasFurnitureTakeover(listing),
                mandatoryFurnitureTakeover: mandatoryFurniture,
                furnitureCost: mandatoryFurniture ? parseFurnitureCost(listing) : null,
            });
            log(`  score=${evaluation.score} | ${evaluation.reason.slice(0, 100)}`);

            let messageText = null;
            let messageSent = false;
            let messageSentAt = null;
            let shouldStayQueued = false;
            let sendFailureNote = null;

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
                        if (isPermanentSendBlockError(sendErr)) {
                            sendFailureNote = `Send blocked by WG/recipient: ${sendErr.message}`;
                        } else {
                            shouldStayQueued = true;
                        }
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
                    evaluated: sendFailureNote ? true : !shouldStayQueued,
                    score: evaluation.score,
                    evaluation_notes: sendFailureNote || evaluation.reason,
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
