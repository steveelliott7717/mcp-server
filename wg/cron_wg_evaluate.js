// /opt/supabase-mcp/custom/wg/cron_wg_evaluate.js
// Evaluates un-scored WG-Gesucht listings and auto-sends messages above threshold.

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { callTool } from "./mcp.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_FILE = path.join(__dirname, "session.json");

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const SCORE_THRESHOLD = Number(process.env.WG_SCORE_THRESHOLD || 60);
const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

function log(msg) { console.log(`[wg_evaluate] ${msg}`); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function cookieHeader() {
    const session = JSON.parse(fs.readFileSync(SESSION_FILE, "utf8"));
    const now = Date.now() / 1000;
    return (session.cookies || [])
        .filter(c => /(\.|^)wg-gesucht\.de$/.test(c.domain))
        .filter(c => !c.expires || c.expires < 0 || c.expires > now)
        .map(c => `${c.name}=${c.value}`)
        .join("; ");
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
        listing.description_sonstiges,
    ].join(" ").toLowerCase();

    return text.includes("immatrikulationsbescheinigung") ||
        text.includes("studentenwohnheim") ||
        text.includes("nur für studenten");
}

function isActuallyWG(listing) {
    const text = [listing.title, listing.description_wohnung, listing.description_sonstiges]
        .join(" ").toLowerCase();
    return text.includes("mitbewohner") ||
        text.includes("wg-zimmer") ||
        text.includes("gemeinschaftsküche") ||
        text.includes("shared kitchen") ||
        text.includes("room in a shared") ||
        text.includes("room in shared") ||
        text.includes("shared apartment");
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
- Commercial listing (GmbH/Immobilien): harder to get, more competitive — apply −10 penalty${flags.commercial ? "\n\nFLAGS: This listing appears to be from a commercial agency (GmbH/Immobilien). Apply the −10 commercial penalty." : ""}

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
    const COOKIE = cookieHeader();

    // Fetch listing page to get CSRF token and landlord user ID
    const pageRes = await fetch(listing.url, {
        headers: {
            cookie: COOKIE,
            "user-agent": USER_AGENT,
            accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "de-DE,de;q=0.9,en;q=0.8",
            referer: "https://www.wg-gesucht.de/",
        },
    });

    if (!pageRes.ok || /cuba\.html/i.test(pageRes.url)) {
        throw new Error(`Cannot load listing page (${pageRes.status} / ${pageRes.url}) — rate limited or session expired`);
    }

    const html = await pageRes.text();

    const csrfMatch = html.match(/name=["']?csrf_token["']?[^>]*value=["']([^"']{10,})["']/i)
        || html.match(/"csrf_token"\s*:\s*"([^"]{10,})"/i);
    if (!csrfMatch) throw new Error("CSRF token not found on listing page");
    const csrfToken = csrfMatch[1];

    const userIdMatch = html.match(/user_id\s*[=:]\s*['"]?(\d+)/i)
        || html.match(/data-user-id=["'](\d+)/i)
        || html.match(/\/nutzer\/(\d+)/)
        || html.match(/\/profile\/(\d+)/);
    if (!userIdMatch) throw new Error("Landlord user ID not found on listing page");
    const landlordId = userIdMatch[1];

    const adTypeMatch = html.match(/['"](ad_type|anzeigen_typ)['"]\s*[=:]\s*['"]?(\d)/i);
    const adType = adTypeMatch?.[2] || "1";

    const body = new URLSearchParams({
        csrf_token: csrfToken,
        user_id: landlordId,
        ad_id: listing.listing_id,
        ad_type: adType,
        message: messageText,
        action: "create_contact_offer",
    });

    const sendRes = await fetch("https://www.wg-gesucht.de/ajax/conversations.php", {
        method: "POST",
        headers: {
            cookie: COOKIE,
            "user-agent": USER_AGENT,
            "content-type": "application/x-www-form-urlencoded",
            referer: listing.url,
            "x-requested-with": "XMLHttpRequest",
        },
        body: body.toString(),
    });

    const responseText = await sendRes.text();
    if (!sendRes.ok) throw new Error(`Send failed ${sendRes.status}: ${responseText.slice(0, 300)}`);

    try {
        const json = JSON.parse(responseText);
        if (json.errors?.length || json.error) {
            throw new Error(`API error: ${JSON.stringify(json.errors || json.error)}`);
        }
    } catch (e) {
        if (e.message.startsWith("API error:")) throw e;
        // Non-JSON 200 OK — treat as success
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

            const evaluation = await evaluateListing(listing, { commercial: isCommercial(listing) });
            log(`  score=${evaluation.score} | ${evaluation.reason.slice(0, 100)}`);

            let messageText = null;
            let messageSent = false;
            let messageSentAt = null;

            if (evaluation.score >= SCORE_THRESHOLD) {
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
