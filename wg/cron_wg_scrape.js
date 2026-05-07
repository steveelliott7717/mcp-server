// /opt/supabase-mcp/custom/wg/cron_wg_scrape.js
// Scrapes WG-Gesucht search results over plain HTTP using cookies from session.json.

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { callTool } from "./mcp.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_FILE = path.join(__dirname, "session.json");

const HOME_URL = "https://www.wg-gesucht.de/";
const SEARCH_URL = "https://www.wg-gesucht.de/1-zimmer-wohnungen-und-wohnungen-in-Leipzig.77.1+2.1.0.html?categories%5B%5D=1&categories%5B%5D=2&rent_types%5B%5D=1&rent_types%5B%5D=2&min_size=30&rent_range=0%2C800&city_id=77&sort_order=0";
const MAX_PAGES = 1;
const START_JITTER_MAX_MS = Number(process.env.WG_SCRAPE_START_JITTER_MS || 180000);
const MAX_DETAILS_PER_RUN = Number(process.env.WG_SCRAPE_MAX_DETAILS || 5);
const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function parseNum(str) {
    if (!str) return null;
    // Extract first contiguous number sequence to avoid "55m2" → 552
    const m = String(str).match(/(\d[\d,.]*)/);
    if (!m) return null;
    const n = parseFloat(m[1].replace(",", "."));
    return Number.isFinite(n) ? n : null;
}

function parseDate(str) {
    if (!str) return null;
    const m = String(str).match(/(\d{2})\.(\d{2})\.(\d{4})/);
    if (m) return `${m[3]}-${m[2]}-${m[1]}`;
    if (/sofort/i.test(str)) return new Date().toISOString().split("T")[0];
    if (/\b(heute|\d+\s*(minute|minuten|stunde|stunden))\b/i.test(str)) return new Date().toISOString().split("T")[0];
    if (/\bgestern\b/i.test(str)) {
        const d = new Date();
        d.setDate(d.getDate() - 1);
        return d.toISOString().split("T")[0];
    }
    return null;
}

function decodeHtml(value = "") {
    return String(value)
        .replace(/&nbsp;/g, " ")
        .replace(/&sup2;/g, "²")
        .replace(/&euro;/g, "€")
        .replace(/&amp;/g, "&")
        .replace(/&quot;/g, '"')
        .replace(/&#039;|&apos;/g, "'")
        .replace(/<br\s*\/?>/gi, "\n")
        .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)))
        .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCharCode(parseInt(n, 16)));
}

function stripTags(html = "") {
    return decodeHtml(html)
        .replace(/<script[\s\S]*?<\/script>/gi, "")
        .replace(/<style[\s\S]*?<\/style>/gi, "")
        .replace(/<[^>]+>/g, " ")
        .replace(/[ \t]+\n/g, "\n")
        .replace(/\n[ \t]+/g, "\n")
        .replace(/[ \t]{2,}/g, " ")
        .replace(/\n{3,}/g, "\n\n")
        .trim();
}

function attr(tag, name) {
    const m = tag.match(new RegExp(`${name}=["']([^"']+)["']`, "i"));
    return m ? decodeHtml(m[1]) : "";
}

function parseToolRows(res) {
    const content = res?.result?.content || res?.content || [];
    const text = content[0]?.text;
    if (!text) return [];
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) return parsed;
    return parsed.rows || parsed.data || [];
}

function isInterstitialText(value) {
    return /cuba\.html|aktiver werbeblocker|adblock|werbeblocker|bitte bestätigen sie, dass sie ein mensch sind|confirm.*human|verify.*human/i.test(String(value || ""));
}

function isAdblockText(value) {
    return /aktiver werbeblocker|adblock|werbeblocker/i.test(String(value || ""));
}

function cookieHeader() {
    const session = JSON.parse(fs.readFileSync(SESSION_FILE, "utf8"));
    const now = Date.now() / 1000;
    return (session.cookies || [])
        .filter(c => /(^|\.)wg-gesucht\.de$/.test(c.domain))
        .filter(c => !c.expires || c.expires < 0 || c.expires > now)
        .map(c => `${c.name}=${c.value}`)
        .join("; ");
}

const COOKIE = cookieHeader();

async function fetchHtml(url, referer = HOME_URL) {
    const res = await fetch(url, {
        headers: {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "no-cache",
            "cookie": COOKIE,
            "referer": referer,
            "user-agent": USER_AGENT,
        },
        redirect: "follow",
    });

    const html = await res.text();
    if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
    if (/cuba\.html/i.test(res.url) || /bitte bestätigen sie, dass sie ein mensch sind/i.test(html)) {
        throw new Error(`WG-Gesucht interstitial/captcha page detected at ${res.url}`);
    }
    return { html, finalUrl: res.url };
}

function validateDetailForInsert(detail) {
    if (isInterstitialText(detail.title) || isInterstitialText(detail.page_title)) {
        throw new Error("Interstitial page detected");
    }
    if (!detail.title && !detail.desc_wohnung && !detail.costs?.rent && !detail.size_text) {
        throw new Error("No usable listing data scraped");
    }
}

async function getExistingIds(ids) {
    if (!ids.length) return new Set();
    const res = await callTool("query_table", {
        schema: "finance",
        table: "wg_gesucht_listings",
        where: { listing_id: { op: "in", value: ids } },
        select: "listing_id",
        limit: ids.length,
    });
    return new Set(parseToolRows(res).map(r => r.listing_id));
}

async function listingExists(listing_id) {
    const res = await callTool("query_table", {
        schema: "finance",
        table: "wg_gesucht_listings",
        where: { listing_id: { op: "eq", value: listing_id } },
        select: "listing_id",
        limit: 1,
    });
    return parseToolRows(res).some(r => r.listing_id === listing_id);
}

function parseSearchCards(html) {
    const cards = [];
    const re = /<div[^>]+id=["']liste-details-ad-(\d+)["'][\s\S]*?(?=<div[^>]+id=["']liste-details-ad-\d+["']|<ul[^>]+class=["'][^"']*pagination|<\/body>)/gi;
    let m;
    while ((m = re.exec(html))) {
        const block = m[0];
        const listing_id = m[1];
        const link = block.match(/<a[^>]+href=["']([^"']+\.html)["'][^>]*>/i);
        if (!link) continue;
        const tag = link[0];
        const href = decodeHtml(link[1]);
        const title = attr(tag, "title").replace(/^Anzeige ansehen:\s*/i, "").trim();
        cards.push({
            listing_id,
            url: href.startsWith("http") ? href : `https://www.wg-gesucht.de${href}`,
            title,
        });
    }
    return cards;
}

async function scrapeSearchPage(url) {
    const { html } = await fetchHtml(url, HOME_URL);
    if (!/mein-wg-gesucht|my-wg-gesucht|X-Refresh-Token|logout|abmelden/i.test(html)) {
        console.warn("[wg_scrape] Could not confirm logged-in marker in search HTML; continuing with saved cookies");
    }
    return parseSearchCards(html);
}

function parseKeyFacts(html) {
    const facts = {};
    const re = /<div[^>]*class=["'][^"']*text-center[^"']*["'][^>]*>[\s\S]*?<span[^>]*class=["'][^"']*key_fact_detail[^"']*["'][^>]*>([\s\S]*?)<\/span>[\s\S]*?<b[^>]*class=["'][^"']*key_fact_value[^"']*["'][^>]*>([\s\S]*?)<\/b>[\s\S]*?<\/div>/gi;
    let m;
    while ((m = re.exec(html))) {
        const label = stripTags(m[1]).toLowerCase();
        const value = stripTags(m[2]);
        if (/größe|groesse/.test(label)) facts.size_text = value;
        else if (/gesamtmiete|warmmiete/.test(label)) facts.rent_summary = value;
        else if (/zimmer/.test(label)) facts.rooms_text = value;
    }
    const text = stripTags(html);
    facts.size_text ||= (text.match(/Größe\s*:\s*([0-9,.]+\s*m2?)/i) || [])[1] || "";
    facts.rent_summary ||= (text.match(/Gesamtmiete\s*:\s*([0-9,.]+\s*€)/i) || [])[1] || "";
    facts.rooms_text ||= (text.match(/Zimmer\s*:\s*([0-9,.]+)/i) || [])[1] || "";
    return facts;
}

function parsePanelValues(html) {
    const costs = {};
    let available_text = "";
    let available_to_text = "";

    // Pair each detail label with the section_panel_value that immediately follows it
    // (within ~600 chars, and only if no other detail label appears in between).
    // This avoids misalignment from unpaired labels (address span, SCHUFA link, Online field).
    const re = /class=["'][^"']*section_panel_detail[^"']*["'][^>]*>([\s\S]*?)<\/(?:span|a|div)>([\s\S]{0,600}?)class=["'][^"']*section_panel_value[^"']*["'][^>]*>([\s\S]*?)<\/span>/gi;
    let m;
    while ((m = re.exec(html))) {
        const gap = m[2];
        // Skip if another detail label appears before the value (false pairing across rows)
        if (/section_panel_detail/i.test(gap)) continue;
        const label = stripTags(m[1]).toLowerCase().replace(/[:\s]+$/, "").trim();
        const value = stripTags(m[3]).trim();
        if (!label || !value) continue;
        if (/kaltmiete|grundmiete|^miete$/.test(label) && !costs.rent) costs.rent = value;
        else if (/nebenkosten/.test(label)) costs.additional = value;
        else if (/sonstige kosten/.test(label)) costs.other = value;
        else if (/kaution/.test(label)) costs.deposit = value;
        else if (/gesamt|warm/.test(label)) costs.total = value;
        else if (/frei ab|verfügbar ab|ab dem/.test(label)) available_text = value;
        else if (/frei bis|bis zum|befristet bis/.test(label)) available_to_text = value;
    }
    return { costs, available_text, available_to_text };
}

function parseAddress(html) {
    const text = stripTags(html);
    const m = text.match(/Adresse\s+([\s\S]*?)\s+Verfügbarkeit/i);
    if (!m) return "";
    return m[1]
        .replace(/\s+/g, " ")
        .trim();
}

function parseAvailability(html) {
    const text = stripTags(html);
    return {
        available_text: (text.match(/frei ab:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4}|sofort)/i) || [])[1] || "",
        available_to_text: (text.match(/frei bis:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})/i) || [])[1] || "",
        posted_text: (text.match(/Online:\s*([^\n]+?)(?:\s{2,}|$)/i) || [])[1] || "",
    };
}

function parseUtilityFeatures(html) {
    const start = html.search(/<div[^>]+class=["'][^"']*utility_icons[^"']*["']/i);
    if (start < 0) return [];
    const chunk = html.slice(start, start + 12000);
    const blocks = [...chunk.matchAll(/<div[^>]+class=["'][^"']*text-center[^"']*["'][^>]*>([\s\S]*?)<\/div>/gi)]
        .map(m => stripTags(m[1]))
        .map(t => t.replace(/\s+/g, " ").trim())
        .filter(Boolean)
        .filter(t => !/^Angaben zum Objekt$/i.test(t));
    return [...new Set(blocks)];
}

function inferRoomsFromListing(card = {}, detail = {}) {
    const source = `${card.url || ""} ${card.title || ""} ${detail.title || ""}`;
    if (/\b1[-\s]?zimmer\b|studio/i.test(source)) return "1";
    return "";
}

function sectionBetween(html, id, nextIds = []) {
    const start = html.search(new RegExp(`<[^>]+id=["']${id}["']`, "i"));
    if (start < 0) return null;
    let end = html.length;
    for (const next of nextIds) {
        const i = html.slice(start + 1).search(new RegExp(`<[^>]+id=["']${next}["']`, "i"));
        if (i >= 0) end = Math.min(end, start + 1 + i);
    }
    return stripTags(html.slice(start, end));
}

function parseDescriptions(html) {
    // freitext_3 can bleed into the required-docs / upsell panel — stop at known anchors
    const sonstigesStops = ["freitext_4", "rhs-contact-information", "utilities_rhs", "note_saved_feedback"];
    const rawSonstiges = sectionBetween(html, "freitext_3", sonstigesStops);
    // Truncate at "Benötigte Unterlagen" / upsell headings that follow the free-text section
    const truncate = (s) => {
        if (!s) return null;
        const i = s.search(/\n[ \t]*(?:Benötigte Unterlagen|Schon gewusst\?|WG-Gesucht\+|Statistiken|Profilcheck)\b/i);
        return (i > 0 ? s.slice(0, i) : s).trim() || null;
    };
    const sonstiges = truncate(rawSonstiges);
    return {
        desc_wohnung: truncate(sectionBetween(html, "freitext_0", ["freitext_1", "freitext_2", "freitext_3"])),
        desc_lage: truncate(sectionBetween(html, "freitext_1", ["freitext_2", "freitext_3"])),
        desc_sonstiges: sonstiges,
    };
}

function parseDetailHtml(html, card = {}) {
    const page_title = stripTags((html.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [])[1] || "");
    const h1 = stripTags((html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i) || [])[1] || "");
    const title = isAdblockText(h1) ? card.title : (h1 || card.title || "");
    const body_text = stripTags(html).slice(0, 3000);
    const facts = parseKeyFacts(html);
    const panels = parsePanelValues(html);
    const availability = parseAvailability(html);
    const descriptions = parseDescriptions(html);
    const address = stripTags((html.match(/<[^>]+id=["']mapAddress["'][^>]*>([\s\S]*?)<\/[^>]+>/i) || [])[1] || "") || parseAddress(html);
    const features = parseUtilityFeatures(html);
    const allDescText = [descriptions.desc_wohnung, descriptions.desc_lage, descriptions.desc_sonstiges, title].filter(Boolean).join(" ");
    // For description text, require a non-negated mention (avoids "kein Balkon", "ohne Keller", etc.)
    const hasPositiveDescMention = (kw) => {
        const re = new RegExp(kw, "gi");
        let m;
        while ((m = re.exec(allDescText)) !== null) {
            const before = allDescText.slice(Math.max(0, m.index - 50), m.index);
            const after = allDescText.slice(m.index + m[0].length, m.index + m[0].length + 30);
            if (/kein\w*|ohne|nicht\s*$/i.test(before)) continue;
            if (/^\s*(nicht|leider|kein)/i.test(after)) continue;
            return true;
        }
        return false;
    };
    const hasFeature = (kw) => features.some(f => new RegExp(kw, "i").test(f)) || hasPositiveDescMention(kw);

    return {
        title,
        page_title,
        body_text,
        address,
        size_text: facts.size_text || "",
        rooms_text: facts.rooms_text || inferRoomsFromListing(card, { title }),
        rent_summary: facts.rent_summary || "",
        costs: panels.costs,
        available_text: panels.available_text || availability.available_text,
        available_to_text: panels.available_to_text || availability.available_to_text,
        posted_text: availability.posted_text,
        ...descriptions,
        features: features.length ? features : null,
        has_balcony: hasFeature("Balkon|Terrasse|Garten"),
        has_elevator: hasFeature("Fahrstuhl|Aufzug|Lift"),
        has_washing_machine: hasFeature("Wasch"),
        has_dishwasher: hasFeature("Geschirrsp|Spülmaschine|Spuelmaschine"),
        has_basement: hasFeature("Keller"),
        has_bathroom: hasFeature("Bad|Dusche"),
        has_own_kitchen: hasFeature("Küche|Kueche|Kochnische"),
        furnished: hasFeature("möbliert|moebliert|Möbel"),
        // Derived from utility_icons labels, with description text fallback
        floor_level: (() => {
            const f = features.find(f => /^(\d+)\.\s*OG$/i.test(f) || /^EG$/i.test(f) || /^DG$/i.test(f));
            if (f) {
                if (/^EG$/i.test(f)) return 0;
                if (/^DG$/i.test(f)) return -1; // sentinel for attic
                return parseInt(f);
            }
            // Fallback: "im 3. Obergeschoss", "3. OG", "Erdgeschoss", "Dachgeschoss"
            const dm = allDescText.match(/\b(\d+)\.\s*(?:OG|Obergeschoss)\b/i) ||
                       allDescText.match(/\bim\s+(\d+)\.\s*(?:Stock|Etage)\b/i);
            if (dm) return parseInt(dm[1]);
            if (/\bErdgeschoss/i.test(allDescText)) return 0;
            if (/\bDachgeschoss/i.test(allDescText)) return -1;
            return null;
        })(),
        building_type: features.find(f => /^(Neubau|Altbau|Erstbezug|Denkmalschutz)$/i.test(f)) ||
            (allDescText.match(/\b(Neubau|Altbau|Erstbezug|Denkmalschutz)\b/i)?.[1] ?? null),
        floor_type: features.find(f => /Parkett|Laminat|Fliesen|Teppich|Dielen|Estrich|Linoleum/i.test(f)) || null,
        minutes_on_foot: (() => {
            const f = features.find(f => /minute[n]?\s*zu\s*fuß/i.test(f));
            if (!f) return null;
            const m = f.match(/(\d+)/);
            return m ? parseInt(m[1]) : null;
        })(),
    };
}

async function scrapeListingDetail(url, referer, card) {
    const { html } = await fetchHtml(url, referer);
    return parseDetailHtml(html, card);
}

async function insertListing(listing_id, url, detail, card = {}) {
    const title = isAdblockText(detail.title) ? (card.title || null) : (detail.title || card.title || null);
    const safeDetail = { ...detail, title };
    validateDetailForInsert(safeDetail);

    const row = {
        listing_id,
        url,
        title: safeDetail.title,
        address: detail.address || null,
        size_m2: parseNum(detail.size_text),
        rooms: parseNum(detail.rooms_text),
        rent: parseNum(detail.costs?.rent),
        additional_costs: parseNum(detail.costs?.additional),
        other_costs: parseNum(detail.costs?.other),
        deposit: parseNum(detail.costs?.deposit),
        total_rent: parseNum(detail.costs?.total || detail.rent_summary),
        available_from: parseDate(detail.available_text),
        available_to: parseDate(detail.available_to_text),
        posted_online: parseDate(detail.posted_text),
        description_wohnung: detail.desc_wohnung,
        description_lage: detail.desc_lage,
        description_sonstiges: detail.desc_sonstiges,
        features: detail.features,
        has_balcony: detail.has_balcony,
        has_elevator: detail.has_elevator,
        has_washing_machine: detail.has_washing_machine,
        has_dishwasher: detail.has_dishwasher,
        has_basement: detail.has_basement,
        has_bathroom: detail.has_bathroom,
        has_own_kitchen: detail.has_own_kitchen,
        furnished: detail.furnished,
        floor_level: detail.floor_level,
        building_type: detail.building_type || null,
        floor_type: detail.floor_type || null,
        minutes_on_foot: detail.minutes_on_foot,
        evaluated: false,
        message_sent: false,
    };

    Object.keys(row).forEach(k => row[k] === null && delete row[k]);

    const res = await callTool("insert_data", {
        schema: "finance",
        table: "wg_gesucht_listings",
        data: row,
    });
    const body = res?.result?.content?.[0]?.text || res?.content?.[0]?.text || "";
    let parsed;
    try { parsed = JSON.parse(body); } catch { parsed = {}; }
    if (parsed?.error) {
        if (await listingExists(listing_id)) {
            console.warn(`[wg_scrape] Insert returned error but row exists: ${listing_id}`);
            return;
        }
        throw new Error(`insert_data error: ${parsed.message || parsed.error}`);
    }
}

async function main() {
    console.log(`[wg_scrape] Starting at ${new Date().toISOString()}`);
    if (START_JITTER_MAX_MS > 0) {
        const jitter = Math.floor(Math.random() * START_JITTER_MAX_MS);
        console.log(`[wg_scrape] Startup jitter: waiting ${Math.round(jitter / 1000)}s before fetching`);
        await sleep(jitter);
    }

    await fetchHtml(HOME_URL, HOME_URL);
    await sleep(1200 + Math.random() * 800);

    let totalNew = 0;
    let detailBudget = MAX_DETAILS_PER_RUN;
    for (let pageNum = 0; pageNum < MAX_PAGES; pageNum++) {
        const pageUrl = SEARCH_URL.replace(".0.html", `.${pageNum}.html`);
        console.log(`[wg_scrape] Scraping search page ${pageNum + 1}: ${pageUrl}`);

        const cards = await scrapeSearchPage(pageUrl);
        console.log(`[wg_scrape] Found ${cards.length} listings on page ${pageNum + 1}`);
        if (!cards.length) break;

        const existingIds = await getExistingIds(cards.map(c => c.listing_id));
        const newCards = cards.filter(c => !existingIds.has(c.listing_id));
        console.log(`[wg_scrape] ${newCards.length} new listings to scrape`);
        if (newCards.length > detailBudget) {
            console.log(`[wg_scrape] Detail cap active: scraping first ${detailBudget} of ${newCards.length} new listings this run`);
        }

        for (const card of newCards.slice(0, detailBudget)) {
            try {
                console.log(`[wg_scrape] Scraping detail: ${card.listing_id} — ${card.url}`);
                const detail = await scrapeListingDetail(card.url, pageUrl, card);
                await insertListing(card.listing_id, card.url, detail, card);
                console.log(`[wg_scrape] ✅ Inserted: ${card.listing_id}`);
                totalNew++;
                detailBudget--;
                await sleep(1500 + Math.random() * 1000);
            } catch (err) {
                console.error(`[wg_scrape] ❌ Skipped listing ${card.listing_id}:`, err.message);
                if (/interstitial|captcha/i.test(err.message)) break;
            }
        }

        if (detailBudget <= 0) {
            console.log("[wg_scrape] Detail cap reached — stopping until next scheduled run");
            break;
        }

        if (newCards.length === 0) {
            console.log(`[wg_scrape] All listings on page ${pageNum + 1} already known — stopping pagination`);
            break;
        }
    }

    console.log(`[wg_scrape] Done. ${totalNew} new listings inserted at ${new Date().toISOString()}`);
}

main().catch(err => {
    console.error("[wg_scrape] Fatal error:", err);
    process.exit(1);
});
