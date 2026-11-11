// /opt/supabase-mcp/custom/flights/cron_matrix_watch.js
//
// Fare-level watch through ITA Matrix (matrix.itasoftware.com), alongside the
// SerpApi price watch in cron_flight_watch.js. Google Flights gives a price;
// Matrix shows what the price is made of: fare basis, booking class, base fare
// vs carrier surcharges (YQ/YR) vs taxes, and the fare rules. One row per date
// pair per run goes to finance.flight_fare_checks.
//
// Which watches: active rows of finance.flight_watches with a `matrix` jsonb,
// e.g. {"stops":0,"origins":["FRA"],"sales_city":"FRA"}. Date pairs come from
// the watch exactly as in cron_flight_watch.js (outbound_dates/return_dates or
// the from–to ranges). `origins`/`destinations` override the watch's airports
// (Matrix gets one airport per side here); `stops` 0 = nonstop only.
//
// Alert: a push when the cheapest fare's fare basis or booking class changes
// for a date pair (e.g. class B sold out → the next bucket), the early warning
// before the price itself moves. The first sighting of a pair is the baseline.
//
// Matrix has no API; each search drives the real site through the server's
// browser_flow tool (~30–60 s). Keep it to a few runs a day.
//
// Flags: --dry-run (search and print, write and push nothing)
//        --pair YYYY-MM-DD:YYYY-MM-DD  (only this pair)  --sales-city XXX (override)
//        --watch N --matrix '{"stops":0,...}'  (one watch, with this setting instead of its own)
import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import { callTool } from "./mcp.js";

const NOTIFY_CATEGORY = process.env.FLIGHT_NOTIFY_CATEGORY || "purchases";
const RULE_CATEGORIES = [5, 15, 16];   // advance purchase/ticketing, sales restrictions, penalties
const PAUSE_MS = 20000;                // between searches, and before a retry

const argv = process.argv.slice(2);
const DRY_RUN = argv.includes("--dry-run");
const argVal = (k) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : null);
const ONLY_PAIR = argVal("--pair");
const SALES_CITY = argVal("--sales-city");
const ONLY_WATCH = argVal("--watch") != null ? Number(argVal("--watch")) : null;
const MATRIX_OVERRIDE = argVal("--matrix") ? JSON.parse(argVal("--matrix")) : null;

const runId = Date.now().toString(36);
const log = (msg) => console.log(`[matrix_watch] [${runId}] ${msg}`);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ---------------------------------------------------------------- MCP helpers

function toolJson(res, what) {
    const text = res?.result?.content?.[0]?.text ?? res?.content?.[0]?.text;
    if (text == null) {
        if (res?.error) throw new Error(`${what}: ${JSON.stringify(res.error).slice(0, 300)}`);
        return res;
    }
    let parsed;
    try { parsed = JSON.parse(text); } catch { throw new Error(`${what}: non-JSON tool response: ${text.slice(0, 200)}`); }
    if (parsed && typeof parsed === "object" && parsed.error) throw new Error(`${what}: ${parsed.message || parsed.error}`);
    return parsed;
}

const rowsOf = (parsed) => parsed?.rows ?? parsed?.data ?? (Array.isArray(parsed) ? parsed : []);
const queryRows = async (args) => rowsOf(toolJson(await callTool("query_table", args), `query ${args.table}`));

// ---------------------------------------------------------------- dates

const isoDate = (d) => d.toISOString().slice(0, 10);
function* eachDate(from, to) {
    for (let d = new Date(`${String(from).slice(0, 10)}T00:00:00Z`); d <= new Date(`${String(to).slice(0, 10)}T00:00:00Z`); d.setUTCDate(d.getUTCDate() + 1))
        yield isoDate(d);
}
function datesFor(list, from, to) {
    if (Array.isArray(list) && list.length) return [...new Set(list.map(d => String(d).slice(0, 10)))].sort();
    return from ? [...eachDate(from, to)] : [];
}
const nights = (a, b) => Math.round((Date.parse(b) - Date.parse(a)) / 86400000);

function pairsFor(w, today) {
    const out = [];
    const returns = datesFor(w.return_dates, w.return_from, w.return_to);
    for (const o of datesFor(w.outbound_dates, w.outbound_from, w.outbound_to)) {
        if (o <= today) continue;
        for (const r of returns) {
            if (r <= o) continue;
            const n = nights(o, r);
            if (w.min_nights != null && n < w.min_nights) continue;
            if (w.max_nights != null && n > w.max_nights) continue;
            out.push({ outbound_date: o, return_date: r });
        }
    }
    return out;
}

// ---------------------------------------------------------------- Matrix

const mdy = (iso) => { const [y, m, d] = iso.split("-").map(Number); return `${m}/${d}/${y}`; };
const STOPS_LABEL = { 0: "Nonstop", 1: "Up to 1 stop", 2: "Up to 2 stops" };

// Runs in the page: wait for the result list, open the cheapest itinerary.
const PICK_CHEAPEST = `return (async()=>{
  const price = t => { const m = t.match(/€([\\d,]+(?:\\.\\d+)?)/); return m ? Number(m[1].replace(/,/g,'')) : null; };
  let rows = [];
  for (let i = 0; i < 90; i++) {
    rows = [...document.querySelectorAll('tr')].filter(r => price(r.innerText) != null && /\\d{1,2}:\\d{2}\\s?[AP]M/.test(r.innerText));
    if (rows.length) break;
    if (/no (flights|results)|could not find/i.test(document.body.innerText)) return { error: 'no results' };
    await new Promise(r => setTimeout(r, 1000));
  }
  if (!rows.length) return { error: 'results did not load', spinner: !!document.querySelector('mat-spinner') };
  const best = rows.reduce((a, b) => price(b.innerText) < price(a.innerText) ? b : a);
  const listed = rows.length, listedPrice = price(best.innerText);
  (best.querySelector('a,button,[role=button]') || best).click();
  return { listed, listed_price: listedPrice };
})()`;

// Runs in the page: wait for the itinerary details, return their text.
const READ_ITINERARY = `return (async()=>{
  for (let i = 0; i < 45; i++) {
    if (location.pathname.includes('itin') && /Fare for \\d+ adult/.test(document.body.innerText)) break;
    await new Promise(r => setTimeout(r, 1000));
  }
  return { path: location.pathname, text: document.body.innerText };
})()`;

// Runs in the page: open the first fare's rules and return the wanted categories.
const READ_RULES = `return (async()=>{
  const b = [...document.querySelectorAll('button,a')].find(e => e.innerText.trim() === 'rules');
  if (!b) return { error: 'no rules link' };
  b.click();
  let t = '';
  for (let i = 0; i < 30; i++) {
    const d = document.querySelector('mat-dialog-container,.cdk-overlay-pane');
    t = d ? d.innerText : '';
    if (t.length > 500 && !document.querySelector('mat-dialog-container mat-spinner')) break;
    await new Promise(r => setTimeout(r, 1000));
  }
  const want = new RegExp('^Category (' + ${JSON.stringify(RULE_CATEGORIES.join("|"))} + '):');
  return { categories: t.split(/\\n(?=Category \\d+:)/).filter(p => want.test(p)).map(p => p.slice(0, 4000)) };
})()`;

async function matrixSearch({ origin, destination, outbound, ret, stops, currency, salesCity }) {
    const steps = [
        // "networkidle" timed out once on the 3rd search in a row; the form being there is what matters.
        { op: "goto", url: "https://matrix.itasoftware.com/search", wait_until: "domcontentloaded", timeout_ms: 45000 },
        { op: "click", selector: "(//input[@placeholder='Add airport'])[1]", timeout_ms: 30000 },
        { op: "type", selector: "(//input[@placeholder='Add airport'])[1]", text: origin },
        { op: "click", selector: "mat-option >> nth=0", timeout_ms: 8000 },
        { op: "click", selector: "(//input[@placeholder='Add airport'])[2]" },
        { op: "type", selector: "(//input[@placeholder='Add airport'])[2]", text: destination },
        { op: "click", selector: "mat-option >> nth=0", timeout_ms: 8000 },
        { op: "fill", selector: "input[placeholder='Start date']", value: mdy(outbound) },
        { op: "fill", selector: "input[placeholder='End date']", value: mdy(ret) },
    ];
    if (STOPS_LABEL[stops]) steps.push(
        { op: "click", selector: "mat-select[formcontrolname='stops']" },
        { op: "click", selector: `mat-option:has-text('${STOPS_LABEL[stops]}')`, timeout_ms: 8000 });
    steps.push(
        { op: "click", selector: "input[formcontrolname='currency']" },
        { op: "type", selector: "input[formcontrolname='currency']", text: currency },
        { op: "click", selector: `mat-option:has-text('${currency}')`, timeout_ms: 8000 },
        { op: "click", selector: "input[formcontrolname='salesCity']" },
        { op: "type", selector: "input[formcontrolname='salesCity']", text: salesCity },
        { op: "click", selector: "mat-option >> nth=0", timeout_ms: 8000 },
        { op: "click", selector: "button:has-text('Search') >> nth=-1" },
        { op: "evaluate", script: PICK_CHEAPEST },
        { op: "evaluate", script: READ_ITINERARY },
        { op: "evaluate", script: READ_RULES },
    );
    const out = toolJson(await callTool("browser_flow", {
        options: { stealth: true, locale: "en-US", timezone: "Europe/Berlin" }, steps,
    }), "browser_flow");
    const evals = (out.results || []).filter(r => r.op === "evaluate").map(r => r.result);
    const [pick, itin, rules] = evals;
    if (!pick || pick.error) throw new Error(`search: ${pick?.error || "no result"}${pick?.spinner ? " (spinner still running)" : ""}`);
    if (!itin?.text || !/Fare for \d+ adult/.test(itin.text)) throw new Error("itinerary details did not load");
    return { pick, itinerary: parseItinerary(itin.text), rules: rules?.categories || null, rules_error: rules?.error || null };
}

const money = (s) => (s == null ? null : Number(String(s).replace(/,/g, "")));

/** Pull the fare structure out of the itinerary page's text. */
function parseItinerary(text) {
    const lines = text.split("\n").map(s => s.trim());
    const fares = [...text.matchAll(/Fare \d+: Carrier (\w+) (\S+) (\w{3}) to (\w{3})/g)]
        .map(m => ({ carrier: m[1], basis: m[2], from: m[3], to: m[4] }));
    const bookingCodes = [...text.matchAll(/booking code (\w+)/g)].map(m => m[1]);
    const base = [...text.matchAll(/Covers [^\n]+\n+€([\d,.]+)/g)].reduce((s, m) => s + money(m[1]), 0);
    const surcharge = (code) => { const m = text.match(new RegExp(`\\(${code}\\)\\n+€([\\d,.]+)`)); return m ? money(m[1]) : 0; };
    const total = money(text.match(/Fare for \d+ adult\n+€([\d,.]+)/)?.[1]);
    const yq = surcharge("YQ"), yr = surcharge("YR");
    const flights = [];
    for (let i = 0; i < lines.length; i++) {
        const m = lines[i].match(/^([A-Za-z][A-Za-z .'-]+?) (\d{1,4})$/);
        if (m && /\((\w)\)$/.test(lines[i + 2] || "")) flights.push(`${m[1]} ${m[2]} ${lines[i + 2].match(/\((\w)\)$/)[1]}`);
    }
    return {
        total, base_fare: Math.round(base * 100) / 100, yq, yr,
        taxes: total != null ? Math.round((total - base - yq - yr) * 100) / 100 : null,
        fare_basis: fares.map(f => f.basis).join(" / ") || null,
        booking_codes: bookingCodes.join("/") || null,
        carriers: [...new Set(fares.map(f => f.carrier))].join(" + ") || null,
        flights: flights.join(" · ") || null,
        changeable: /Changes to this ticket will incur a penalty fee/.test(text) ? "fee"
            : /Changes to this ticket are free|No change fee/i.test(text) ? "free"
            : /not changeable|Changes are not (permitted|allowed)/i.test(text) ? "no" : null,
        refundable: /non-refundable/i.test(text) ? false : /refundable/i.test(text) ? true : null,
        fare_construction: text.match(/Fare Construction[^\n]*\n+([^\n]+)/)?.[1] || null,
    };
}

// ---------------------------------------------------------------- alerts

const dayLabel = (d) => new Date(`${d}T00:00:00Z`).toLocaleDateString("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
// The alert compares fare basis only: for the same AA Basic fare Matrix shows
// booking code B on one run and O (the bucket it sells from) on the next, while
// the fare basis' first letter already carries the bucket (ONN4C8B4 → O).
const fareKey = (r) => r.fare_basis || "?";
const fareLabel = (r) => `${r.fare_basis || "?"} (${r.booking_codes || "?"})`;

async function pushChange(w, pair, prev, cur) {
    const title = `✈️ ${w.label}: fare changed — ${dayLabel(pair.outbound_date)} → ${dayLabel(pair.return_date)}`;
    const body = [
        `Fare basis (class): ${fareLabel(prev)} → ${fareLabel(cur)}`,
        `Matrix total: €${prev.total} → €${cur.total}${cur.base_fare != null ? ` (base €${cur.base_fare}, YQ/YR €${cur.yq + cur.yr})` : ""}`,
        cur.flights ? `Flights: ${cur.flights}` : null,
        "The previous fare bucket is no longer the cheapest — the price may follow.",
    ].filter(Boolean).join("\n");
    toolJson(await callTool("notify_push", { provider: "pushover", category: NOTIFY_CATEGORY, title, body }), "notify_push");
}

// ---------------------------------------------------------------- main

async function main() {
    const now = new Date();
    const today = isoDate(now);
    log(`start ${now.toISOString()} dry_run=${DRY_RUN}`);

    const watches = (await queryRows({
        schema: "finance", table: "flight_watches",
        where: { active: { op: "eq", value: true } }, orderBy: { column: "id", ascending: true },
    }))
        .filter(w => ONLY_WATCH == null || w.id === ONLY_WATCH)
        .map(w => (MATRIX_OVERRIDE ? { ...w, matrix: MATRIX_OVERRIDE } : w))
        .filter(w => w.matrix && typeof w.matrix === "object");
    log(`${watches.length} active watch(es) with a matrix setting: ${watches.map(w => `#${w.id} ${w.label}`).join(", ") || "none"}`);

    let searches = 0, inserted = 0, alerts = 0, failures = 0;
    for (const w of watches) {
        const m = w.matrix;
        const origin = (m.origins || w.origins)[0];
        const destination = (m.destinations || w.destinations)[0];
        const salesCity = SALES_CITY || m.sales_city || origin;
        let pairs = pairsFor(w, today);
        if (ONLY_PAIR) pairs = pairs.filter(p => `${p.outbound_date}:${p.return_date}` === ONLY_PAIR);
        for (const pair of pairs) {
            const when = `${pair.outbound_date}→${pair.return_date}`;
            const row = {
                watch_id: w.id, source: "matrix", origin, destination,
                outbound_date: pair.outbound_date, return_date: pair.return_date,
                sales_city: salesCity, currency: w.currency || "EUR",
            };
            if (searches) await sleep(PAUSE_MS);
            searches++;
            try {
                const t0 = Date.now();
                const args = {
                    origin, destination, outbound: pair.outbound_date, ret: pair.return_date,
                    stops: m.stops, currency: row.currency, salesCity,
                };
                let res;
                try { res = await matrixSearch(args); }
                catch (err) {
                    log(`#${w.id} ${when}: first try failed (${String(err.message).split("\n")[0].slice(0, 120)}) — retrying in ${PAUSE_MS / 1000} s`);
                    await sleep(PAUSE_MS);
                    res = await matrixSearch(args);
                }
                Object.assign(row, res.itinerary, { rules: res.rules ? { categories: res.rules } : null });
                log(`#${w.id} ${when}: €${row.total} ${row.flights || row.carriers} | ${fareLabel(row)} | base €${row.base_fare} YQ ${row.yq} YR ${row.yr} tax €${row.taxes}`
                    + ` | ${res.pick.listed} listed | ${Math.round((Date.now() - t0) / 1000)} s` + (res.rules_error ? ` | rules: ${res.rules_error}` : ""));
            } catch (err) {
                failures++;
                row.error = String(err.message || err).slice(0, 500);
                log(`#${w.id} ${when}: ❌ ${row.error}`);
            }
            if (DRY_RUN) continue;

            if (!row.error) {
                const prev = (await queryRows({
                    schema: "finance", table: "flight_fare_checks",
                    select: "id,checked_at,total,fare_basis,booking_codes,base_fare,yq,yr",
                    where: {
                        watch_id: { op: "eq", value: w.id }, outbound_date: { op: "eq", value: pair.outbound_date },
                        return_date: { op: "eq", value: pair.return_date }, sales_city: { op: "eq", value: salesCity },
                        error: { op: "is_null" },
                    },
                    orderBy: { column: "checked_at", ascending: false }, limit: 1,
                }))[0];
                if (prev && fareKey(prev) !== fareKey(row)) {
                    row.alert_reason = `fare ${fareLabel(prev)} → ${fareLabel(row)}`;
                    try { await pushChange(w, pair, prev, row); row.alerted = true; alerts++; }
                    catch (err) { log(`#${w.id} ${when}: push failed: ${err.message}`); }
                }
            }
            toolJson(await callTool("insert_data", { schema: "finance", table: "flight_fare_checks", data: row }), "insert flight_fare_checks");
            inserted++;
        }
    }
    log(`RESULT ${JSON.stringify({ dry_run: DRY_RUN, searches, inserted, alerts, failures })}`);
}

main().catch(err => { log(`Fatal: ${err.stack || err.message}`); process.exit(1); });
