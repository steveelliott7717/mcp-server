// /opt/supabase-mcp/custom/flights/cron_flight_watch.js
//
// Watches flight prices for the trips in finance.flight_watches and records what
// Google Flights shows (via SerpApi) in finance.flight_price_checks, one row per
// search. Alerts are evaluated and written to alert_reason on every row, but a
// push is only sent when FLIGHT_NOTIFY=true — until then this is intake only.
//
// Budget: each SerpApi account (SERPAPI_KEY, SERPAPI_KEY_2, …; free plan = 250
// searches/month) is asked how many searches it has left (that call is free).
// Each account's daily share is (left − RESERVE) / days to its renewal; the sum is
// the day's budget, divided over the day's scheduled runs (see Schedule), and each
// run splits its part across the active watches by `priority`. Within a watch the
// searches go to the date combinations checked longest ago, so every combination
// gets covered in turn.
//
// Notes earned on the first real calls (2026-10-24):
//  * departure_id/arrival_id accept comma-separated lists, so one search covers
//    BER,FRA,MUC → ICN. One search = one credit regardless.
//  * `price` is the round-trip total when type=1. The itineraries returned are
//    the outbound options (with a departure_token for the return leg); stops and
//    duration below describe the outbound.
//  * The cheapest itinerary is often in other_flights, not best_flights — scan both.
//  * price_insights (Google's "typical range") was absent on a multi-origin search
//    seven months out. Stored when present, otherwise null; alert rules fall back
//    to the watch's own history and target_price.
//  * show_hidden=true ("View more flights") returns ~190 itineraries instead of
//    ~11 for the same single credit (~330 KB, ~20–40 KB once Postgres compresses
//    the jsonb). It is not a strict superset: on 12→26 May it dropped the 30 h
//    Air India fare the default view had. Every itinerary stays queryable through
//    the finance.flight_options view, which unpacks `raw` on read.
//
// Schedule: cron starts this every 15 minutes; it only works when one of the
// FLIGHT_RUN_TIMES (Berlin local, e.g. "07:00,13:00,19:00") has come due and has
// not run yet today, and exits silently otherwise — so the times live in .env,
// not in the crontab. The day's budget is fixed at the first run of the Berlin
// day and divided over the runs still to come; state is kept in .run_state.json.
// A slot missed while the server was down is skipped, its share rolls onward.
//
// Flags: --dry-run (search, print, write nothing)  --limit N (cap searches this run)
//        --force (run now regardless of the schedule; counts against today's budget)

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { callTool } from "./mcp.js";

const STATE_FILE = path.join(path.dirname(fileURLToPath(import.meta.url)), ".run_state.json");
const RUN_TIMES = String(process.env.FLIGHT_RUN_TIMES || "07:00")
    .split(",").map(s => s.trim()).filter(s => /^\d{1,2}:\d{2}$/.test(s))
    .map(s => s.padStart(5, "0")).sort();

// SERPAPI_KEY plus any SERPAPI_KEY_2, SERPAPI_KEY_3, … — one per SerpApi account.
const SERPAPI_KEYS = [...new Set(Object.entries(process.env)
    .filter(([k, v]) => /^SERPAPI_KEY(_\d+)?$/.test(k) && v && v.trim())
    .sort(([a], [b]) => a.localeCompare(b, undefined, { numeric: true }))
    .map(([, v]) => v.trim().replace(/^["']|["']$/g, "")))];
const RESERVE = Number(process.env.FLIGHT_SEARCH_RESERVE ?? 10);       // credits kept back per account each month
const HARD_CAP = Number(process.env.FLIGHT_MAX_SEARCHES_PER_RUN ?? 12); // never more than this per run
const BUDGET_MULT = Number(process.env.FLIGHT_BUDGET_MULTIPLIER ?? 1);  // temporary boost; >1 overspends the monthly pace
const NO_CACHE = String(process.env.FLIGHT_NO_CACHE || "").toLowerCase() === "true"; // SerpApi serves repeats <1 h old from cache otherwise
const NOTIFY = String(process.env.FLIGHT_NOTIFY || "").toLowerCase() === "true";
const NOTIFY_CATEGORY = process.env.FLIGHT_NOTIFY_CATEGORY || "purchases";
const STEP_ALERT = String(process.env.FLIGHT_STEP_ALERT || "").toLowerCase() === "true";
const STEP_EUR = Number(process.env.FLIGHT_STEP_EUR ?? 15);
const STEP_FILE = path.join(path.dirname(fileURLToPath(import.meta.url)), ".step_state.json");
const HISTORY_DAYS = 30;        // window for the "below median" rule
const BELOW_MEDIAN_PCT = 15;    // alert when this far under the 30-day median

const argv = process.argv.slice(2);
const DRY_RUN = argv.includes("--dry-run");
const FORCE = argv.includes("--force");
const LIMIT = argv.includes("--limit") ? Number(argv[argv.indexOf("--limit") + 1]) : null;

const runId = Date.now().toString(36);
const log = (msg) => console.log(`[flight_watch] [${runId}] ${msg}`);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ---------------------------------------------------------------- MCP helpers

/** Unwrap a tool response into parsed JSON and throw on a tool-level error. */
function toolJson(res, what) {
    const text = res?.result?.content?.[0]?.text ?? res?.content?.[0]?.text;
    if (text == null) {
        if (res?.error) throw new Error(`${what}: ${JSON.stringify(res.error).slice(0, 300)}`);
        return res;
    }
    let parsed;
    try { parsed = JSON.parse(text); } catch { throw new Error(`${what}: non-JSON tool response: ${text.slice(0, 200)}`); }
    if (parsed && typeof parsed === "object" && parsed.error) {
        throw new Error(`${what}: ${parsed.message || parsed.error}`);
    }
    return parsed;
}

function rowsOf(parsed) {
    return parsed?.rows ?? parsed?.data ?? (Array.isArray(parsed) ? parsed : []);
}

async function queryRows(args) {
    return rowsOf(toolJson(await callTool("query_table", args), `query ${args.table}`));
}

// ---------------------------------------------------------------- SerpApi

const keyTag = (key) => `…${key.slice(-4)}`;

async function serpAccount(key) {
    const res = await fetch(`https://serpapi.com/account.json?api_key=${encodeURIComponent(key)}`);
    if (!res.ok) throw new Error(`SerpApi account check failed: ${res.status}`);
    const a = await res.json();
    if (a.error) throw new Error(`SerpApi account check: ${a.error}`);
    return a;
}

/** Today's share per account, from what each has left and its own plan month. */
async function loadAccounts(now) {
    const accounts = [];
    for (const key of SERPAPI_KEYS) {
        try {
            const a = await serpAccount(key);
            const left = Number(a.total_searches_left ?? a.plan_searches_left ?? 0);
            const daysLeft = daysLeftInPlan(a, now);
            const share = Math.max(0, Math.floor((left - RESERVE) / daysLeft));
            accounts.push({ key, left, daysLeft, share, used: 0, dead: false });
            log(`SerpApi ${keyTag(key)}: ${a.plan_name || "?"}, ${left} left, ${daysLeft} day(s) to renewal → share today ${share}`);
        } catch (err) {
            log(`SerpApi ${keyTag(key)}: skipped — ${err.message}`);
        }
    }
    return accounts;
}

/** The live account with the most searches left above its reserve. */
function pickAccount(accounts) {
    const live = accounts.filter(a => !a.dead && a.left - a.used > RESERVE);
    if (!live.length) return null;
    return live.reduce((a, b) => (b.left - b.used > a.left - a.used ? b : a));
}

// ---------------------------------------------------------------- schedule

/** Berlin-local date and HH:MM for an instant. */
function berlinClock(d) {
    const parts = Object.fromEntries(new Intl.DateTimeFormat("en-CA", {
        timeZone: "Europe/Berlin", year: "numeric", month: "2-digit", day: "2-digit",
        hour: "2-digit", minute: "2-digit", hourCycle: "h23",
    }).formatToParts(d).map(p => [p.type, p.value]));
    return { date: `${parts.year}-${parts.month}-${parts.day}`, time: `${parts.hour}:${parts.minute}` };
}

function loadState(date) {
    try {
        const s = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
        if (s.date === date) return s;
    } catch { /* first run or unreadable — start fresh */ }
    return { date, daily_budget: null, used: 0, done: [] };
}

function saveState(state) {
    if (DRY_RUN) return;
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

/** stops → SerpApi's encoding: 0 nonstop only, 1 → ≤1 stop, 2 → ≤2 stops, else any. */
function stopsParam(maxStops) {
    if (maxStops === 0) return "1";
    if (maxStops === 1) return "2";
    if (maxStops === 2) return "3";
    return "0";
}
const CLASS_PARAM = { economy: "1", premium_economy: "2", business: "3", first: "4" };

async function serpSearch(watch, outboundDate, returnDate, apiKey) {
    const p = new URLSearchParams({
        engine: "google_flights",
        departure_id: watch.origins.join(","),
        arrival_id: watch.destinations.join(","),
        outbound_date: outboundDate,
        type: returnDate ? "1" : "2",
        stops: stopsParam(watch.max_stops),
        travel_class: CLASS_PARAM[watch.travel_class] || "1",
        adults: String(watch.adults || 1),
        currency: watch.currency || "EUR",
        hl: "en",
        gl: "de",
        show_hidden: "true",
        api_key: apiKey,
    });
    if (returnDate) p.set("return_date", returnDate);
    if (NO_CACHE) p.set("no_cache", "true");
    const res = await fetch(`https://serpapi.com/search.json?${p}`, { signal: AbortSignal.timeout(90000) });
    const body = await res.json().catch(() => ({}));
    if (!res.ok || body.error) throw new Error(`SerpApi ${res.status}: ${body.error || "unknown error"}`);
    return body;
}

/** Reduce a SerpApi response to the cheapest itinerary plus Google's insights. */
function summarize(body) {
    const all = [...(body.best_flights || []), ...(body.other_flights || [])]
        .filter(f => Number.isFinite(Number(f.price)));
    if (!all.length) return null;
    const best = all.reduce((a, b) => (Number(b.price) < Number(a.price) ? b : a));
    const legs = best.flights || [];
    const airlines = [...new Set(legs.map(l => l.airline).filter(Boolean))].join(" + ");
    const pi = body.price_insights || {};
    const range = Array.isArray(pi.typical_price_range) ? pi.typical_price_range : [];
    return {
        price: Number(best.price),
        origin: legs[0]?.departure_airport?.id || null,
        destination: legs[legs.length - 1]?.arrival_airport?.id || null,
        airlines,
        stops: Array.isArray(best.layovers) ? best.layovers.length : Math.max(0, legs.length - 1),
        duration_min: Number.isFinite(Number(best.total_duration)) ? Number(best.total_duration) : null,
        typical_low: Number.isFinite(Number(range[0])) ? Number(range[0]) : null,
        typical_high: Number.isFinite(Number(range[1])) ? Number(range[1]) : null,
        price_level: pi.price_level || null,
        google_flights_url: body.search_metadata?.google_flights_url || null,
        options_seen: all.length,
    };
}

// ---------------------------------------------------------------- dates

const isoDate = (d) => d.toISOString().slice(0, 10);
function* eachDate(from, to) {
    for (let d = new Date(`${from}T00:00:00Z`); isoDate(d) <= to; d.setUTCDate(d.getUTCDate() + 1)) yield isoDate(d);
}
const nightsBetween = (a, b) => Math.round((Date.parse(`${b}T00:00:00Z`) - Date.parse(`${a}T00:00:00Z`)) / 86400000);

/** An explicit date list when the watch has one, else every day of the range. */
function datesFor(list, from, to) {
    if (Array.isArray(list) && list.length) return [...new Set(list.map(d => String(d).slice(0, 10)))].sort();
    return from ? [...eachDate(from, to)] : [];
}

/** Every (outbound, return) pair a watch asks for, excluding dates already past.
 *  outbound_dates / return_dates, when set, replace the from–to ranges. */
function combinations(watch, today) {
    const out = [];
    const returns = datesFor(watch.return_dates, watch.return_from, watch.return_to);
    for (const o of datesFor(watch.outbound_dates, watch.outbound_from, watch.outbound_to)) {
        if (o <= today) continue;
        if (!returns.length) { out.push({ outbound_date: o, return_date: null }); continue; }
        for (const r of returns) {
            const n = nightsBetween(o, r);
            if (n < 1) continue;
            if (watch.min_nights != null && n < watch.min_nights) continue;
            if (watch.max_nights != null && n > watch.max_nights) continue;
            out.push({ outbound_date: o, return_date: r });
        }
    }
    return out;
}

/** Days left in the SerpApi plan month (falls back to the calendar month). */
function daysLeftInPlan(account, now) {
    const renew = account.plan_monthly_renewal || account.next_billing_date || account.plan_renewal_date;
    const end = renew ? new Date(renew) : new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1));
    return Math.max(1, Math.ceil((end - now) / 86400000));
}

// ---------------------------------------------------------------- history & alerts

async function historyFor(watchId) {
    const rows = await queryRows({
        schema: "finance",
        table: "flight_price_checks",
        select: ["outbound_date", "return_date", "checked_at", "price"],
        where: { watch_id: { op: "eq", value: watchId } },
        orderBy: { column: "checked_at", ascending: false },
        limit: 2000,
    });
    const lastChecked = new Map();   // "out|ret" → latest checked_at
    for (const r of rows) {
        const k = `${r.outbound_date}|${r.return_date ?? ""}`;
        if (!lastChecked.has(k)) lastChecked.set(k, r.checked_at);
    }
    return { rows, lastChecked };
}

function median(nums) {
    const s = nums.slice().sort((a, b) => a - b);
    if (!s.length) return null;
    const m = Math.floor(s.length / 2);
    return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

/** Which alert rules a result trips. Returns [] when none. */
function alertReasons(watch, summary, history, now) {
    const reasons = [];
    const priced = history.rows.filter(r => Number.isFinite(Number(r.price)));
    const cutoff = now.getTime() - HISTORY_DAYS * 86400000;
    const recent = priced.filter(r => Date.parse(r.checked_at) >= cutoff).map(r => Number(r.price));
    const med = median(recent);
    const lowEver = priced.length ? Math.min(...priced.map(r => Number(r.price))) : null;

    if (watch.target_price != null && summary.price <= Number(watch.target_price))
        reasons.push(`at or below target ${watch.target_price}`);
    if (summary.typical_low != null && summary.price < summary.typical_low)
        reasons.push(`below Google's typical range ${summary.typical_low}–${summary.typical_high}`);
    if (med != null && recent.length >= 5 && summary.price <= med * (1 - BELOW_MEDIAN_PCT / 100))
        reasons.push(`${Math.round((1 - summary.price / med) * 100)}% below ${HISTORY_DAYS}-day median ${Math.round(med)}`);
    if (lowEver != null && priced.length >= 5 && summary.price < lowEver)
        reasons.push(`new low (previous ${lowEver})`);
    return reasons;
}

async function pushAlert(watch, combo, summary, reasons) {
    const when = combo.return_date ? `${combo.outbound_date} → ${combo.return_date}` : combo.outbound_date;
    const res = await callTool("notify_push", {
        provider: "pushover",
        category: NOTIFY_CATEGORY,
        title: `✈️ ${watch.label}: ${summary.price} ${watch.currency}`,
        body: `${summary.origin}→${summary.destination} ${when} · ${summary.airlines} · ${summary.stops} stop(s)\n${reasons.join("; ")}\n${summary.google_flights_url || ""}`,
    });
    toolJson(res, "notify_push");   // throws if the server rejected it
}

// ---------------------------------------------------------------- step alerts
//
// FLIGHT_STEP_ALERT=true: independent of FLIGHT_NOTIFY, push when the cheapest
// nonstop price of a date pair moves by FLIGHT_STEP_EUR or more from the level
// last reported (the first sighting is the baseline), or when the nonstop stops
// being offered. One push per run, listing every pair that moved; levels live
// in .step_state.json so a price that stays put is reported once.

/** Cheapest nonstop round-trip price in a response, or null if none is offered. */
function cheapestDirect(body) {
    const prices = [...(body.best_flights || []), ...(body.other_flights || [])]
        .filter(f => (f.flights || []).length === 1 && Number.isFinite(Number(f.price)))
        .map(f => Number(f.price));
    return prices.length ? Math.min(...prices) : null;
}

function loadSteps() {
    try { return JSON.parse(fs.readFileSync(STEP_FILE, "utf8")); } catch { return {}; }
}

const stepKey = (w, c) => `${w.id}|${c.outbound_date}|${c.return_date ?? ""}`;

function noteStep(steps, events, w, combo, direct) {
    const key = stepKey(w, combo);
    const s = steps[key];
    if (!s) { steps[key] = { level: direct, last: direct }; return; }
    const moved = s.level == null ? direct != null : direct == null || Math.abs(direct - s.level) >= STEP_EUR;
    if (moved) {
        events.push({ w, combo, key, from: s.level, to: direct });
        if (direct == null || (s.level != null && direct > s.level)) s.rose_at = Date.now();
        s.level = direct;
    }
    s.last = direct;
}

async function pushSteps(events, steps, plans) {
    const day = d => new Date(`${d}T00:00:00Z`).toLocaleDateString("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
    const pair = c => c.return_date ? `${day(c.outbound_date)} → ${day(c.return_date)}` : day(c.outbound_date);
    const eur = v => v == null ? "none" : `€${v}`;
    const lines = events.map(e => `${pair(e.combo)}: direct ${eur(e.from)} → ${eur(e.to)}`
        + (e.from != null && e.to != null ? ` (${e.to > e.from ? "+" : "−"}€${Math.abs(e.to - e.from)})` : ""));
    const rose = events.filter(e => e.to == null || (e.from != null && e.to > e.from));
    // Pairs are spread over several runs when the budget is tight, so "rose
    // together" means every return for that outbound rose within the last hour.
    const roseRecently = (w, c) => Date.now() - (steps[stepKey(w, c)]?.rose_at ?? 0) < 60 * 60 * 1000;
    for (const p of plans) {
        const moved = new Set(events.filter(e => e.w.id === p.watch.id).map(e => e.key));
        const still = p.ranked.filter(c => !moved.has(stepKey(p.watch, c)) && steps[stepKey(p.watch, c)]);
        if (still.length) lines.push(`Unchanged: ${still.map(c => `${pair(c)} ${eur(steps[stepKey(p.watch, c)].last)}`).join(", ")}`);
        const byOut = Object.groupBy(p.ranked, c => c.outbound_date);
        for (const [out, combos] of Object.entries(byOut)) {
            if (combos.length > 1 && combos.some(c => rose.some(e => e.key === stepKey(p.watch, c)))
                && combos.every(c => roseRecently(p.watch, c)))
                lines.push(`All returns with ${day(out)} out rose together — likely the outbound fare.`);
        }
    }
    const first = events[0];
    const title = `✈️ ${first.w.label}: direct ${rose.length ? "up" : "down"} — ${pair(first.combo)} ${eur(first.to)}`
        + (events.length > 1 ? ` (+${events.length - 1} more)` : "");
    toolJson(await callTool("notify_push", {
        provider: "pushover", category: NOTIFY_CATEGORY, title, body: lines.join("\n"),
    }), "notify_push");
    log(`step alert pushed: ${lines.join(" | ")}`);
}

// ---------------------------------------------------------------- main

async function main() {
    const now = new Date();
    const today = isoDate(now);

    // 0. Is a scheduled run due? Silent exit if not, so the 15-minute cron
    //    doesn't fill the log.
    const clock = berlinClock(now);
    const state = loadState(clock.date);
    const passed = RUN_TIMES.filter(t => t <= clock.time);
    const due = passed.filter(t => !state.done.includes(t)).pop() || null;
    if (!due && !FORCE) return;

    log(`start ${now.toISOString()} (Berlin ${clock.date} ${clock.time}, slot ${due ?? "forced"}) dry_run=${DRY_RUN} notify=${NOTIFY}`);
    if (!SERPAPI_KEYS.length) { log("Fatal: no SERPAPI_KEY set"); process.exit(1); }

    // Claim the slot (and any earlier ones missed today) before spending anything,
    // so a crash mid-run cannot make the next tick repeat it.
    if (due) { state.done = [...new Set([...state.done, ...passed])]; saveState(state); }

    // 1. Budget: the day's total is fixed at the first run of the Berlin day from
    //    each account's share; each run takes what's unused divided by the runs
    //    still to come (this one included), then the caps.
    const accounts = await loadAccounts(now);
    const left = accounts.reduce((s, a) => s + a.left, 0);
    if (state.daily_budget == null) {
        state.daily_budget = accounts.reduce((s, a) => s + a.share, 0);
        saveState(state);
    }
    const dayBudget = Math.floor(state.daily_budget * BUDGET_MULT);
    const runsLeft = Math.max(1, RUN_TIMES.filter(t => t > clock.time).length + 1);
    let budget = Math.max(0, Math.floor((dayBudget - state.used) / runsLeft));
    budget = Math.min(budget, HARD_CAP);
    if (LIMIT != null) budget = Math.min(budget, LIMIT);
    log(`SerpApi: ${accounts.length}/${SERPAPI_KEYS.length} account(s) usable, ${left} left in total · today ${state.used}/${dayBudget} used`
        + (BUDGET_MULT !== 1 ? ` (×${BUDGET_MULT} of ${state.daily_budget})` : "") + " · "
        + `${runsLeft} run(s) left today (${RUN_TIMES.join(", ")} Berlin) → this run ${budget} (reserve ${RESERVE}/account, cap ${HARD_CAP})`);

    // 2. Active watches.
    const watches = await queryRows({
        schema: "finance", table: "flight_watches",
        where: { active: { op: "eq", value: true } },
        orderBy: { column: "id", ascending: true },
    });
    log(`${watches.length} active watch(es): ${watches.map(w => `#${w.id} ${w.label}`).join(", ") || "none"}`);
    if (!watches.length || budget === 0) { finish({ budget, searches: 0, inserted: 0, alerts: 0 }); return; }

    // 3. Per watch: combinations ranked by staleness.
    const plans = [];
    for (const w of watches) {
        const combos = combinations(w, today);
        const history = await historyFor(w.id);
        const ranked = combos
            .map(c => ({ ...c, last: history.lastChecked.get(`${c.outbound_date}|${c.return_date ?? ""}`) || null }))
            .sort((a, b) => (a.last ? Date.parse(a.last) : 0) - (b.last ? Date.parse(b.last) : 0));
        const unchecked = ranked.filter(c => !c.last).length;
        log(`#${w.id} ${w.label}: ${combos.length} combination(s), ${unchecked} never checked`);
        plans.push({ watch: w, ranked, history, take: 0 });
    }

    // 4. Split this run's budget by priority (equal when all are 1), capped per
    //    watch, leftover to the stalest.
    const active = plans.filter(p => p.ranked.length);
    const weight = (p) => Math.max(0, Number(p.watch.priority ?? 1));
    const totalWeight = active.reduce((s, p) => s + weight(p), 0) || 1;
    let remaining = budget;
    for (const p of active) {
        p.take = Math.min(Math.floor(budget * weight(p) / totalWeight), p.watch.max_searches_per_run ?? Infinity, p.ranked.length);
        remaining -= p.take;
    }
    while (remaining > 0) {
        const cands = active.filter(p => p.take < Math.min(p.watch.max_searches_per_run ?? Infinity, p.ranked.length));
        if (!cands.length) break;
        const stalest = cands.reduce((a, b) => {
            const la = a.ranked[a.take]?.last, lb = b.ranked[b.take]?.last;
            return (lb ? Date.parse(lb) : 0) < (la ? Date.parse(la) : 0) ? b : a;
        });
        stalest.take++; remaining--;
    }

    // 5. Search, record, evaluate.
    let searches = 0, inserted = 0, alerts = 0, failures = 0;
    const steps = STEP_ALERT ? loadSteps() : {};
    const stepEvents = [];
    for (const p of active) {
        const w = p.watch;
        for (const combo of p.ranked.slice(0, p.take)) {
            const when = combo.return_date ? `${combo.outbound_date}→${combo.return_date}` : combo.outbound_date;
            let body, summary;
            // Try the account with the most share left; if it errors, retire it
            // for this run and try the next one.
            for (let acct = pickAccount(accounts); acct; acct = pickAccount(accounts)) {
                try {
                    body = await serpSearch(w, combo.outbound_date, combo.return_date, acct.key);
                    acct.used++;
                    searches++;
                    state.used++;
                    saveState(state);
                    break;
                } catch (err) {
                    failures++;
                    acct.dead = true;
                    log(`#${w.id} ${when}: ❌ ${keyTag(acct.key)} ${err.message} — retiring this key for the run`);
                }
            }
            if (!body) {
                log(`#${w.id} ${when}: no usable SerpApi account left`);
                continue;
            }
            if (STEP_ALERT && !DRY_RUN) noteStep(steps, stepEvents, w, combo, cheapestDirect(body));
            summary = summarize(body);
            if (!summary) {
                log(`#${w.id} ${when}: no priced itineraries returned`);
                if (!DRY_RUN) await recordCheck(w, combo, null, body, []);
                continue;
            }
            const reasons = alertReasons(w, summary, p.history, now);
            log(`#${w.id} ${when}: ${summary.price} ${w.currency} ${summary.origin}→${summary.destination} ${summary.airlines} ${summary.stops} stop(s) ${summary.duration_min}min`
                + (summary.typical_low != null ? ` typical ${summary.typical_low}–${summary.typical_high} (${summary.price_level})` : "")
                + (reasons.length ? ` ⚑ ${reasons.join("; ")}` : ""));
            if (DRY_RUN) continue;

            let alerted = false;
            if (reasons.length && NOTIFY) {
                try { await pushAlert(w, combo, summary, reasons); alerted = true; alerts++; }
                catch (err) { log(`#${w.id} ${when}: push failed: ${err.message}`); }
            }
            await recordCheck(w, combo, summary, body, reasons, alerted);
            inserted++;
            await sleep(1500);
        }
        if (!DRY_RUN && p.take) {
            toolJson(await callTool("update_data", {
                schema: "finance", table: "flight_watches", pk: ["id"],
                data: [{ id: w.id, last_run_at: now.toISOString() }],
            }), "update flight_watches");
        }
    }

    if (stepEvents.length) {
        try { await pushSteps(stepEvents, steps, active); alerts++; }
        catch (err) {
            log(`step alert push failed: ${err.message}`);
            for (const e of stepEvents) steps[e.key].level = e.from;   // retry on the next run
        }
    }
    if (STEP_ALERT && !DRY_RUN) fs.writeFileSync(STEP_FILE, JSON.stringify(steps, null, 2));

    finish({ slot: due ?? "forced", budget, searches, inserted, alerts, failures, left_after: left - searches,
        today_used: state.used, today_budget: Math.floor(state.daily_budget * BUDGET_MULT),
        per_account: Object.fromEntries(accounts.map(a => [keyTag(a.key), a.used])) });
}

async function recordCheck(w, combo, summary, body, reasons, alerted = false) {
    const row = {
        watch_id: w.id,
        origin: summary?.origin || w.origins[0],
        destination: summary?.destination || w.destinations[0],
        outbound_date: combo.outbound_date,
        return_date: combo.return_date,
        price: summary?.price ?? null,
        currency: w.currency,
        airlines: summary?.airlines ?? null,
        stops: summary?.stops ?? null,
        duration_min: summary?.duration_min ?? null,
        typical_low: summary?.typical_low ?? null,
        typical_high: summary?.typical_high ?? null,
        price_level: summary?.price_level ?? null,
        google_flights_url: summary?.google_flights_url ?? body?.search_metadata?.google_flights_url ?? null,
        alerted,
        alert_reason: reasons.length ? reasons.join("; ") : null,
        raw: body,
    };
    toolJson(await callTool("insert_data", { schema: "finance", table: "flight_price_checks", data: row }), "insert flight_price_checks");
}

function finish(summary) {
    log(`RESULT ${JSON.stringify({ checked_at: new Date().toISOString(), dry_run: DRY_RUN, notify: NOTIFY, ...summary })}`);
}

main().catch(err => {
    log(`❌ ${err.message}`);
    process.exitCode = 1;
});
