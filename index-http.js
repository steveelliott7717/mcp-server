#!/usr/bin/env node
/**
 * MCP Supabase HTTP Server — Extended (PART 1/4)
 * - Safe CRUD + discovery for Supabase
 * - Advanced HTTP fetch (rate limits, retries/backoff, redirect policies, allow/deny, size clamp, streaming, destinations)
 * - Playwright browser flows (multi-step, stealth-ish, context, session, uploads, screenshots, PDFs, extract, jitter)
 * - Notify push (webhook + destinations)
 *
 * Concatenate parts 1–4 into a single file named: /opt/supabase-mcp/runtime/index-http.js
 *   cat index-http-part1.txt index-http-part2.txt index-http-part3.txt index-http-part4.txt > index-http.js
 */
// Use Duffel, not Amadeus or aviaflight
import dotenv from 'dotenv';
dotenv.config({ path: '/opt/supabase-mcp/custom/.env' }); 
import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import rateLimit from 'express-rate-limit';
import crypto from 'node:crypto';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { createClient } from '@supabase/supabase-js';
import { URL } from 'node:url';
import dns from 'node:dns/promises';
import net from 'node:net';
import mime from 'mime-types';
import http from 'http';
import fetch from "node-fetch";
import { Buffer } from "buffer";
import bodyParser from "body-parser";
import pino from 'pino';

const logger = pino({
    level: process.env.LOG_LEVEL || 'info',
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: { level: (label) => ({ level: label }) },
});

const UPLOAD_DIR = '/tmp/mcp-uploads';


// --- Gmail Token Auto-Injector ---
async function maybeInjectGmailToken(args) {
    try {
        const u = new URL(args.url);

        // ✅ Allow token injection for Gmail and Drive
        const allowedHosts = ['gmail.googleapis.com', 'mail.google.com', 'www.googleapis.com'];
        if (!allowedHosts.includes(u.hostname)) return args;

        const tokenPath = '/opt/supabase-mcp/secrets/gmail_token.json';
        if (!fs.existsSync(tokenPath)) {
            console.error('[GMAIL TOKEN INJECT] No token file found at', tokenPath);
            return args;
        }

        const tokenData = JSON.parse(fs.readFileSync(tokenPath, 'utf8'));
        const token = tokenData.token || tokenData.access_token;
        if (!token) {
            console.error('[GMAIL TOKEN INJECT] Token missing in file.');
            return args;
        }

        args.headers = {
            ...(args.headers || {}),
            Authorization: `Bearer ${token}`,
        };

        console.error(`[GMAIL TOKEN INJECT] Added token for ${u.hostname}`);
        return args;
    } catch (err) {
        console.error('[GMAIL TOKEN INJECT] Failed:', err);
        return args;
    }
}

// --- end Gmail Token Auto-Injector ---

// --- Gmail Token Auto-Refresher ---
import { google } from "googleapis";

const GMAIL_TOKEN_PATH = "/opt/supabase-mcp/secrets/gmail_token.json";

/**
 * Refreshes Gmail OAuth token automatically using stored refresh_token.
 * Runs on startup and every 50 minutes thereafter.
 */
async function ensureGmailTokenFresh() {
    try {
        const raw = await fsp.readFile(GMAIL_TOKEN_PATH, "utf8");
        const creds = JSON.parse(raw);

        if (!creds.refresh_token) {
            console.error("⚠️ [GMAIL REFRESH] No refresh_token found in gmail_token.json");
            return;
        }

        const oauth2Client = new google.auth.OAuth2(
            creds.client_id,
            creds.client_secret,
            "http://localhost:8080/"
        );
        oauth2Client.setCredentials({
            refresh_token: creds.refresh_token,
        });

        const newAccessToken = (await oauth2Client.getAccessToken()).token;

        if (newAccessToken && newAccessToken !== creds.token) {
            creds.token = newAccessToken;
            creds.access_token = newAccessToken;
            creds.expiry_date = Date.now() + 55 * 60 * 1000; // ~55 minutes from now

            await fsp.writeFile(GMAIL_TOKEN_PATH, JSON.stringify(creds, null, 2));
            console.log("🔄 [GMAIL REFRESH] Token automatically refreshed");
        } else {
            console.log("✅ [GMAIL REFRESH] Token still valid, no refresh needed");
        }
    } catch (err) {
        console.error("❌ [GMAIL REFRESH] Failed to refresh token:", err.message);
    }
}

// Run once on startup
ensureGmailTokenFresh();
// Schedule auto-refresh every 50 minutes
setInterval(ensureGmailTokenFresh, 50 * 60 * 1000);
// --- end Gmail Token Auto-Refresher ---

// ───────────────────────────────────────────────────────────────
// --- GitHub Token Auto-Injector ---
async function maybeInjectGithubToken(args) {
    try {
        const u = new URL(args.url);
        const allowedHosts = ['api.github.com', 'uploads.github.com'];
        if (!allowedHosts.includes(u.hostname)) return args;

        const tokenPath = '/opt/supabase-mcp/secrets/github_token.json';
        if (!fs.existsSync(tokenPath)) {
            console.error('[GITHUB TOKEN INJECT] No token file found at', tokenPath);
            return args;
        }

        const tokenData = JSON.parse(fs.readFileSync(tokenPath, 'utf8'));
        const token = tokenData.token || tokenData.access_token || tokenData.github_token;
        if (!token) {
            console.error('[GITHUB TOKEN INJECT] Token missing in file.');
            return args;
        }

        args.headers = {
            ...(args.headers || {}),
            Authorization: `Bearer ${token}`,
            Accept: 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28',
        };

        console.error(`[GITHUB TOKEN INJECT] Added token for ${u.hostname}`);
        return args;
    } catch (err) {
        console.error('[GITHUB TOKEN INJECT] Failed:', err);
        return args;
    }
}
// --- end GitHub Token Auto-Injector ---

// --- Aviasales Token Auto-Injector ---
async function maybeInjectAviasalesToken(args) {
    try {
        const u = new URL(args.url);
        const allowedHosts = ['api.travelpayouts.com'];
        if (!allowedHosts.includes(u.hostname)) return args;

        const tokenPath = '/opt/supabase-mcp/secrets/aviasales_token.json';
        if (!fs.existsSync(tokenPath)) {
            console.error('[AVIASALES TOKEN INJECT] No token file found at', tokenPath);
            return args;
        }

        const tokenData = JSON.parse(fs.readFileSync(tokenPath, 'utf8'));
        const token = tokenData.token || tokenData.access_token;
        if (!token) {
            console.error('[AVIASALES TOKEN INJECT] Token missing in file.');
            return args;
        }

        args.headers = {
            ...(args.headers || {}),
            'X-Access-Token': token,
        };

        console.error(`[AVIASALES TOKEN INJECT] Added token for ${u.hostname}`);
        return args;
    } catch (err) {
        console.error('[AVIASALES TOKEN INJECT] Failed:', err);
        return args;
    }
}
// --- end Aviasales Token Auto-Injector ---


// --- GitHub Token Refresher ---
const GITHUB_TOKEN_PATH = "/opt/supabase-mcp/secrets/github_token.json";
const GITHUB_APP_ID = process.env.GITHUB_APP_ID;
const GITHUB_INSTALLATION_ID = process.env.GITHUB_INSTALLATION_ID;
const GITHUB_PRIVATE_KEY_PATH = "/opt/supabase-mcp/secrets/github_app_private.pem";

// For static tokens (PATs)
async function ensureGithubTokenPresent() {
    try {
        const raw = await fsp.readFile(GITHUB_TOKEN_PATH, "utf8");
        const creds = JSON.parse(raw);
        const token = creds.token || creds.access_token || creds.github_token;
        if (!token) {
            console.error("⚠️ [GITHUB TOKEN] Missing token in github_token.json");
            return;
        }
        console.log("✅ [GITHUB TOKEN] Token found and ready for use");
    } catch (err) {
        console.error("❌ [GITHUB TOKEN] Could not read github_token.json:", err.message);
    }
}

// For GitHub App tokens (auto-refresh)
async function ensureGithubTokenFresh() {
    try {
        if (!GITHUB_APP_ID || !GITHUB_INSTALLATION_ID || !fs.existsSync(GITHUB_PRIVATE_KEY_PATH)) {
            await ensureGithubTokenPresent(); // fallback to PAT check
            return;
        }

        const privateKey = await fsp.readFile(GITHUB_PRIVATE_KEY_PATH, "utf8");
        const now = Math.floor(Date.now() / 1000);
        const payload = { iat: now - 60, exp: now + 9 * 60, iss: GITHUB_APP_ID };
        const jwtToken = jwt.sign(payload, privateKey, { algorithm: "RS256" });

        const res = await fetch(
            `https://api.github.com/app/installations/${GITHUB_INSTALLATION_ID}/access_tokens`,
            {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${jwtToken}`,
                    Accept: "application/vnd.github+json",
                },
            }
        );

        if (!res.ok) throw new Error(`GitHub API ${res.status}`);
        const data = await res.json();
        const tokenData = { token: data.token, expires_at: data.expires_at };
        await fsp.writeFile(GITHUB_TOKEN_PATH, JSON.stringify(tokenData, null, 2));

        console.log("🔄 [GITHUB REFRESH] Token refreshed successfully");
    } catch (err) {
        console.error("❌ [GITHUB REFRESH] Failed to refresh token:", err.message);
    }
}

// Run once on startup & schedule
ensureGithubTokenFresh();
setInterval(ensureGithubTokenFresh, 50 * 60 * 1000);
// --- end GitHub Token Refresher ---

// --- Amadeus Token Auto-Refresher (Production Environment Only) ---
const AMADEUS_TOKEN_PATH = "/opt/supabase-mcp/secrets/amadeus_token.json";

async function ensureAmadeusTokenFresh() {
    try {
        const raw = await fsp.readFile(AMADEUS_TOKEN_PATH, "utf8");
        const creds = JSON.parse(raw);

        // 🔹 Always use the production Amadeus API
        const AMADEUS_AUTH_URL = "https://api.amadeus.com/v1/security/oauth2/token";
        const host = "api.amadeus.com";

        const expired =
            !creds.access_token || Date.now() > (creds.expiry_date || 0) - 60_000;

        if (!expired) {
            console.log("✅ [AMADEUS REFRESH:prod] Token still valid, no refresh needed");
            FETCH_HOST_TOKENS[host] = `Bearer ${creds.access_token}`;
            return;
        }

        console.log(`🔄 [AMADEUS REFRESH:prod] Requesting new token from ${AMADEUS_AUTH_URL}`);

        const params = new URLSearchParams({
            grant_type: "client_credentials",
            client_id: creds.client_id,
            client_secret: creds.client_secret,
        });

        const res = await fetch(AMADEUS_AUTH_URL, {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: params.toString(),
        });

        if (!res.ok) {
            const text = await res.text();
            throw new Error(`Amadeus token fetch failed: ${res.status} ${text}`);
        }

        const data = await res.json();

        creds.access_token = data.access_token;
        creds.expiry_date = Date.now() + (data.expires_in - 60) * 1000; // refresh 1 min early
        creds.token_type = "Bearer";
        creds.source = "production";

        await fsp.writeFile(AMADEUS_TOKEN_PATH, JSON.stringify(creds, null, 2));

        // 🧩 Immediately register token for http_fetch
        FETCH_HOST_TOKENS[host] = `Bearer ${data.access_token}`;

        console.log("✅ [AMADEUS REFRESH:prod] Token refreshed successfully");
    } catch (err) {
        console.error("❌ [AMADEUS REFRESH:prod] Failed:", err.message);
    }
}

// Run on startup and every 25 minutes
ensureAmadeusTokenFresh();
setInterval(ensureAmadeusTokenFresh, 25 * 60 * 1000);
// --- end Amadeus Token Auto-Refresher (Production Environment Only) ---




// ====== Playwright Browser Path Override (for MCP isolation fix) ======
process.env.PLAYWRIGHT_BROWSERS_PATH = "/opt/supabase-mcp/node_modules/playwright/.local-browsers";
process.env.ICU_DATA = "/opt/supabase-mcp/node_modules/playwright/.local-browsers/chromium_headless_shell-1194/chrome-linux/icudtl.dat";
process.env.CHROME_HEADLESS_DISABLE_CRASHPAD = "true";
process.env.HOME = "/home/mcp";
process.env.DEBUG = (process.env.DEBUG || '') + ',pw:api,pw:browser*';
console.error('[Playwright ENV]', {
    HOME: process.env.HOME,
    PLAYWRIGHT_BROWSERS_PATH: process.env.PLAYWRIGHT_BROWSERS_PATH
});


/* ========================= Core Config ========================= */
const SUPABASE_URL      = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
//const PORT              = Number(process.env.PORT || 3000);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const TRUST_TOKEN       = process.env.MCP_TRUST_TOKEN || '';
const CURSOR_SECRET     = process.env.CURSOR_SIGNING_SECRET || '';
const SSE_PING_MS       = Number(process.env.MCP_SSE_PING_MS || 15000);

const LOG_EVENTS        = (process.env.MCP_LOG_EVENTS || '0') === '1';
const LOG_TABLE_FQTN    = process.env.MCP_LOG_TABLE || ''; // e.g. system.event_log

/* ========================= HTTP Fetch knobs ========================= */
const FETCH_MAX_BYTES_DEFAULT   = Number(process.env.HTTP_FETCH_MAX_BYTES || 2*1024*1024); // 2MB
const FETCH_RETRY_STATUS        = (process.env.HTTP_FETCH_RETRY_STATUS || '429,500,502,503,504')
                                  .split(',').map(s=>parseInt(s.trim(),10)).filter(Boolean);
const FETCH_RETRY_MAX           = Number(process.env.HTTP_FETCH_RETRY_MAX || 3);
const FETCH_BACKOFF_BASE_MS     = Number(process.env.HTTP_FETCH_BACKOFF_BASE_MS || 300);
const FETCH_BACKOFF_JITTER_MS   = Number(process.env.HTTP_FETCH_BACKOFF_JITTER_MS || 250);
const FETCH_ALLOW_HOSTS         = (process.env.HTTP_FETCH_ALLOW_HOSTS || '').split(',').map(s=>s.trim()).filter(Boolean);
const FETCH_DENY_HOSTS          = (process.env.HTTP_FETCH_DENY_HOSTS  || '').split(',').map(s=>s.trim()).filter(Boolean);
const FETCH_PER_HOST_CAPACITY   = Number(process.env.HTTP_FETCH_BUCKET_CAPACITY || 10);
const FETCH_PER_HOST_REFILL     = Number(process.env.HTTP_FETCH_BUCKET_REFILL_PER_SEC || 5);
const FETCH_PER_HOST_MAX_WAIT   = Number(process.env.HTTP_FETCH_BUCKET_MAX_WAIT_MS || 2000);
/** redirect policy: any | same_host | same_site | allow_hosts_only */
const FETCH_REDIRECT_POLICY     = (process.env.HTTP_FETCH_REDIRECT_POLICY || 'any');
const FETCH_ALLOW_REDIRECT_HOSTS= (process.env.HTTP_FETCH_ALLOW_REDIRECT_HOSTS || '').split(',').map(s=>s.trim()).filter(Boolean);
/** auth/cache header helpers */
const FETCH_HOST_TOKENS         = safelyParseJSON(process.env.HTTP_FETCH_HOST_TOKENS || '{}'); // {"example.com":"Bearer XXX"}
const FETCH_CACHE_ENABLED       = (process.env.HTTP_FETCH_CACHE || '1') === '1';

/* ========================= Query knobs ========================= */
const QUERY_RANGE_MAX_DAYS      = Number(process.env.QUERY_RANGE_MAX_DAYS || 7);
const QUERY_RANGE_DELAY_MS      = Number(process.env.QUERY_RANGE_DELAY_MS || 100);
/* ========================= Browser knobs ========================= */
const BROWSER_DENY_LOCALHOST    = (process.env.BROWSER_DENY_LOCALHOST || '1') === '1';

/* ========================= Destinations (GitHub/Supabase) ========================= */
const SUPABASE_SERVICE_ROLE     = process.env.SUPABASE_SERVICE_ROLE || '';
const SUPABASE_STORAGE_BUCKET   = process.env.SUPABASE_STORAGE_BUCKET || '';
const GITHUB_TOKEN              = process.env.GITHUB_TOKEN || '';
const GITHUB_REPO               = process.env.GITHUB_REPO || ''; // owner/name
const GITHUB_BRANCH             = process.env.GITHUB_BRANCH || 'main';

/* ========================= Write Safety & Allowlists ========================= */
const REQUIRE_WHERE_UPD_DEL     =
  (process.env.DBWRITE_REQUIRE_WHERE_FOR_UPDATE_DELETE || '0') === '1';

function envCsv(name, fallback=''){ const v=process.env[name]||fallback||''; return new Set(v.split(',').map(s=>s.trim()).filter(Boolean)); }
const READ_TABLE_ALLOW  = envCsv('DBREAD_TABLE_ALLOWLIST');
const WRITE_TABLE_ALLOW = envCsv('DBWRITE_TABLE_ALLOWLIST');
function readColsAllowFor(fqtn){ return envCsv(`DBREAD_COL_ALLOWLIST_${fqtn}`); }
function writeColsAllowFor(fqtn){ const s1=envCsv(`DB_WRITE_COL_ALLOWLIST_${fqtn}`); const s2=envCsv(`DBWRITE_COL_ALLOWLIST_${fqtn}`); return new Set([...s1,...s2]); }

/* ========================= Health Steps (env) ========================= */
const HEALTH_TABLE_NAME = process.env.HEALTH_TABLE_NAME || 'health_metrics';
const HEALTHKIT_INGEST_TOKEN = process.env.HEALTHKIT_INGEST_TOKEN || '';

/* ========================= Boot checks ========================= */
if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_ANON_KEY'); process.exit(1);
}

/* ========================= Supabase / Express ========================= */
const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

// ESM-safe __dirname for sendFile, etc.
const __dirname = path.dirname(new URL(import.meta.url).pathname);

// ── JSON/body size & parsers (single block; remove duplicates below) ──────────
const MAX_JSON_BYTES = process.env.MCP_JSON_LIMIT || '5mb';

const app = express();
app.use(helmet());
app.use(bodyParser.json({ limit: "100mb" }));
app.use(bodyParser.urlencoded({ limit: "100mb", extended: true }));
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    logger.info({ msg: 'MCP server started', port: PORT });
});



// ✅ JSON + CORS middleware (single setup)
app.use(express.json({ limit: MAX_JSON_BYTES, type: ['application/json', 'text/plain'] }));
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-MCP-Trust', 'Origin', 'User-Agent'],
    maxAge: 86400,
}));
app.options('*', (_req, res) => res.sendStatus(204));
app.set('trust proxy', true);


// Request logging middleware
app.use((req, res, next) => {
    const start = Date.now();
    res.on('finish', () => {
        logger.info({
            method: req.method,
            path: req.path,
            ip: req.ip,
            status: res.statusCode,
            duration_ms: Date.now() - start,
        });
    });
    next();
});

// Trust key middleware (same as before)
app.use((req, res, next) => {
    const trust = req.get('X-MCP-Trust');
    const local = req.ip === '127.0.0.1' || req.ip === '::1';

    // ✅ Allow ChatGPT MCP connections (bypass for /sse and /tools used by MCP runtime)
    if (req.path.startsWith('/sse') || req.path.startsWith('/tools')) {
        console.log('[AUTH BYPASS] Allowing MCP system route:', req.path);
        return next();
    }

    // ✅ Normal local or trusted header access
    if (
        trust === process.env.MCP_TRUST_TOKEN ||
        local ||
        req.path === '/health' ||
        req.path === '/gpt/health'
    ) {
        return next();
    }

    // ❌ Everything else is blocked
    console.error('[AUTH BLOCKED]', req.method, req.path, { trust });
    res.status(403).json({ error: 'Forbidden: missing or invalid X-MCP-Trust header' });
});



app.get(['/health', '/sse/health'], (req, res) => {
    res.json({ ok: true, server: 'mcp-server', ts: Date.now() });
});

app.get(['/tools', '/sse/tools'], (req, res) => {
    res.json({ jsonrpc: '2.0', id: 0, result: toolsPayload() });
});

/* ========================= OAuth 2.0 for Claude MCP Connector ========================= */

// OAuth Discovery Endpoint
app.get('/.well-known/oauth-authorization-server', (_req, res) => {
    const baseUrl = process.env.MCP_PUBLIC_URL || 'https://mcp.mcp-server.fyi';
    res.json({
        issuer: baseUrl,
        token_endpoint: `${baseUrl}/oauth/token`,
        token_endpoint_auth_methods_supported: ['client_secret_post', 'client_secret_basic'],
        grant_types_supported: ['client_credentials'],
        response_types_supported: ['token'],
        scopes_supported: ['mcp:tools', 'mcp:read', 'mcp:write'],
        service_documentation: `${baseUrl}/docs`
    });
});

// Also support the generic OpenID discovery path
app.get('/.well-known/openid-configuration', (_req, res) => {
    const baseUrl = process.env.MCP_PUBLIC_URL || 'https://mcp.mcp-server.fyi';
    res.json({
        issuer: baseUrl,
        token_endpoint: `${baseUrl}/oauth/token`,
        token_endpoint_auth_methods_supported: ['client_secret_post', 'client_secret_basic'],
        grant_types_supported: ['client_credentials'],
        response_types_supported: ['token'],
        scopes_supported: ['mcp:tools', 'mcp:read', 'mcp:write']
    });
});

// OAuth Token Endpoint (client_credentials grant)
app.post('/oauth/token', express.urlencoded({ extended: true }), (req, res) => {
    console.log('[OAuth /token] Request received', {
        grant_type: req.body?.grant_type,
        hasClientId: !!req.body?.client_id,
        hasClientSecret: !!req.body?.client_secret,
        authHeader: req.get('authorization') ? 'present' : 'missing'
    });

    // Extract credentials from body OR Basic auth header
    let clientId = req.body?.client_id;
    let clientSecret = req.body?.client_secret;

    // Support HTTP Basic Auth: Authorization: Basic base64(client_id:client_secret)
    const authHeader = req.get('authorization') || '';
    if (authHeader.toLowerCase().startsWith('basic ')) {
        try {
            const b64 = authHeader.slice(6);
            const decoded = Buffer.from(b64, 'base64').toString('utf8');
            const [id, secret] = decoded.split(':');
            if (id) clientId = id;
            if (secret) clientSecret = secret;
        } catch (e) {
            console.error('[OAuth /token] Failed to decode Basic auth:', e.message);
        }
    }

    const grantType = req.body?.grant_type;

    // Validate grant type
    if (grantType !== 'client_credentials') {
        return res.status(400).json({
            error: 'unsupported_grant_type',
            error_description: 'Only client_credentials grant is supported'
        });
    }

    // Validate credentials against MCP_TRUST_TOKEN or MCP_URL_TOKEN
    // Claude will send client_id + client_secret; we check if client_secret matches our token
    const validToken = process.env.MCP_TRUST_TOKEN || process.env.MCP_URL_TOKEN;

    // Accept if client_secret matches our trust token
    // OR if client_id matches (for flexibility)
    const isValid = (
        (clientSecret && clientSecret === validToken) ||
        (clientId && clientId === validToken)
    );

    if (!isValid) {
        console.error('[OAuth /token] Invalid credentials', {
            clientIdProvided: !!clientId,
            clientSecretProvided: !!clientSecret,
            clientSecretMatch: clientSecret === validToken,
            clientIdMatch: clientId === validToken
        });
        return res.status(401).json({
            error: 'invalid_client',
            error_description: 'Invalid client credentials'
        });
    }

    // Issue a bearer token (we'll just return the same token for simplicity)
    // In production, you might want to issue a JWT with expiration
    const accessToken = validToken;
    const expiresIn = 86400; // 24 hours

    console.log('[OAuth /token] ✅ Token issued successfully');

    res.json({
        access_token: accessToken,
        token_type: 'Bearer',
        expires_in: expiresIn,
        scope: 'mcp:tools mcp:read mcp:write'
    });
});

/* ========================= End OAuth 2.0 ========================= */

/* ========================= Utils ========================= */
function safelyParseJSON(s){ try { return JSON.parse(s);} catch { return {}; } }
function splitFqtn(fqtn){ const i=fqtn.indexOf('.'); return i>0?{schema:fqtn.slice(0,i),table:fqtn.slice(i+1)}:{schema:'public',table:fqtn}; }
function encodeToken(v){ if(v===null) return 'null'; const s=String(v); return s.replace(/\(/g,'%28').replace(/\)/g,'%29').replace(/,/g,'%2C').replace(/ /g,'%20'); }
function mapSimple(op){ return ({'=':'eq','eq':'eq','ne':'neq','neq':'neq','!=':'neq','gt':'gt','gte':'gte','lt':'lt','lte':'lte','like':'like','ilike':'ilike','is':'is'})[op]||op; }
function nowIso(){ return new Date().toISOString(); }
function chunkBy(arr,n){ const out=[]; for(let i=0;i<arr.length;i+=n) out.push(arr.slice(i,i+n)); return out; }

function parseTable(input, schema) {
  if (!input) throw new Error('table is required');
  if (input.includes('.')) {
    const [s, t] = input.split('.');
    return { schema: schema ?? s, table: t };
  }
  return { schema: schema ?? 'public', table: input };
}

/** Convert tool text to JSON so UI won't render prose mid-chain */
function sanitizeToolContent(chainId, content) {
  console.error("[speak-gate] sanitizeToolContent invoked chainId=", chainId, "items=", (content || []).length);

  const out = [];
  for (const item of (content || [])) {
    if (item?.type === "text") {
      const txt = String(item.text || "");
      const allowed = nlAllowed(chainId, txt);

      console.error("[speak-gate] item", {
        chainId,
        txt: txt.slice(0, 120), // log only the first 120 chars
        allowed,
        isFinal: isFinalVerified(chainId)
      });

      if (allowed) {
        // Only the FINAL summary should survive as text
        out.push({ type: "text", text: txt });
      } else {
        // Mid-chain or disallowed prose: force JSON instead
        try {
          const parsed = JSON.parse(txt);
          out.push({ type: "json", json: parsed });
        } catch {
          out.push({
            type: "json",
            json: { dropped: true, reason: "speak-gate", payload: txt }
          });
        }
      }
    } else {
      // Non-text content (json, bytes, etc) just pass through
      out.push(item);
    }
  }

  console.error("[speak-gate] sanitizeToolContent OUT", JSON.stringify(out, null, 2));
  return out;
}



/**
 * Global callTool that works inside and outside MCP runtime.
 * Reuses existing TRUST_TOKEN and server URL logic.
 */
globalThis.callTool = async function callTool(name, args = {}) {
    // ✅ Use in-process tool dispatcher if available (runtime plugin)
    if (globalThis.mcp_mcp_server_fyi__jit_plugin?.callTool) {
        return await globalThis.mcp_mcp_server_fyi__jit_plugin.callTool({
            name,
            arguments: args,
        });
    }

    // ✅ Otherwise, fallback to HTTP call to local MCP server
    const MCP_SERVER_URL =
        process.env.MCP_URL ||
        process.env.MCP_SERVER_URL ||
        `${process.env.MCP_PUBLIC_URL}/sse`;

    const payload = { name, arguments: args };

    const res = await fetch(MCP_SERVER_URL, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            // 🟢 Reuse the same token your middleware already uses
            "X-MCP-Trust": TRUST_TOKEN,
        },
        body: JSON.stringify(payload),
    });

    if (!res.ok) {
        const text = await res.text();
        throw new Error(`MCP tool ${name} failed: ${res.status} ${text}`);
    }

    const json = await res.json();
    return json;
};

// --------------------------- Chain helpers (single-file, dependency-free) ---------------------------

// Per-run variables (scoped by chainId)
const CHAIN_VARS = new Map();   // chainId -> { k:v }

// Basic filters (add more if you like)
function b64tohex(b64) {
    // Strip optional data URL prefix
    const pure = String(b64 || '').replace(/^data:.*?;base64,/, '');
    const buf = Buffer.from(pure, 'base64');
    return buf.toString('hex');
}

// Render ${var} and ${var|filter} inside strings; deep-render arrays/objects
function deepRender(value, scope) {
    if (value == null) return value;
    if (Array.isArray(value)) return value.map(v => deepRender(v, scope));
    if (typeof value === 'object') {
        const out = {};
        for (const [k, v] of Object.entries(value)) out[k] = deepRender(v, scope);
        return out;
    }
    if (typeof value !== 'string') return value;

    // If the entire string is a single ${...} expression, return the raw resolved value
    // preserving its original type (number, boolean, object, etc.)
    const singleMatch = value.match(/^\$\{([^}]+)\}$/);
    if (singleMatch) {
        const [rawPath, rawFilter] = singleMatch[1].split('|').map(s => s.trim());
        const parts = rawPath.replace(/\[(\d+)\]/g, '.$1').split('.');
        let val = scope;
        for (const part of parts) {
            if (val == null) break;
            val = val[part];
        }
        val = val ?? '';
        if (!rawFilter) return val; // ← returns number, boolean, object as-is
        if (rawFilter === 'b64tohex') return b64tohex(val);
        return String(val);
    }

    // For interpolation embedded within a larger string, stringify as before
    return value.replace(/\$\{([^}]+)\}/g, (_, expr) => {
        const [rawPath, rawFilter] = expr.split('|').map(s => s.trim());
        const parts = rawPath.replace(/\[(\d+)\]/g, '.$1').split('.');
        let val = scope;
        for (const part of parts) {
            if (val == null) break;
            val = val[part];
        }
        val = val ?? '';
        if (!rawFilter) return typeof val === 'object' ? JSON.stringify(val) : String(val);
        if (rawFilter === 'b64tohex') return b64tohex(val);
        return String(val);
    });
}

function saveVar(chainId, name, value) {
    const bag = CHAIN_VARS.get(chainId) || {};
    bag[name] = value;
    CHAIN_VARS.set(chainId, bag);
    return bag;
}
function getVars(chainId) {
    return CHAIN_VARS.get(chainId) || {};
}

// Minimal content extractors
function extractFirstJson(content) {
    const item = Array.isArray(content) ? content.find(c => c?.type === 'json') : null;
    if (item?.json != null) return item.json;

    // Also try parsing text blocks
    const textItem = Array.isArray(content) ? content.find(c => c?.type === 'text') : null;
    if (textItem?.text) {
        try { return JSON.parse(textItem.text); } catch { }
    }
    return null;
}

// If browser_flow returns inline base64 (when you ask for {return:"data", encoding:"base64"})
function extractScreenshotBase64(flowContent) {
    const j = extractFirstJson(flowContent);
    if (!j || !Array.isArray(j.results)) return null;

    // Your browser_flow returns: { ok:true, results:[ {op:'screenshot', path:..., destination:..., data:"base64..."}, ...] }
    const shot = j.results.find(r => r?.op === 'screenshot');
    return shot?.data || null;
}


function resolveChromiumPath() {
    const base = '/home/mcp/.cache/ms-playwright';
    try {
        const dirs = fs.readdirSync(base).filter(d => d.startsWith('chromium-'));
        if (!dirs.length) return null;
        // pick the newest
        const pick = dirs.sort().slice(-1)[0];
        const p = `${base}/${pick}/chrome-linux/chrome`;
        return fs.existsSync(p) ? p : null;
    } catch {
        return null;
    }
}

/** Helper for tools to return machine data, not prose */
function asJsonContent(obj) {
    return [{ type: 'text', text: JSON.stringify(obj, null, 2) }];
}

// Single tool dispatch, reusing your existing handlers
async function callOneToolByName(name, args) {
    // keep this mapping in sync with your /sse dispatcher
    if (name === 'query_table') return tool_query_table(args);
    else if (name === 'insert_data') return tool_insert_data(args);
    else if (name === 'upsert_data') return tool_upsert_data(args);
    else if (name === 'update_data') return tool_update_data(args);
    else if (name === 'delete_data') return tool_delete_data(args);
    else if (name === 'list_schemas') return tool_list_schemas(args);
    else if (name === 'list_tables') return tool_list_tables(args);
    else if (name === 'list_columns') return tool_list_columns(args);
    else if (name === 'list_rpcs') return tool_list_rpcs(args);
    else if (name === 'get_function_definition') return tool_get_function_definition(args);
    else if (name === 'list_functions') return tool_list_functions(args);
    else if (name === 'list_triggers') return tool_list_triggers(args);
    else if (name === 'list_event_triggers') return tool_list_event_triggers(args);
    else if (name === 'list_views') return tool_list_views(args);
    else if (name === 'list_matviews') return tool_list_matviews(args);
    else if (name === 'get_view_definition') return tool_get_view_definition(args);
    else if (name === 'get_trigger_definition') return tool_get_trigger_definition(args);
    else if (name === 'manage_cron_job') return tool_manage_cron_job(args);
    else if (name === 'tool_encode_attachment') return tool_encode_attachment(args);
    else if (name === 'tool_upload_file') return tool_upload_file(args);
    else if (name === 'tool_get_file') return tool_get_file(args);
    else if (name === 'tool_delete_file') return tool_delete_file(args);
    else if (name === 'tool_edit_slice') return tool_edit_slice(args);
    else if (name === 'tool_run_check') return tool_run_check(args);
    else if (name === 'tool_commit_file') return tool_commit_file(args);
    else if (name === "send_email") return tool_send_email(args);
    else if (name === 'http_fetch') return tool_http_fetch(args);
    else if (name === 'notify_push') return tool_notify_push(args);
    else if (name === 'browser_flow') return tool_browser_flow(args);
    else if (name === 'finalize_verification' && typeof tool_finalize_verification === 'function')
        return tool_finalize_verification(args);
    else if (name === 'enforce_mapping') return tool_enforce_mapping(args);
    else if (name === 'query_health_metrics_range') return tool_query_health_metrics_range(args);
    else if (name === 'chain') {
        const chainId = crypto.randomUUID?.() || Math.random().toString(36).slice(2);
        return asJsonContent(await runChain(chainId, args));
    }
    else if (name === 'rpc_expose_constraints_filtered') {
        const { target_schema, target_table } = args;
        const { data, error } = await supabase.rpc('rpc_expose_constraints_filtered', {
            target_schema,
            target_table
        });
        if (error) throw error;
        content = asJsonContent(data || []);
    }
    else if (name === 'rpc_expose_indexes_filtered') {
        const { target_schema, target_table } = args;
        const { data, error } = await supabase.rpc('rpc_expose_indexes_filtered', {
            target_schema,
            target_table
        });
        if (error) throw error;
        content = asJsonContent(data || []);
    }

    throw new Error(`Unknown tool '${name}' in callOneToolByName`);

}

// Ensure directory exists on startup
if (!fs.existsSync('/tmp/mcp-uploads')) fs.mkdirSync('/tmp/mcp-uploads', { recursive: true });

async function tool_upload_file({ filename, content, upload_to_drive = false }) {
    if (!filename || !content) throw new Error('Missing filename or content');

    const safeName = path.basename(filename);
    const destDir = '/tmp/mcp-uploads';
    if (!fs.existsSync(destDir)) fs.mkdirSync(destDir, { recursive: true });

    const dest = path.join(destDir, safeName);

    // 🔹 Write the content exactly as given — no conversion
    const buffer = Buffer.isBuffer(content)
        ? content
        : typeof content === 'string'
            ? Buffer.from(content)
            : Buffer.from(String(content));

    fs.writeFileSync(dest, buffer);

    let driveResult = null;

    if (upload_to_drive) {
        try {
            const tokenPath = '/opt/supabase-mcp/secrets/gmail_token.json';
            if (!fs.existsSync(tokenPath)) throw new Error(`Missing token file at ${tokenPath}`);

            const tokenData = JSON.parse(fs.readFileSync(tokenPath, 'utf8'));
            const token = tokenData.access_token || tokenData.token;
            if (!token) throw new Error('OAuth token missing or invalid');

            // 🧾 Build a minimal multipart body (no MIME metadata)
            const boundary = 'rawBoundary' + Date.now();
            const head = `--${boundary}\r\n\r\n${safeName}\r\n--${boundary}\r\n\r\n`;
            const tail = `\r\n--${boundary}--`;
            const headBuffer = Buffer.from(head);
            const tailBuffer = Buffer.from(tail);
            const body = Buffer.concat([headBuffer, buffer, tailBuffer]);

            const res = await fetch(
                'https://www.googleapis.com/upload/drive/v3/files?uploadType=media',
                {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${token}`,
                        'Content-Type': 'application/octet-stream',
                        'Content-Length': body.length.toString()
                    },
                    body
                }
            );

            if (!res.ok) {
                const errText = await res.text();
                throw new Error(`Drive upload failed (${res.status}): ${errText}`);
            }

            driveResult = await res.json();
        } catch (err) {
            console.error('[DRIVE UPLOAD ERROR]', err);
            driveResult = { error: err.message };
        }
    }

    return asJsonContent({
        ok: true,
        filename: safeName,
        stored_at: dest,
        size_bytes: buffer.length,
        uploaded_to_drive: !!upload_to_drive,
        drive: driveResult
    });
}




async function tool_get_file({ filename }) {
    const safeName = path.basename(filename);
    const filePath = path.join(UPLOAD_DIR, safeName);
    if (!fs.existsSync(filePath)) throw new Error(`File not found: ${safeName}`);
    const data = fs.readFileSync(filePath);
    return asJsonContent({ filename: safeName, content_base64: data.toString('base64') });
}



// ==========================================================
// File Navigation Utility
// ==========================================================
// Find a string anchor in a file and return surrounding context
async function tool_find_anchor({ relpath, anchor, context_lines = 20 }) {
    const SAFE_BASE = "/opt/supabase-mcp/runtime";
    const filePath = path.join(SAFE_BASE, relpath);

    if (!filePath.startsWith(SAFE_BASE))
        throw new Error(`Path not allowed outside ${SAFE_BASE}`);

    const text = await fs.readFile(filePath, "utf8");
    const lines = text.split(/\r?\n/);
    const idx = lines.findIndex((l) => l.includes(anchor));

    if (idx === -1) {
        throw new Error(`Anchor not found: ${anchor}`);
    }

    const start = Math.max(0, idx - context_lines);
    const end = Math.min(lines.length, idx + context_lines);
    const snippet = lines.slice(start, end).join("\n");

    return [
        {
            type: "json",
            json: { relpath, anchor, start, end, snippet },
        },
    ];
}

// 🧩 Retrieve the full definition of a function by matching braces
async function tool_get_anchor_function_definition({ relpath, anchor }) {
    const SAFE_BASE = "/opt/supabase-mcp/runtime";
    const filePath = path.join(SAFE_BASE, relpath);

    if (!filePath.startsWith(SAFE_BASE)) {
        throw new Error(`Path not allowed outside ${SAFE_BASE}`);
    }

    const text = await fs.readFile(filePath, "utf8");
    const startIdx = text.indexOf(anchor);
    if (startIdx === -1) {
        throw new Error(`Anchor not found: ${anchor}`);
    }

    // find the first opening brace after the anchor
    let braceStart = text.indexOf("{", startIdx);
    if (braceStart === -1) throw new Error("No opening brace found after anchor");

    // walk through the text to find the matching closing brace
    let depth = 1;
    let i = braceStart + 1;
    while (i < text.length && depth > 0) {
        if (text[i] === "{") depth++;
        else if (text[i] === "}") depth--;
        i++;
    }

    const braceEnd = i;
    const snippet = text.slice(startIdx, braceEnd);

    const startLine = text.slice(0, startIdx).split(/\r?\n/).length;
    const endLine = text.slice(0, braceEnd).split(/\r?\n/).length;

    return [
        {
            type: "json",
            json: { relpath, anchor, startLine, endLine, snippet },
        },
    ];
}

// ==========================================================
// File Editing and Validation Utilities
// ==========================================================

//////////////////////////////
// tool_edit_slice
//////////////////////////////
// Atomically replace a section of a file by line range, with backup
async function tool_edit_slice({ relpath, start_line, end_line, new_lines }) {
    const SAFE_BASE = "/opt/supabase-mcp/runtime";
    const filePath = path.join(SAFE_BASE, relpath);
    if (!filePath.startsWith(SAFE_BASE)) {
        throw new Error(`Path not allowed outside ${SAFE_BASE}`);
    }

    const text = await fs.readFile(filePath, "utf8");
    const lines = text.split(/\r?\n/);

    // Safety: ensure valid bounds
    const total = lines.length;
    const s = Math.max(1, Math.min(start_line, total));
    const e = Math.max(s, Math.min(end_line, total));

    const before = lines.slice(0, s - 1);
    const after = lines.slice(e);
    const merged = [...before, ...new_lines, ...after];

    // Backup before overwrite
    const backupPath = `${filePath}.bak-${Date.now()}`;
    await fs.writeFile(backupPath, text, "utf8");

    // Atomic write using temp file + rename
    const tmpPath = `${filePath}.tmp-${Date.now()}`;
    await fs.writeFile(tmpPath, merged.join("\n"), { mode: 0o600 });
    await fs.rename(tmpPath, filePath);

    return [
        {
            type: "json",
            json: { ok: true, relpath, backup: backupPath, newLineCount: new_lines.length },
        },
    ];
}

//////////////////////////////
// tool_run_check
//////////////////////////////
// Validate JavaScript syntax using Node.js
async function tool_run_check({ relpath, command = "node --check" }) {
    const { exec } = await import("child_process");
    const target = relpath ? path.join("/opt/supabase-mcp/runtime", relpath) : null;
    const cmd = target ? `${command} ${target}` : command;

    return await new Promise((resolve) => {
        exec(cmd, (err, stdout, stderr) => {
            resolve([
                {
                    type: "json",
                    json: {
                        ok: !err,
                        code: err ? err.code : 0,
                        stdout,
                        stderr,
                    },
                },
            ]);
        });
    });
}

//////////////////////////////
// tool_revert_file
//////////////////////////////
// Restore the most recent .bak backup for a file
async function tool_revert_file({ relpath, backup }) {
    const SAFE_BASE = "/opt/supabase-mcp/runtime";
    const filePath = path.join(SAFE_BASE, relpath);
    if (!filePath.startsWith(SAFE_BASE)) {
        throw new Error(`Path not allowed outside ${SAFE_BASE}`);
    }

    const dir = path.dirname(filePath);
    const base = path.basename(filePath);
    let backupPath;

    if (backup) {
        backupPath = path.join(dir, backup);
    } else {
        // Find latest backup automatically
        const candidates = fs.readdirSync(dir)
            .filter(f => f.startsWith(`${base}.bak-`))
            .sort()
            .reverse();
        if (!candidates.length) throw new Error(`No backup found for ${relpath}`);
        backupPath = path.join(dir, candidates[0]);
    }

    const content = await fs.readFile(backupPath, "utf8");
    await fs.writeFile(filePath, content, "utf8");

    return [{ type: "json", json: { ok: true, restored: backupPath } }];
}

//////////////////////////////
// tool_dry_run_edit
//////////////////////////////
// Simulate a file edit and return a unified diff (no write)
async function tool_dry_run_edit({ relpath, start_line, end_line, new_lines }) {
    const SAFE_BASE = "/opt/supabase-mcp/runtime";
    const filePath = path.join(SAFE_BASE, relpath);
    if (!filePath.startsWith(SAFE_BASE)) throw new Error(`Path not allowed outside ${SAFE_BASE}`);

    const text = await fs.readFile(filePath, "utf8");
    const lines = text.split(/\r?\n/);

    const before = lines.slice(0, start_line - 1);
    const after = lines.slice(end_line);
    const merged = [...before, ...new_lines, ...after];
    const newText = merged.join("\n");

    // Generate unified diff
    const oldText = text.split(/\r?\n/).join("\n");
    const diff = generateUnifiedDiff(oldText, newText, relpath);

    return [{ type: "json", json: { ok: true, relpath, diff } }];
}

// Helper: unified diff generator
function generateUnifiedDiff(oldText, newText, fileName = "file") {
    const { diffLines } = require("diff");
    const parts = diffLines(oldText, newText);
    let diff = `--- ${fileName}\n+++ ${fileName}\n`;

    let oldLine = 1, newLine = 1;
    for (const part of parts) {
        const lines = part.value.split("\n").slice(0, -1);
        if (part.added) {
            diff += `@@ -${oldLine},0 +${newLine},${lines.length} @@\n`;
            for (const l of lines) diff += `+${l}\n`;
            newLine += lines.length;
        } else if (part.removed) {
            diff += `@@ -${oldLine},${lines.length} +${newLine},0 @@\n`;
            for (const l of lines) diff += `-${l}\n`;
            oldLine += lines.length;
        } else {
            oldLine += lines.length;
            newLine += lines.length;
        }
    }
    return diff;
}

//////////////////////////////
// tool_commit_file
//////////////////////////////
// Commit and push file changes to Git (GitHub-safe)
async function tool_commit_file({ relpath, message = "Automated MCP edit" }) {
    const { execFile } = await import("child_process");
    const cwd = "/opt/supabase-mcp/runtime";
    const safeRel = relpath.replace(/[^a-zA-Z0-9._/-]/g, "");

    const run = (cmd, args) => new Promise((resolve, reject) => {
        execFile(cmd, args, { cwd }, (err, stdout, stderr) => {
            if (err) reject(new Error(stderr.trim() || err.message));
            else resolve(stdout.trim());
        });
    });

    return await new Promise(async (resolve) => {
        try {
            await run("git", ["add", safeRel]);
            await run("git", ["commit", "-m", message]);
            await run("git", ["push", "origin", "HEAD"]);
            resolve([{ type: "json", json: { ok: true } }]);
        } catch (e) {
            resolve([{ type: "json", json: { ok: false, error: e.message } }]);
        }
    });
}


async function tool_delete_file({ filename }) {
    const safeName = path.basename(filename);
    const filePath = path.join(UPLOAD_DIR, safeName);
    if (!fs.existsSync(filePath)) throw new Error(`File not found: ${safeName}`);
    fs.unlinkSync(filePath);
    return asJsonContent({ deleted: true, filename: safeName });
}

// New tool: manage_cron_job
async function tool_manage_cron_job({ mode = 'schedule', args = {} }) {
    if (!['schedule', 'unschedule'].includes(mode))
        throw new Error('mode must be "schedule" or "unschedule"');

    const fn =
        mode === 'schedule'
            ? 'f_schedule_from_cron_jobs'
            : 'f_unschedule_from_cron_jobs';

    const { data, error } = await supabase.rpc(fn, args);
    if (error) throw new Error(JSON.stringify(error));

    return {
        type: 'json',
        content: [
            {
                ok: true,
                action: mode,
                function: fn,
                input: args,
                result: data,
            },
        ],
    };
}


// The CHAIN runner
async function runChain(chainId, chainArgs) {
    // chainArgs: { steps: [ { name, arguments, saveAs? }, ... ], vars? }
    const { steps = [], vars = {} } = chainArgs || {};
    saveVar(chainId, '__init__', true); // ensure bag exists
    Object.assign(getVars(chainId), vars); // seed variables

    const outputs = [];

    for (const step of steps) {
        if (!step || !step.name) throw new Error('chain step missing "name"');

        // Render the step arguments using accumulated variables
        const scope = getVars(chainId);
        const resolvedArgs = deepRender(step.arguments || {}, scope);

        // Call the underlying tool
        const content = await callOneToolByName(step.name, resolvedArgs);
        const jsonOut = extractFirstJson(content) ?? { content }; // fallback

        // Special case: collect base64 from browser_flow screenshot for later use
        if (step.name === 'browser_flow') {
            const b64 = extractScreenshotBase64(content);
            if (b64) saveVar(chainId, 'screenshot_base64', b64);
        }

        // Save variable if requested (e.g., "saveAs": "shot")
        if (step.saveAs) {
            saveVar(chainId, step.saveAs, jsonOut);
        }

        // Push a summarized output for the /sse result
        outputs.push({ step: step.name, args: resolvedArgs, result: jsonOut });
    }
    return outputs;
}



/* ========================= Multi-row helpers ========================= */
const DBWRITE_MAX_BATCH = Number(process.env.DBWRITE_MAX_BATCH || 200);

/** Accepts object | array | JSON string | NDJSON and returns an array of rows */
function coerceRows(input) {
  if (Array.isArray(input)) return input;
  if (input && typeof input === 'object') return [input];
  if (typeof input === 'string') {
    const s = input.trim();
    // Try JSON array
    if (s.startsWith('[')) {
      try { const a = JSON.parse(s); if (Array.isArray(a)) return a; } catch {}
    }
    // Try single JSON object
    if (s.startsWith('{')) {
      try { return [JSON.parse(s)]; } catch {}
    }
    // NDJSON (one JSON object per line)
    const lines = s.split(/\r?\n/).map(x => x.trim()).filter(Boolean);
    const out = [];
    for (const ln of lines) {
      try { out.push(JSON.parse(ln)); } catch { throw new Error('ApiSyntaxError: Extra data (invalid NDJSON line)'); }
    }
    if (out.length) return out;
  }
  throw new Error('invalid data: expected object, array, JSON array, or NDJSON string');
}

/** Remove PK fields from a row to form an update patch */
function omitKeys(obj, keys) {
  const kset = new Set(keys || []);
  const out = {};
  for (const [k,v] of Object.entries(obj || {})) if (!kset.has(k)) out[k] = v;
  return out;
}


/* ========================= Speak-Gate (runtime) ========================= */
/** We gate NL output per "chain" (conversation/run). Chain is resolved from:
 *  - req header: x-mcp-chain
 *  - else body.params.chain
 *  - else body.id
 *  - else req.ip (last resort)
 */
const FINAL_VERIFIED_BY_CHAIN = new Map(); // chainId -> boolean

function resolveChainId(req, jr, params) {
  return (
    req.get('x-mcp-chain') ||
    params?.chain ||
    (jr?.id ? String(jr.id) : '') ||
    req.ip ||
    'default'
  );
}

function setFinalVerified(chainId, v=true) {
  FINAL_VERIFIED_BY_CHAIN.set(chainId, !!v);
}
function isFinalVerified(chainId) {
  return FINAL_VERIFIED_BY_CHAIN.get(chainId) === true;
}

/** Allow-list one final message, block everything else mid-chain */
const NL_ALLOW = /^✅ (Success|Done)\. (\d+ rows updated|No changes needed)\. Verified across target scope\.$/;

const NL_BLOCKERS = [
  // status/ack/promise phrasing
  /(^|\s)(Understood|Proceeding|Continuing|I’ll now|I will|I’ll continue|Moving on|Running|Got it|All set|Processing(?: remaining)?|Next up|Starting now|Starting with IDs?|I won’t pause|without stopping|continuous run|non-stop|do not stop)/i,
  // ID ranges & progress narration
  /IDs?\s*\d+(–|-|—| to )\d+/,
  // per-row checkmarks
  /^✅\s*(ID|Row)\s*\d+/m,
  // batch chatter
  /(^|\s)(next batch|batch \d+ of \d+)/i,
  // progress emojis at start of line
  /^[⏳⏱️⌛]/m,
];

function nlAllowed(chainId, text) {
  // Mid-chain: drop everything
  if (!isFinalVerified(chainId)) return false;
  // Final: must match allow-list AND not match any blocker
  if (!NL_ALLOW.test(text || '')) return false;
  for (const rx of NL_BLOCKERS) if (rx.test(text)) return false;
  return true;
}


/* ========================= OpenAPI export ========================= */
app.get('/openapi.json', (_req, res) =>
    res.sendFile(path.join(__dirname, 'openapi.json'))
);

app.get('/CRUD.json', (_req, res) =>
    res.sendFile(path.join(process.cwd(), 'CRUD.json'))
);



/* ========================= /sse auth + rate limit ========================= */
function isTrustedClient(req) {
  const bearer = (req.get('authorization') || '').replace(/^Bearer\s+/i, '').trim();
    const got = req.get('X-MCP-Trust') || req.query.trust || '';
  const origin = req.get('origin') || '';

  return (
    (process.env.MCP_URL_TOKEN && bearer === process.env.MCP_URL_TOKEN) ||
    (TRUST_TOKEN && got === TRUST_TOKEN) ||
    origin.includes("chat.openai.com")
  );
}

// ── Auth gate for /sse ─────────────────────────────────────────────
const normalize = (v) => String(v ?? '')
    .replace(/\r/g, '')
    .trim()
    .replace(/^"(.*)"$/, '$1');

app.use('/sse', (req, res, next) => {
    const expectedBearer = normalize(process.env.MCP_URL_TOKEN);
    const expectedTrust = normalize(process.env.MCP_TRUST_TOKEN);

    // allow preflight / read / handshake
    if (req.method === 'OPTIONS' || req.method === 'GET') return next();
    if (req.method === 'POST') {
        const b = req.body || {};
        if (b.method === 'initialize') return next();
    }

    // collect creds from all supported places
    const bearerRaw = (req.get('authorization') || '').replace(/^Bearer\s+/i, '');
    const urlTok = req.query.token || req.get('x-mcp-token') || '';
    const trustHdr = req.get('X-MCP-Trust') || '';
    const trustQ = req.query.trust || '';

    const bearer = normalize(bearerRaw);
    const token = normalize(urlTok);
    const trust = normalize(trustHdr || trustQ);

    const ok =
        (expectedTrust && trust && trust === expectedTrust) ||
        (expectedBearer && ((bearer && bearer === expectedBearer) || (token && token === expectedBearer)));

    if (!ok) {
        const exp = expectedTrust || expectedBearer || '';
        const got = trust || bearer || token || '';
        console.error("[AUTH] Unauthorized: expected", JSON.stringify(exp), "got", JSON.stringify(got));
        return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
});



const manHits = new Map();
const MAN_WINDOW_MS = 10_000, MAN_LIMIT = 5;
function manualLimiter(req, res, next) {
  const now = Date.now();
  const ip = (req.ip ?? req.socket?.remoteAddress ?? 'unknown');
  const arr = manHits.get(ip) ?? [];
  const recent = arr.filter(t => now - t < MAN_WINDOW_MS);
  recent.push(now);
  manHits.set(ip, recent);
  if (recent.length > MAN_LIMIT) return res.status(429).json({ error: 'Too many requests (manual limiter)' });
  next();
}
const sseLimiter = rateLimit({
  windowMs: 10_000,
  max: 120,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => (req.ip ?? req.socket?.remoteAddress ?? ''),
  message: { error: 'Too many requests, slow down.' }
});
app.use('/sse', (req, res, next) => {
    if (isTrustedClient(req)) return next();
    manualLimiter(req, res, (err) => {
        if (err) return next(err);
        sseLimiter(req, res, next);
    });

});

/* ===================== JSON-RPC helpers =================== */
function rpcOK(id, result) {
  return { jsonrpc: "2.0", id, result };
}
function rpcErr(id, code, message, data) {
  const error = { code, message };
  if (data !== undefined) error.data = data;
  return { jsonrpc: "2.0", id, error };
}

app.use((req, res, next) => {
    if (req.path === '/sse' && req.method === 'POST') {
        console.error('[BODY DEBUG]', 'ct=', req.get('content-type') || '',
            'hasBody=', req.body !== undefined,
            'type=', typeof req.body,
            'sample=', typeof req.body === 'string' ? req.body.slice(0, 100) : JSON.stringify(req.body || {}).slice(0, 100));
    }
    next();
});


// ── Optional REST→RPC shim for /sse (set MCP_REST_SHIM=0 to disable) ───────────
if ((process.env.MCP_REST_SHIM || '1') === '1') {
    app.use('/sse', (req, res, next) => {
        try {
            const b = req.body;
            // If client posts { name, arguments } (no jsonrpc/method), wrap it
            if (b && typeof b === 'object' && !b.method && (b.name || b.arguments)) {
                req.body = {
                    jsonrpc: '2.0',
                    id: b.id ?? null,
                    method: 'tools/call',
                    params: { name: b.name, arguments: b.arguments || {} }
                };
            }
        } catch { }
        next();
    });
}

app.use('/sse', (req, res, next) => {
    const tool = req.body?.params?.name || null;
    res.on('finish', () => {
        logger.info({ path: '/sse', method: req.body?.method, tool, status: res.statusCode });
    });
    next();
});

// ✅ Compatibility alias so MCP clients can POST to /sse/call or /sse/tools/call
app.post(['/sse/call', '/sse/tools/call'], (req, res, next) => {
    req.url = '/sse';
    next();
});
// allow POST / as tool call for legacy connectors
app.post('/', (req, res, next) => {
    req.url = '/sse';
    next();
});

/* ========================= SSE Stream for MCP Streamable HTTP ========================= */
app.get('/sse', (req, res) => {
    console.log('[SSE] GET connection opened');

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.flushHeaders();

    res.write(': connected\n\n');

    const pingInterval = setInterval(() => {
        res.write(': ping\n\n');
    }, 15000);

    req.on('close', () => {
        console.log('[SSE] GET connection closed');
        clearInterval(pingInterval);
    });
});
/* ========================= End SSE Stream ========================= */

// ── JSON-RPC handler for /sse ──────────────────────────────────────────────────
// JSON-RPC endpoint
app.post('/sse', async (req, res) => {
    try {
        const jr = req.body || {};

        // --- Guard: require proper JSON-RPC shape ---
        if (!jr || typeof jr !== 'object' || !jr.method) {
            const errorId = (jr?.id !== undefined && jr?.id !== null) ? jr.id : 0;
            return res.status(400).json({
                jsonrpc: '2.0',
                id: errorId,
                error: { code: -32600, message: 'Invalid JSON-RPC: missing method' }
            });
        }

        // Resolve a per-run chainId (for interpolation/vars)
        const paramsForChainId = jr.params || {};
        const chainId = (jr.id && String(jr.id)) || crypto.randomUUID?.() || Math.random().toString(36).slice(2);


        // --- Handshake ---
        if (jr.method === 'initialize') {
            return res.json({
                jsonrpc: '2.0',
                id: jr.id ?? 0,
                result: {
                    protocolVersion: '2025-06-18',
                    capabilities: {
                        tools: {}
                    },
                    serverInfo: { name: 'mcp-supabase-http', version: '1.0.0' }
                }
            });
        }

        // --- Notifications (fire-and-forget) ---
        if (jr.method && jr.method.startsWith('notifications/')) {
            return res.status(204).end();
        }

        // --- Tool listing ---
        if (jr.method === 'tools/list') {
            return res.json({
                jsonrpc: '2.0',
                id: jr.id ?? 0,
                result: toolsPayload()
            });
        }

        // --- Tool call ---
        if (jr.method === 'tools/call') {
            const params = jr.params || {};

            // Support either { name, arguments } OR { tool:{name,arguments} }
            let name, args;
            if (params.name) {
                name = params.name;
                args = params.arguments || {};
            } else if (params.tool && params.tool.name) {
                name = params.tool.name;
                args = params.tool.arguments || {};
            } else {
                return res.json({
                    jsonrpc: '2.0',
                    id: jr.id,
                    error: { code: -32602, message: 'Invalid tool call: no name/arguments' }
                });
            }

            // --- new: CHAIN dispatcher ---
            if (name === 'chain') {
                const chainResults = await runChain(chainId, args);
                return res.json({
                    jsonrpc: '2.0',
                    id: jr.id,
                    result: {
                        content: asJsonContent(chainResults)
                    }
                });
            }

            // --- normal tool dispatch (unchanged) ---
            let content;
            if (name === 'query_table') content = await tool_query_table(args);
            else if (name === 'insert_data') content = await tool_insert_data(args);
            else if (name === 'upsert_data') content = await tool_upsert_data(args);
            else if (name === 'update_data') content = await tool_update_data(args);
            else if (name === 'delete_data') content = await tool_delete_data(args);
            else if (name === 'list_schemas') content = await tool_list_schemas(args);
            else if (name === 'list_tables') content = await tool_list_tables(args);
            else if (name === 'list_columns') content = await tool_list_columns(args);
            else if (name === 'list_rpcs') content = await tool_list_rpcs(args);
            else if (name === 'get_function_definition') content = await tool_get_function_definition(args);
            else if (name === 'list_functions') content = await tool_list_functions(args);
            else if (name === 'list_triggers') content = await tool_list_triggers(args);
            else if (name === 'list_event_triggers') content = await tool_list_event_triggers(args);
            else if (name === 'list_views') content = await tool_list_views(args);
            else if (name === 'list_matviews') content = await tool_list_matviews(args);
            else if (name === 'get_view_definition') content = await tool_get_view_definition(args);
            else if (name === 'get_trigger_definition') content = await tool_get_trigger_definition(args);
            else if (name === "tool_edit_slice") content = await tool_edit_slice(args);
            else if (name === "tool_run_check") content = await tool_run_check(args);
            else if (name === "tool_commit_file") content = await tool_commit_file(args);
            else if (name === 'manage_cron_job') content = await tool_manage_cron_job(args);
            else if (name === "send_email") content = await tool_send_email(args);
            else if (name === 'http_fetch') content = await tool_http_fetch(args);
            else if (name === 'notify_push') content = await tool_notify_push(args);
            else if (name === 'browser_flow') content = await tool_browser_flow(args);
            else if (name === 'finalize_verification' && typeof tool_finalize_verification === 'function')
                content = await tool_finalize_verification(args);
            else if (name === 'enforce_mapping') content = await tool_enforce_mapping(args);
            else if (name === 'query_health_metrics_range') content = await tool_query_health_metrics_range(args);
            else if (name === 'rpc_expose_constraints_filtered') {
                const { target_schema, target_table } = args;
                const { data, error } = await supabase.rpc('rpc_expose_constraints_filtered', {
                    target_schema,
                    target_table
                });
                if (error) throw error;
                content = asJsonContent(data || []);
            }
            else if (name === 'rpc_expose_indexes_filtered') {
                const { target_schema, target_table } = args;
                const { data, error } = await supabase.rpc('rpc_expose_indexes_filtered', {
                    target_schema,
                    target_table
                });
                if (error) throw error;
                content = asJsonContent(data || []);
            }
            else {
                // Unknown tool - still return proper error even if id is null
                return res.json({
                    jsonrpc: '2.0',
                    id: jr.id ?? 0,
                    error: { code: -32601, message: `Unknown tool '${name}'` }
                });
            }

            return res.json({ jsonrpc: '2.0', id: jr.id, result: { content } });
        }

        // Unknown method
        if (jr.id === undefined || jr.id === null) return res.status(204).end();
        return res.json({
            jsonrpc: '2.0',
            id: jr.id,
            error: { code: -32601, message: `Unknown method '${jr.method}'` }
        });

    } catch (e) {
        const id = (req.body && req.body.id != null) ? req.body.id : 0;
        return res.json({
            jsonrpc: '2.0',
            id,
            error: {
                code: e?.code || -32000,
                message: e?.message || String(e),
                data: e?.details || e?.stack || null
            }
        });
    }
});




async function verifyRowsChanged(schema, table, pkCol, ids, opName) {
  if (!ids?.length) {
    throw new Error(`${opName} failed: no rows were affected`);
  }
  const resp = await execOrThrow(
    supabase.schema(schema).from(table).select(pkCol).in(pkCol, ids)
  );
  if (!resp?.data?.length) {
    throw new Error(`${opName} verification failed: rows not found after write`);
  }
  return resp.data;
}

/* ========================= Allowlist enforcement ========================= */
function enforceReadTableAllow(fqtn){ if (READ_TABLE_ALLOW.size && !READ_TABLE_ALLOW.has(fqtn)) throw new Error(`table '${fqtn}' not allowed (DBREAD_TABLE_ALLOWLIST)`); }
function enforceReadColAllow(fqtn, selectCsv){
  const allow=readColsAllowFor(fqtn); if(!allow.size||!selectCsv||selectCsv==='*')return;
  for(const c of selectCsv.split(',').map(s=>s.trim()).filter(Boolean)){ const root=c.split('.',1)[0];
    if(!allow.has(root)) throw new Error(`column '${root}' not allowed for '${fqtn}' (DBREAD_COL_ALLOWLIST_${fqtn})`);
  }
}
function enforceWriteTableAllow(fqtn){
  if (WRITE_TABLE_ALLOW.size && !WRITE_TABLE_ALLOW.has(fqtn) && !WRITE_TABLE_ALLOW.has(fqtn.replace(/^public\./,'')))
    throw new Error(`table '${fqtn}' not allowed (DBWRITE_TABLE_ALLOWLIST)`);
}
function enforceWriteColsAllow(fqtn, rowsOrPatch){
  const allow=writeColsAllowFor(fqtn); if(!allow.size) return;
  const chk=(obj)=>{ const illegal=Object.keys(obj||{}).filter(k=>!allow.has(k));
    if(illegal.length) throw new Error(`columns ${JSON.stringify(illegal)} not allowed for '${fqtn}'`); };
  if(Array.isArray(rowsOrPatch)) rowsOrPatch.forEach(chk); else if(rowsOrPatch && typeof rowsOrPatch==='object') chk(rowsOrPatch);
}

/* ========================= Error mapping & exec ========================= */
function mapApiError(e) {
    const msg = e?.message || e?.details || String(e); const code = e?.code || (e?.hint && /([0-9A-Z]{5})/.exec(e.hint)?.[1]);
    if (code === '23505') { const err = new Error(`unique_violation: ${msg}`); err.code = 'unique_violation'; return err; }
    if (code === '23503') { const err = new Error(`foreign_key_violation: ${msg}`); err.code = 'foreign_key_violation'; return err; }
    if (code === '23502') { const err = new Error(`not_null_violation: ${msg}`); err.code = 'not_null_violation'; return err; }
    if (code === '22P02') { const err = new Error(`invalid_input: ${msg}`); err.code = 'invalid_input'; return err; }
    if (code === '42703') { const err = new Error(`undefined_column: ${msg}`); err.code = 'undefined_column'; return err; }
    if (code === '21000') { const err = new Error(`unsafe_write: ${msg}`); err.code = 'unsafe_write'; return err; }
    if (code === '23514') { const err = new Error(`check_violation: ${msg}`); err.code = 'check_violation'; return err; }
    return new Error(msg);
}

/* ========================= Structured Error helper ========================= */
function structuredError(code, details) {
  const err = new Error(code || 'structured_error');
  err.name = 'StructuredError';
  err.code = code || 'structured_error';
  err.details = details || {};
  return err;
}


async function execOrThrow(p){ try{ return await p; } catch(e){ throw mapApiError(e); } }

/* ========================= Relative time ========================= */
function parseRelativeTime(v){
  if(v==='now') return new Date().toISOString();
  if(typeof v!=='string'||!/^-\d+(m|h|d|w|M|y)$/.test(v)) return v;
  const n=parseInt(v.slice(1,-1),10), u=v.at(-1), d=new Date();
  if(u==='m')d.setMinutes(d.getMinutes()-n); if(u==='h')d.setHours(d.getHours()-n); if(u==='d')d.setDate(d.getDate()-n);
  if(u==='w')d.setDate(d.getDate()-7*n); if(u==='M')d.setMonth(d.getMonth()-n); if(u==='y')d.setFullYear(d.getFullYear()-n);
  return d.toISOString();
}

/* ========================= Filters (rich) ========================= */
function serializeLeaf(k, v){
  if(v===null || typeof v!=='object') return `${k}.eq.${encodeToken(v)}`;
  const iop=(v.op || Object.keys(v).find(x=>x!=='value'&&x!=='cast') || 'eq').toLowerCase();
  let val=v.value ?? v[iop]; if(typeof val==='string') val=parseRelativeTime(val);

  // array containment operators
  if(iop === 'contains_all') return `${k}.cs.{${(val||[]).map(encodeToken).join(',')}}`;
  if(iop === 'contains_any') return `${k}.ov.{${(val||[]).map(encodeToken).join(',')}}`;
  if(iop === 'contained_by') return `${k}.cd.{${(val||[]).map(encodeToken).join(',')}}`;

  // regex operators
  if(iop === 'match')  return `${k}.regex.${encodeToken(val)}`;
  if(iop === 'imatch') return `${k}.iregex.${encodeToken(val)}`;

  // *_any pattern operators (OR chain)
  if(/_any$/.test(iop)){
    const base=iop.replace(/_any$/,'');
    const patt=(val||[]).map(x=>{
      if(base==='starts_with'||base==='istarts_with') return `${k}.${base==='istarts_with'?'ilike':'like'}.${encodeToken(String(x)+'%')}`;
      if(base==='ends_with'||base==='iends_with')     return `${k}.${base==='iends_with'  ?'ilike':'like'}.${encodeToken('%'+String(x))}`;
      if(base==='contains'||base==='icontains')       return `${k}.${base==='icontains'   ?'ilike':'like'}.${encodeToken('%'+String(x)+'%')}`;
      if(base==='like'||base==='ilike')               return `${k}.${base}.${encodeToken(String(x))}`;
      return `${k}.eq.${encodeToken(x)}`;
    }).filter(Boolean);
    return patt.length?`or(${patt.join(',')})`:'';
  }

  if(iop==='in' && Array.isArray(val))     return `${k}.in.(${val.map(encodeToken).join(',')})`;
  if(iop==='not_in' && Array.isArray(val)) return `${k}.not.in.(${val.map(encodeToken).join(',')})`;
  if(iop==='is_null'||iop==='isnull')      return `${k}.is.null`;
  if(iop==='not_null'||iop==='notnull')    return `${k}.not.is.null`;
  return `${k}.${mapSimple(iop)}.${encodeToken(val)}`;
}
function serializeBoolExpr(node){
  if(!node||typeof node!=='object') return '';
  const op=(node.op||'').toLowerCase(); const conds=Array.isArray(node.conditions)?node.conditions:[];
  const parts=conds.map(c=>{
    if(c && typeof c==='object' && (c.op||c.conditions)) return serializeBoolExpr(c);
    const [k,v]=Object.entries(c||{})[0]||[]; if(!k) return ''; return serializeLeaf(k,v);
  }).filter(Boolean);
  if(op==='and') return `and(${parts.join(',')})`;
  if(op==='or')  return `or(${parts.join(',')})`;
  if(op==='not') return `not.and(${parts.join(',')})`;
  return parts.join(',');
}
function applyWhere(q, where) {
    const VALID_OPS = new Set([
        'eq', 'neq', 'ne', '!=', '=', 'gt', 'gte', 'lt', 'lte',
        'like', 'ilike', 'in', 'not_in', 'is', 'is_null', 'isnull', 'not_null', 'notnull',
        'between', 'contains', 'icontains', 'starts_with', 'istarts_with',
        'ends_with', 'iends_with', 'contains_all', 'contains_any', 'contained_by',
        'match', 'imatch', 'like_any', 'ilike_any', 'starts_with_any', 'istarts_with_any',
        'ends_with_any', 'iends_with_any', 'contains_any', 'icontains_any',
    ]);

    if (where && typeof where === 'object' && (where.op || where.conditions)) { const token = serializeBoolExpr(where); return token ? q.or(token) : q; }
    for (const [col, spec] of Object.entries(where || {})) {
        if (spec === null || typeof spec !== 'object' || Array.isArray(spec)) { q = q.eq(col, spec); continue; }
        const iop = (spec.op || Object.keys(spec).find(k => k !== 'cast' && k !== 'value') || 'eq').toLowerCase();
        if (!VALID_OPS.has(iop) && !/_any$/.test(iop)) {
            throw new Error(`invalid_operator: unknown operator '${iop}' on column '${col}'. Valid operators: ${[...VALID_OPS].join(', ')}`);
        }
        let val = spec.value ?? spec[iop]; if (typeof val === 'string') val = parseRelativeTime(val);
        // array containment ops
        if (iop === 'contains_all') { q = q.contains(col, val); continue; }
        if (iop === 'contains_any') { q = q.overlaps(col, val); continue; }
        if (iop === 'contained_by') { q = q.containedBy(col, val); continue; }
        // regex
        if (iop === 'match') { q = q.filter(col, 'regex', String(val)); continue; }
        if (iop === 'imatch') { q = q.filter(col, 'iregex', String(val)); continue; }
        // *_any -> OR token
        if (/_any$/.test(iop)) { const token = serializeLeaf(col, { op: iop, value: val }); if (token) q = q.or(token); continue; }
        if (val === null && (iop === 'eq' || iop === '=')) { q = q.is(col, null); continue; }
        if (val === null && (iop === 'ne' || iop === 'neq' || iop === '!=')) { q = q.not(col, 'is', null); continue; }
        if (iop === 'in') { q = Array.isArray(val) ? q.in(col, val) : q; continue; }
        if (iop === 'not_in') { q = q.filter(col, 'not.in', `(${(val || []).join(',')})`); continue; }
        if (iop === 'between') { const [a, b] = val || []; q = q.gte(col, a).lte(col, b); continue; }
        if (iop === 'like' || iop === 'ilike') { q = q.filter(col, iop, String(val)); continue; }
        if (iop === 'contains' || iop === 'icontains') { q = q.filter(col, iop === 'icontains' ? 'ilike' : 'like', `%${val}%`); continue; }
        if (iop === 'starts_with' || iop === 'istarts_with') { q = q.filter(col, iop === 'istarts_with' ? 'ilike' : 'like', `${val}%`); continue; }
        if (iop === 'ends_with' || iop === 'iends_with') { q = q.filter(col, iop === 'iends_with' ? 'ilike' : 'like', `%${val}`); continue; }
        if (iop === 'gt' || iop === 'gte' || iop === 'lt' || iop === 'lte') { q = q.filter(col, iop, String(val)); continue; }
        if (iop === 'is_null' || iop === 'isnull') { q = q.is(col, null); continue; }
        if (iop === 'not_null' || iop === 'notnull') { q = q.not(col, 'is', null); continue; }
        q = q.eq(col, val);
    }
    return q;
}

/* ========================= Projections & Cursors ========================= */
function buildProjection(select, expand){
  let sel = Array.isArray(select) ? select.map(s=>String(s).trim()).filter(Boolean).join(',') : (select || '*');
  if (expand && typeof expand==='object'){
    const base = (sel && sel.trim()!=='*') ? sel : '*';
    const segs = [base];
    for(const [k,cols] of Object.entries(expand)){
      const list=(cols==='*'||cols==null)?'*':(Array.isArray(cols)?cols.join(','):(()=>{throw new Error(`expand.${k} must be list or '*'`);})());
      segs.push(`${k}(${list})`);
    }
    sel = segs.join(',');
  }
  return sel;
}
function signCursor(obj){ if(!CURSOR_SECRET) return null; const payload=Buffer.from(JSON.stringify(obj)).toString('base64url');
  const mac=crypto.createHmac('sha256',CURSOR_SECRET).update(payload).digest('base64url'); return `${payload}.${mac}`; }
function verifyCursor(token){ if(!CURSOR_SECRET||!token) return null; const [p,mac]=String(token).split('.');
  const mac2=crypto.createHmac('sha256',CURSOR_SECRET).update(p).digest('base64url'); if(mac!==mac2) return null;
  try{ return JSON.parse(Buffer.from(p,'base64url').toString('utf8')); }catch{ return null; } }
function buildCursorToken(orderSeq, values, isAfter = true) {
    if (!orderSeq?.length || !Array.isArray(values) || values.length < orderSeq.length) return '';
    const clauses = [];
    for (let i = 0; i < orderSeq.length; i++) {
        const colName = orderSeq[i].column || orderSeq[i].field || Object.keys(orderSeq[i])[0];
        const parts = [];
        for (let j = 0; j < i; j++) {
            const prevCol = orderSeq[j].column || orderSeq[j].field || Object.keys(orderSeq[j])[0];
            parts.push(`${prevCol}.eq.${encodeToken(values[j])}`);
        }
        const asc = !(orderSeq[i].ascending === false || orderSeq[i].desc === true);
        const op = isAfter ? (asc ? 'gt' : 'lt') : (asc ? 'lt' : 'gt');
        parts.push(`${colName}.${op}.${encodeToken(values[i])}`);
        clauses.push(`and(${parts.join(',')})`);
    }
    return `or(${clauses.join(',')})`;
}

/* ========================= Event logging ========================= */
async function logEventRow(action, fqtn, durationMs, rowsAffected){
  if(!LOG_EVENTS && !LOG_TABLE_FQTN) return;
  try{
    if(LOG_EVENTS) console.error(`[event] ${action} ${fqtn} rows=${rowsAffected} ms=${durationMs}`);
    if(!LOG_TABLE_FQTN) return;
    const {schema, table} = splitFqtn(LOG_TABLE_FQTN);
    await supabase.schema(schema).from(table).insert({ action, fqtn, duration_ms: durationMs, rows: rowsAffected, created_at: nowIso() }, {returning:'minimal'});
  }catch{}
}

/* ========================= PART 1/4 END =========================
   Next: PART 2/4 will define tool signatures (toolsPayload) and
   CRUD handlers (query_table/insert/upsert/update/delete) + discovery.
================================================================== */
/**
 * MCP Supabase HTTP Server — Extended (PART 2/4)
 * Concatenate parts 1–4 into: /opt/supabase-mcp/runtime/index-http.js
 *   cat index-http-part1.txt index-http-part2.txt index-http-part3.txt index-http-part4.txt > index-http.js
 *
 * This part defines:
 *  - Tool schema (toolsPayload)
 *  - CRUD handlers (query_table, insert_data, upsert_data, update_data, delete_data)
 *  - Discovery handlers (list_schemas, list_tables, list_columns)
 */

//////////////////////////////
// Tool definitions (MCP)
//////////////////////////////
function toolsPayload(){
  return { tools: [
    // === CRUD + Discovery ===
      {
          name: 'query_table', description: 'Query data from a Supabase table. Where clause operators: eq, neq, gt, gte, lt, lte, like, ilike, in, not_in, between, contains, icontains, starts_with, ends_with, is_null, not_null, match, imatch, contains_all, contains_any, contained_by', inputSchema: {
      type:'object',
      additionalProperties: true,
      properties:{
        table:{type:'string'}, schema:{type:'string'},
        select:{oneOf:[{type:'string'},{type:'array'}], default:'*'},
          where: { type: 'object', description: 'Filter object. Each key is a column name, value is {op, value}. Valid ops: eq, neq, gt, gte, lt, lte, like, ilike, in, not_in, between, contains, icontains, starts_with, ends_with, is_null, not_null, match, imatch, contains_all, contains_any, contained_by' }, orderBy:{oneOf:[{type:'object'},{type:'array'}]},
        limit:{type:'number'}, offset:{type:'number'},
        expand:{type:'object'}, aggregate:{oneOf:[{type:'string'},{type:'array'},{type:'object'}]},
        distinct:{oneOf:[{type:'boolean'},{type:'array'}]},
        cursor:{type:'string'}, debug:{type:'object'}
      }, required:['table'] } },


    { name:'insert_data', description:'Insert data (idempotency + relation projection)', inputSchema:{
      type:'object',
      additionalProperties: true,
      properties:{
        table:{type:'string'}, schema:{type:'string'}, pk:{type:'string'},
        data:{oneOf:[{type:'object'},{type:'array'}]},
        returning:{type:'string', enum:['none','minimal','representation']},
        idempotency_key:{type:'string'},
        select:{oneOf:[{type:'string'},{type:'array'}]}, expand:{type:'object'},
        count:{type:'string', enum:['none','exact','planned','estimated']}
      }, required:['table','data'] } },

    { name:'upsert_data', description:'Upsert rows with on_conflict and ignore_duplicates (relation projection)', inputSchema:{
      type:'object',
      additionalProperties: true,
      properties:{
        table:{type:'string'}, schema:{type:'string'}, pk:{type:'string'},
        data:{oneOf:[{type:'object'},{type:'array'}]},
        on_conflict:{oneOf:[{type:'string'},{type:'array'}]},
        ignore_duplicates:{type:'boolean'},
        returning:{type:'string', enum:['none','minimal','representation']},
        idempotency_key:{type:'string'},
        select:{oneOf:[{type:'string'},{type:'array'}]}, expand:{type:'object'},
        count:{type:'string', enum:['none','exact','planned','estimated']}
      }, required:['table','data','on_conflict'] } },

    { name:'update_data', description:'Update rows (requires where; inc/dec; optimistic lock; relation projection)', inputSchema:{
      type:'object',
      additionalProperties: true,
      properties:{
        table:{type:'string'}, schema:{type:'string'}, pk:{type:'string'},
        data:{type:'object'}, where:{type:'object'}, orderBy:{oneOf:[{type:'object'},{type:'array'}]},
        limit:{type:'number'}, returning:{type:'string', enum:['none','minimal','representation']},
        idempotency_key:{type:'string'}, expected_updated_at:{type:'string'},
        select:{oneOf:[{type:'string'},{type:'array'}]}, expand:{type:'object'},
        count:{type:'string', enum:['none','exact','planned','estimated']}
      }, required:['table','data','where'] } },

    { name:'delete_data', description:'Delete rows (requires where; two-phase if order/limit)', inputSchema:{
      type:'object',
      additionalProperties: true,
      properties:{
        table:{type:'string'}, schema:{type:'string'}, pk:{type:'string'},
        where:{type:'object'}, orderBy:{oneOf:[{type:'object'},{type:'array'}]},
        limit:{type:'number'}, returning:{type:'string', enum:['none','minimal','representation']},
        count:{type:'string', enum:['none','exact','planned','estimated']}
      }, required:['table','where'] } },

{ name:'finalize_verification', description:'Flip speak-gate after final verification has succeeded', inputSchema:{
  type:'object',
  additionalProperties: true,
  properties:{
    chain:{type:'string'}, // optional: explicit chain id if you don't want header
    summary_rows:{type:'number', description:'Number of rows updated to report in final NL'}
  }, required:[]
}},



    { name:'list_schemas', description:'List allowed schemas (RPC with env allowlist fallback)', inputSchema:{
      type:'object',
      additionalProperties: true,
      properties:{} } },

    { name:'list_tables', description:'List base tables in a schema (RPC with safe fallback)', inputSchema:{
      type:'object',
      additionalProperties: true,
      properties:{schema:{type:'string'}}, required:['schema']} },

    { name:'list_columns', description:'List columns for a table (RPC with safe fallback)', inputSchema:{
      type:'object',
      additionalProperties: true,
      properties:{schema:{type:'string'},table:{type:'string'}}, required:['schema','table']} },

      /* === Catalog discovery (RPCs/functions/triggers) === */
      {
          name: 'list_rpcs', description: 'List RPC-like functions across schemas', inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: { p_schema: { type: 'string', description: 'Filter by schema (optional)' } }
          }
      },

      {
          name: 'get_function_definition', description: 'Get full source for a function/RPC', inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: { p_schema: { type: 'string' }, p_name: { type: 'string' } },
              required: ['p_schema', 'p_name']
          }
      },

      {
          name: 'list_functions', description: 'List all user functions (not only RPCs)', inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: { p_schema: { type: 'string', description: 'Filter by schema (optional)' } }
          }
      },

      {
          name: 'list_triggers', description: 'List table triggers and their call statements', inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: { p_schema: { type: 'string', description: 'Filter by schema (optional)' } }
          }
      },

      {
          name: 'list_event_triggers', description: 'List database-level event triggers', inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: {}
          }
      },

      {
          name: 'list_views', description: 'List views (optionally by schema)', inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: { p_schema: { type: 'string' }, schema: { type: 'string' } }
          }
      },

      {
          name: 'list_matviews', description: 'List materialized views (optionally by schema)', inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: { p_schema: { type: 'string' }, schema: { type: 'string' } }
          }
      },

      {
          name: 'get_view_definition', description: 'Return the SQL text for a view or matview',
          inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: {
                  p_schema: { type: 'string', description: 'Schema name' },
                  p_view: { type: 'string', description: 'View or matview name' },
                  schema: { type: 'string' }, view: { type: 'string' }, name: { type: 'string' }
              },
              required: []
          }
      },

      {
          name: 'get_trigger_definition', description: 'Return the trigger DDL with metadata',
          inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: {
                  p_schema: { type: 'string', description: 'Schema name' },
                  p_trigger: { type: 'string', description: 'Trigger name' },
                  schema: { type: 'string' }, trigger: { type: 'string' }, name: { type: 'string' }
              },
              required: []
          }
      },

      {
          name: 'chain', description: 'Run a sequence of MCP tools with variable interpolation',
          inputSchema: {
              type: 'object', additionalProperties: true,
              properties: {
                  steps: {
                      type: 'array', items: {
                          type: 'object', additionalProperties: true,
                          properties: {
                              name: { type: 'string' },
                              arguments: { type: 'object', additionalProperties: true },
                              saveAs: { type: 'string', description: 'Save this step output into ${saveAs} for later interpolation' }
                          },
                          required: ['name', 'arguments']
                      }
                  }
              },
              required: ['steps']
          }
      },

      {
          name: 'rpc_expose_constraints_filtered',
          description: 'List all constraints (PK, FK, UNIQUE, CHECK) for a specific table',
          inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: {
                  target_schema: {
                      type: 'string',
                      description: 'Schema name (e.g. "health")'
                  },
                  target_table: {
                      type: 'string',
                      description: 'Table name (e.g. "food_inventory")'
                  }
              },
              required: ['target_schema', 'target_table']
          }
      },
      {
          name: 'rpc_expose_indexes_filtered',
          description: 'List all indexes for a specific table with their properties and definitions',
          inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: {
                  target_schema: {
                      type: 'string',
                      description: 'Schema name (e.g. "health")'
                  },
                  target_table: {
                      type: 'string',
                      description: 'Table name (e.g. "food_inventory")'
                  }
              },
              required: ['target_schema', 'target_table']
          }
      },

      // === Developer / Runtime Editing Tools ===
      {
          name: 'tool_edit_slice',
          description: 'Replace a section of a file between specific line numbers',
          inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: {
                  relpath: { type: 'string', description: 'Relative path to the target file (within runtime)' },
                  start_line: { type: 'integer', description: 'Line number to start replacing (0-based index)' },
                  end_line: { type: 'integer', description: 'Line number to end replacing (exclusive)' },
                  new_lines: {
                      type: 'array',
                      items: { type: 'string' },
                      description: 'New lines of code to insert in place of the range'
                  }
              },
              required: ['relpath', 'start_line', 'end_line', 'new_lines']
          }
      },
      {
          name: 'tool_run_check',
          description: 'Perform syntax or lint validation on a file (e.g., `node --check`)',
          inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: {
                  relpath: { type: 'string', description: 'Relative path of the file to check' }
              },
              required: ['relpath']
          }
      },
      {
          name: 'tool_commit_file',
          description: 'Commit the modified file with a git message (used after edit validation)',
          inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: {
                  relpath: { type: 'string', description: 'Relative path of the file to commit' },
                  message: { type: 'string', description: 'Git commit message', default: 'Automated MCP edit' }
              },
              required: ['relpath']
          }
      },

      {
          name: 'send_email',
          description: 'Send a tracked Gmail message and log it to Supabase tables',
          inputSchema: {
              type: 'object',
              additionalProperties: true,
              properties: {
                  to: {
                      type: 'string',
                      description: 'Recipient email address'
                  },
                  cc: {                                    
                      type: 'string',
                      description: 'CC email address(es)'
                  },
                  bcc: {                                   
                      type: 'string',
                      description: 'BCC email address(es)'
                  },
                  subject: {
                      type: 'string',
                      description: 'Email subject line'
                  },
                  body: {
                      type: 'string',
                      description: 'Plaintext or HTML email body'
                  },
                  track: {
                      type: 'boolean',
                      description: 'Whether to track replies (default true)',
                      default: true
                  },
                  mode: {
                      type: 'string',
                      enum: ['draft', 'send'],
                      default: 'draft',
                      description: 'Create draft or send immediately'
                  },
                  attachments: {
                      type: 'array',
                      description: 'Array of {filename, filepath, mimeType} or {filename, content, mimeType}',
                      items: {
                          type: 'object',
                          properties: {
                              filename: { type: 'string' },
                              filepath: { type: 'string' },
                              content: { type: 'string', description: 'Base64 encoded content' },
                              mimeType: { type: 'string' }
                          }
                      }
                  },
                  thread_id: {
                      type: 'string',
                      description: 'Gmail thread ID for replies'
                  },
                  in_reply_to: {
                      type: 'string',
                      description: 'Message ID being replied to'
                  },

                  // ✅ NEW SIGNATURE PARAMETERS
                  signature_template: {
                      type: 'string',
                      enum: ['professional', 'basic', 'none'],
                      default: 'professional',
                      description: 'Signature style: professional (full credentials + LinkedIn), basic (name only), none (no signature)'
                  },
                  include_signature: {
                      type: 'boolean',
                      default: false,
                      description: 'Set to false to omit signature entirely'
                  },
                  sender_template: {
                      type: 'string',
                      enum: ['consulting', 'professional', 'basic'],
                      default: 'basic',
                      description: 'From address template: consulting (Harvard Statistics credential), professional (Steven Elliott), basic (Steve Elliott personal)'
                  },
                  create_followup_event: {
                      type: 'boolean',
                      default: false,
                      description: 'Enable automatic follow-up calendar event creation'
                  },
                  followup_days: {
                      type: 'number',
                      default: 7,
                      description: 'Days until follow-up (default 7)'
                  },
                  followup_time: {
                      type: 'string',
                      default: '15:00:00',
                      description: 'Time of day for follow-up in HH:MM:SS format (Chicago time)'
                  }

              },
              required: ['subject', 'body']  // ← Note: 'to' removed from required (drafts don't need it)
          }
      },



    // === Adapters (unchanged) ===
    { name:'http_fetch', description:'Fetch an HTTP(S) resource with safety, retries, destinations', inputSchema:{
      type:'object',
      additionalProperties: true,
      properties:{
        url:{type:'string'}, method:{type:'string', default:'GET'},
        headers:{type:'object'}, body:{oneOf:[{type:'string'},{type:'object'}]},
        timeout_ms:{type:'number'}, response_type:{type:'string', enum:['text','json','bytes'], default:'text'},
        allow_hosts:{oneOf:[{type:'string'},{type:'array'}]},
        deny_hosts:{oneOf:[{type:'string'},{type:'array'}]},
        redirect_policy:{type:'string', enum:['any','same_host','same_site','allow_hosts_only'], default:'any'},
        allow_redirect_hosts:{oneOf:[{type:'string'},{type:'array'}]},
        max_bytes:{type:'number'}, save_to:{type:'string'},
        paginate:{type:'boolean'},
        destination:{type:'string', enum:['none','github','supabase_storage','supabase_table']},
        destination_opts:{type:'object'}, destination_chain:{type:'array'},
        ua_pool:{type:'array', items:{type:'string'}},
        lang_pool:{type:'array', items:{type:'string'}},
        trace:{type:'boolean'}
      }, required:['url']
    }},

    { name:'notify_push', description:'Send a push/notify event (Slack, Pushover, webhook)', inputSchema:{
      type:'object',
      additionalProperties: true,
      properties:{
        provider:{type:'string', enum:['slack.webhook','pushover','webhook'], default:'slack.webhook'},
        webhook_url:{type:'string'}, channel:{type:'string'}, title:{type:'string'}, body:{type:'string'},
        message:{type:'string'}, level:{type:'string', enum:['info','warn','error'], default:'info'},
        meta:{type:'object'}, data:{type:'object'},
        user_key:{type:'string'}, api_token:{type:'string'}, priority:{type:'number'},
        no_log:{type:'boolean'},
        destination:{type:'string', enum:['none','github','supabase_storage','supabase_table']},
        destination_opts:{type:'object'}
      }, required:['title','body']
    }},

    { name:'browser_flow', description:'Run a multi-step Playwright automation flow with context, stealth, artifacts', inputSchema:{
      type:'object',
      additionalProperties: true,
      properties:{
        steps:{ type:'array', items:{
          type:'object',
          properties:{
            op:{type:'string'},
            url:{type:'string'}, selector:{type:'string'}, value:{type:'string'}, text:{type:'string'},
            delay_ms:{type:'number'}, wait_until:{type:'string', enum:['load','domcontentloaded','networkidle'], default:'load'},
            timeout_ms:{type:'number'}, full_page:{type:'boolean'}, path:{type:'string'}, format:{type:'string'},
            script:{type:'string'}, kind:{type:'string', enum:['text','html'], default:'text'},
            files:{oneOf:[{type:'string'},{type:'array'}]},
            destination:{type:'string', enum:['none','github','supabase_storage','supabase_table']},
            destination_opts:{type:'object'}
          }
        }},
        options:{ type:'object', properties:{
          proxy:{type:'object'}, geolocation:{type:'object'}, timezone:{type:'string'}, locale:{type:'string'},
          user_agent:{type:'string'}, viewport:{type:'object'}, storage_state_in:{type:'string'}, storage_state_out:{type:'string'},
          stealth:{type:'boolean'}, jitter_ms:{type:'number'}
        }}
      }, required:['steps']
    }}
  ]};
}

async function tool_encode_attachment({ filePath, encoding = 'base64url' }) {
    if (!filePath) throw new Error('Missing filePath');

    // Resolve path: absolute or relative to uploads directory
    let resolvedPath = filePath;
    if (!path.isAbsolute(filePath)) {
        resolvedPath = path.join(UPLOAD_DIR, filePath);
    }

    // Safety: ensure it doesn't escape upload dir
    const normalized = path.normalize(resolvedPath);
    if (!normalized.startsWith(UPLOAD_DIR)) {
        throw new Error(`Unsafe file path outside uploads directory: ${filePath}`);
    }

    // Verify existence
    if (!fs.existsSync(normalized)) {
        throw new Error(`File not found: ${normalized}`);
    }

    // Read + encode
    const data = fs.readFileSync(normalized);
    let encoded = data.toString('base64');

    // Gmail-safe Base64URL variant
    if (encoding === 'base64url') {
        encoded = encoded
            .replace(/\+/g, '-')
            .replace(/\//g, '_')
            .replace(/=+$/, ''); // strip padding
    }

    return asJsonContent({
        filename: path.basename(normalized),
        encoded,
        encoding,
        size_bytes: data.length,
        source_path: normalized
    });
}


//////////////////////////////
// Helpers used by CRUD
//////////////////////////////
//////////////////////////////
// CRUD: query_table
//////////////////////////////
async function tool_query_table(args){
    try {
        // Note: This function is called by tool_query_table_range for each day.
        const t0 = Date.now();
        const {
            schema:sArg, table:tArg, select='*', where, orderBy, limit, offset,
            expand, aggregate, distinct, cursor, debug
        } = args || {};

        const { schema, table } = parseTable(tArg, sArg);
        const fqtn = `${schema}.${table}`;
        enforceReadTableAllow(fqtn);

        // projection & allowlist
        let sel = buildProjection(select, expand);
        const baseSelect = Array.isArray(select) ? select.join(',') : (select || '*');
        enforceReadColAllow(fqtn, baseSelect);

        // aggregates
        const aggItems=[]; let countMode=null;
        const pushAgg=(fn,col)=>aggItems.push(`${fn}_${col}:${col}.${fn}()`);
        const aggObj=(typeof aggregate==='string'||Array.isArray(aggregate))?(Array.isArray(aggregate)?aggregate:[aggregate]):aggregate;
        if(Array.isArray(aggObj)){
            for(const token of aggObj){
            const t=String(token).trim().toLowerCase();
            if(t==='count'){ countMode='planned'; continue; }
            if(t.startsWith('count:')){ countMode=t.split(':',2)[1]; continue; }
            if(t.includes(':')){ const [fn,col]=t.split(':',2); if(['min','max','avg','sum'].includes(fn)) pushAgg(fn,col); }
            }
        } else if (aggObj && typeof aggObj==='object'){
            const v=aggObj.count; if(v) countMode=(v===true?'planned':String(v));
            for(const fn of ['min','max','avg','sum']){ const col=aggObj[fn]; if(col) pushAgg(fn,String(col)); }
        }
        if(aggItems.length) sel = sel ? `${sel},${aggItems.join(',')}` : aggItems.join(',');

        // count-only guardrail: strip ordering, force limit(0)
        let q;
        if(countMode && (Number(args?.limit||0)===0)){
            q = supabase.schema(schema).from(table).select('*',{count:countMode}).limit(0);
        } else {
            q = countMode
            ? supabase.schema(schema).from(table).select(sel,{count:countMode})
            : supabase.schema(schema).from(table).select(sel);
        }

        // where
        let whereToken;
        if(where && typeof where==='object'){
            if(where.op||where.conditions) whereToken=serializeBoolExpr(where);
            q = applyWhere(q, where);
        }

        // order
        let ob = Array.isArray(orderBy)?orderBy:(orderBy?[orderBy]:[]);
        if(ob.length===0) ob=[{column:'id',ascending:true}];
        if(!(countMode && (Number(args?.limit||0)===0))){
            for (const item of ob) {
                let col, desc;
                if (item.column || item.field) {
                    col = item.column || item.field;
                    desc = item.ascending === false || item.desc === true || String(item.direction).toLowerCase() === 'desc';
                } else {
                    // shorthand: {"colName": "asc"|"desc"}
                    const keys = Object.keys(item);
                    col = keys[0];
                    desc = keys[0] && String(item[keys[0]]).toLowerCase() === 'desc';
                }
                if (!col) continue; // guard against empty objects
                const nf = (item.nulls || '').toLowerCase();
                q = (nf === 'first' || nf === 'last')
                    ? q.order(col, { ascending: !desc, nullsFirst: nf === 'first' })
                    : q.order(col, { ascending: !desc });
            }
        }

        // multi-column cursor
        if(cursor && CURSOR_SECRET){
            const payload=verifyCursor(cursor); // { after:[...], before:[...], cols:[...] }
            if(payload?.cols && Array.isArray(payload.cols)){
            const seq = ob.length ? ob : payload.cols.map(c=>({column:c,ascending:true}));
            if(payload.after){ const token=buildCursorToken(seq,payload.after,true);  if(token) q=q.or(token); }
            if(payload.before){const token=buildCursorToken(seq,payload.before,false); if(token) q=q.or(token); }
            }
        }

            // paging
            if (!(countMode && (Number(args?.limit || 0) === 0))) {
                if (Number.isInteger(offset) && offset >= 0) { const lim = (Number.isInteger(limit) && limit > 0) ? limit : 100; q = q.range(offset, offset + lim - 1); }
                else if (Number.isInteger(limit) && limit > 0) q = q.limit(limit);
            }

        const resp = await execOrThrow(q);
        if (resp?.error) throw mapApiError(resp.error);
        let rows = resp?.data ?? resp ?? [];

        // distinct client-side
        let distinctTruncated = false;
        if (distinct) {
            const keys = Array.isArray(distinct) ? distinct
                : (typeof baseSelect === 'string' && baseSelect !== '*' ? baseSelect.split(',').map(s => s.trim()).filter(Boolean) : []);
            if (keys.length) {
                const preDedupCount = rows.length;
                const seen = new Set(), dedup = [];
                for (const r of rows) { const k = JSON.stringify(keys.map(k => r?.[k])); if (seen.has(k)) continue; seen.add(k); dedup.push(r); }
                rows = dedup;
                // warn if DB returned a full page before dedup — distinct results may be incomplete
                if (Number.isInteger(limit) && limit > 0 && preDedupCount === limit) {
                    distinctTruncated = true;
                }
            }
        }

        // next cursor
        let nextCursor=null;
        if(CURSOR_SECRET && rows?.length && Number.isInteger(limit) && limit>0){
            const cols = ob.map(o => o.column || o.field || Object.keys(o)[0]);
            const last = cols.map(c=>rows.at(-1)?.[c]);
            nextCursor = signCursor({ after:last, cols });
        }

        // count-only
        if(countMode && (Number(args?.limit||0)===0)){
            const cnt = resp?.count ?? 0;
            const body=[{count:Number(cnt)}];
            const meta=debug?.explain?{projection:sel,countMode,where:whereToken||'(compiled)',orderApplied:false}:undefined;
            await logEventRow('read', fqtn, Date.now()-t0, body.length);
            if (meta) return asJsonContent({ rows: body, meta });
            
            return asJsonContent({ rows: body });
        }

        const meta=debug?.explain?{projection:sel,where:whereToken||'(compiled)',aggregate:aggItems,orderBy:ob,cursorUsed:Boolean(cursor)}:undefined;
        const out = nextCursor || meta || distinctTruncated
            ? { rows, ...(nextCursor ? { nextCursor } : {}), ...(meta ? { meta } : {}), ...(distinctTruncated ? { warning: 'distinct results may be incomplete — page was full before deduplication, increase limit or paginate' } : {}) }
            : rows;
        await logEventRow('read', fqtn, Date.now()-t0, Array.isArray(rows)?rows.length:0);

        return asJsonContent(out);
    } catch (e) {
        const msg = (e && typeof e === 'object' && e.message) ? e.message : String(e);
        const details = (e && typeof e === 'object' && e.details) ? e.details : null;
        return asJsonContent({
            error: e?.code || "query_error",
            message: msg,
            details: details,
            hint: details?.hint || null,
            stack: process.env.NODE_ENV !== 'production' ? e?.stack || null : null
        });
    }
}


//////////////////////////////
// CRUD: idempotency helpers
//////////////////////////////
async function idempoCheck(key, op, fqtn, args){
  if(!key) return null;
  try{ const r=await supabase.schema('system').from('idempo_cache').select('result').eq('key',key).single();
    if(r?.data?.result) return r.data.result; }catch{}
  return null;
}
async function idempoStore(key, op, fqtn, args, result){
  if(!key) return;
  try{ await supabase.schema('system').from('idempo_cache').insert({ key, op, fqtn, args, result }, { returning:'minimal' }); }catch{}
}
function buildWriteMeta(rows, count){ if(!count||count==='none') return undefined; return { count: rows?.length ?? 0, mode: count }; }

//////////////////////////////
// Helper: detect math ops
//////////////////////////////
function hasMathOps(p) {
  return Object.values(p || {}).some(
    v => v && typeof v === 'object' && (v.op === 'inc' || v.op === 'dec')
  );
}

//////////////////////////////
// CRUD: insert_data (robust batched, JSON array friendly)
//////////////////////////////
async function tool_insert_data(args) {
    try {
        const t0 = Date.now();
        const {
            schema: sArg,
            table: tArg,
            data,
            idempotency_key,
            select,
            expand,
            count,
            pk,
            returning
        } = args || {};

        const { schema, table } = parseTable(tArg, sArg);
        const fqtn = `${schema}.${table}`;
        const pkCol = pk || 'id';

        enforceWriteTableAllow(fqtn);

        // Accept object | array | JSON string | NDJSON
        const rows = coerceRows(data);
        enforceWriteColsAllow(fqtn, rows);

        const hit = await idempoCheck(idempotency_key, 'insert', fqtn, args);
        if (hit) return asJsonContent(hit);

        const projection = buildProjection(select || '*', expand);
        const collected = [];
        const BATCH = Number(process.env.DBWRITE_MAX_BATCH || 200);

        for (const chunk of chunkBy(rows, BATCH)) {
            const base = supabase.schema(schema).from(table).insert(chunk);
            const resp = await execOrThrow((select || expand) ? base.select(projection) : base.select());
            const part = resp?.data ?? [];
            if (!part.length && returning !== 'none') {
                throw new Error(`insert_silent_fail: No rows returned from ${fqtn} — possible permission denial, constraint violation, or empty result`);
            }
            if (part?.length) collected.push(...part);
        }

        if (collected.length) {
            const ids = collected.map(r => r?.[pkCol]).filter(v => v != null);
            if (ids.length) await verifyRowsChanged(schema, table, pkCol, ids, 'insert_data');
        }

        const result = {
            rows: collected,
            ...(buildWriteMeta(collected, count) ? { meta: buildWriteMeta(collected, count) } : {})
        };

        await idempoStore(idempotency_key, 'insert', fqtn, args, result);
        await logEventRow('insert', fqtn, Date.now() - t0, collected.length);

        return asJsonContent(result);
    } catch (e) {
        const msg = (e && typeof e === 'object' && e.message) ? e.message : String(e);
        const details = (e && typeof e === 'object' && e.details) ? e.details : null;
        return asJsonContent({
            error: e?.code || "insert_error",
            message: msg,
            details: details,
            hint: details?.hint || null,
            stack: process.env.NODE_ENV !== 'production' ? e?.stack || null : null
        });
    }
}


// ---- Upsert helpers (full-row merge) ---------------------------------------

function getConflictKeyCols(onConflict, pkCol) {
    if (Array.isArray(onConflict) && onConflict.length) return onConflict;
    if (typeof onConflict === 'string' && onConflict.trim()) {
        return onConflict.split(',').map(s => s.trim());
    }
    return [pkCol];
}

async function getAllColumns(schema, table) {
    try {
        const { data, error } = await supabase.rpc('list_columns', { p_schema: schema, p_table: table });
        if (error) throw error;
        return (data || []).map(r => r.column_name);
    } catch (err) {
        // If RPC fails, fall back to information_schema
        const { data, error: fbErr } = await supabase
            .from('information_schema.columns')
            .select('column_name')
            .eq('table_schema', schema)
            .eq('table_name', table)
            .order('ordinal_position', { ascending: true });
        if (fbErr) throw fbErr;
        return (data || []).map(r => r.column_name);
    }
}

async function prefetchExistingByPk(schema, table, pkCol, rows) {
    const ids = rows.map(r => r?.[pkCol]).filter(v => v != null);
    if (!ids.length) return new Map();
    const res = await supabase.schema(schema).from(table).select('*').in(pkCol, ids);
    if (res.error) throw mapApiError(res.error);
    const map = new Map();
    for (const row of (res?.data || [])) map.set(row[pkCol], row);
    return map;
}

async function prefetchExistingByKeys(schema, table, keyCols, rows) {
    const map = new Map();
    for (const r of rows) {
        let q = supabase.schema(schema).from(table).select('*').limit(1);
        for (const c of keyCols) q = q.eq(c, r?.[c]);
        const res = await q;
        if (res.error) throw mapApiError(res.error);
        const got = Array.isArray(res?.data) ? res.data[0] : null;
        if (got) map.set(JSON.stringify(keyCols.map(k => r?.[k])), got);
    }
    const keyFor = (r) => JSON.stringify(keyCols.map(k => r?.[k]));
    return { map, keyFor };
}

//////////////////////////////
// CRUD: upsert_data (multi-row, batched, Supabase JS v2)
//////////////////////////////
async function tool_upsert_data(args) {
    try {
        const t0 = Date.now();
        const {
            schema: sArg,
            table: tArg,
            data,
            on_conflict,
            ignore_duplicates,
            idempotency_key,
            select,
            expand,
            count,
            pk
        } = args || {};

        const { schema, table } = parseTable(tArg, sArg);
        const fqtn = `${schema}.${table}`;
        const pkCol = pk || 'id';

        enforceWriteTableAllow(fqtn);

        // Accept object | array | JSON | NDJSON
        const rows = coerceRows(data);
        enforceWriteColsAllow(fqtn, rows);

        // Idempotency
        const hit = await idempoCheck(idempotency_key, 'upsert', fqtn, args);
        if (hit) return asJsonContent(hit);

        // Determine conflict keys
        const conflictKeys = getConflictKeyCols(on_conflict, pkCol);

        // Fatal errors that should never be swallowed
        const FATAL_CODES = new Set([
            '42P01', 'undefined_table',
            '42703', 'undefined_column',
            '23503', 'foreign_key_violation',
            '23502', 'not_null_violation',
            '23514', 'check_violation'
        ]);

        function isFatal(err) {
            const code = err?.code || err?.details?.code;
            const msg = err?.message || '';
            return (
                FATAL_CODES.has(code) ||
                /relation .+ does not exist/i.test(msg) ||
                /does not exist/i.test(msg) ||
                /foreign key/i.test(msg) ||
                /not.null/i.test(msg) ||
                /check constraint/i.test(msg) ||
                /invalid input syntax/i.test(msg)
            );
        }

        // ---- Prefetch existing rows and merge full rows before upsert ------------
        let mergedRows = rows;

        try {
            if (conflictKeys.length === 1) {
                const key = conflictKeys[0];
                if (key === pkCol) {
                    const existingMap = await prefetchExistingByPk(schema, table, pkCol, rows);
                    mergedRows = rows.map(r => {
                        const base = existingMap.get(r?.[pkCol]) || {};
                        return { ...base, ...r };
                    });
                } else {
                    const { map, keyFor } = await prefetchExistingByKeys(schema, table, [key], rows);
                    mergedRows = rows.map(r => {
                        const base = map.get(keyFor(r)) || {};
                        return { ...base, ...r };
                    });
                }
            } else {
                const { map, keyFor } = await prefetchExistingByKeys(schema, table, conflictKeys, rows);
                mergedRows = rows.map(r => {
                    const base = map.get(keyFor(r)) || {};
                    return { ...base, ...r };
                });
            }
        } catch (prefetchErr) {
            console.error('[upsert isFatal check]', {
                code: prefetchErr?.code,
                message: prefetchErr?.message,
                isFatalResult: isFatal(prefetchErr)
            });
            if (isFatal(prefetchErr)) throw prefetchErr;
            console.error('[upsert_data] prefetch/merge failed; proceeding without merge:', prefetchErr?.message || prefetchErr);
            mergedRows = rows;
        }

        // ---- Normalize merged rows to include all columns ------------------------
        try {
            const allCols = await getAllColumns(schema, table);
            if (allCols.length) {
                mergedRows = mergedRows.map(r => {
                    const out = {};
                    for (const c of allCols) {
                        if (r[c] !== undefined) out[c] = r[c];
                    }
                    return out;
                });
            }
        } catch (normErr) {
            if (isFatal(normErr)) throw normErr;
            console.error('[upsert_data] column normalization failed:', normErr?.message || normErr);
        }

        const onConflictCsv =
            Array.isArray(on_conflict) && on_conflict.length
                ? on_conflict.join(',')
                : (on_conflict || pkCol);

        // --- build safe projection string ---
        let projection = '*';
        try {
            const built = buildProjection(select || '*', expand);
            projection = typeof built === 'string' ? built : '*';
        } catch {
            projection = '*';
        }

        const collected = [];

        for (const chunk of chunkBy(mergedRows, DBWRITE_MAX_BATCH)) {
            try {
                const base = supabase
                    .schema(schema)
                    .from(table)
                    .upsert(chunk, { onConflict: onConflictCsv, ignoreDuplicates: !!ignore_duplicates });

                const resp = await execOrThrow(base.select(projection));
                if (resp?.error) throw mapApiError(resp.error); // ← add this line
                const part = resp?.data ?? [];
                if (part?.length) collected.push(...part);
            } catch (err) {
                if (isFatal(err)) throw err;
                console.error('[upsert_data] batch upsert failed, attempting per-row fallback:', err?.message || err);

                for (const row of chunk) {
                    try {
                        const single = supabase
                            .schema(schema)
                            .from(table)
                            .upsert(row, { onConflict: onConflictCsv, ignoreDuplicates: !!ignore_duplicates });

                        const r = await execOrThrow((select || expand) ? single.select(projection) : single.select());
                        const d = r?.data ?? [];
                        if (d?.length) collected.push(...d);
                    } catch (inner) {
                        console.error('[upsert_data] single-row fallback failed', inner?.message || inner);
                        throw structuredError('verify_failed_upsert', {
                            tool: 'upsert_data',
                            table: fqtn,
                            row,
                            error: inner?.message || inner
                        });
                    }
                }
            }
        }

        // Post-write verification by PK values
        if (collected.length) {
            const ids = collected.map(r => r?.[pkCol]).filter(v => v != null);
            if (ids.length) await verifyRowsChanged(schema, table, pkCol, ids, 'upsert_data');
        }

        const result = {
            rows: collected,
            ...(buildWriteMeta(collected, count) ? { meta: buildWriteMeta(collected, count) } : {})
        };

        await idempoStore(idempotency_key, 'upsert', fqtn, args, result);
        await logEventRow('upsert', fqtn, Date.now() - t0, collected.length);

        return asJsonContent(result);

    } catch (e) {
        const msg = (e && typeof e === 'object' && e.message) ? e.message : String(e);
        const details = (e && typeof e === 'object' && e.details) ? e.details : null;
        return asJsonContent({
            error: e?.code || 'upsert_error',
            message: msg,
            details: details,
            hint: details?.hint || null,
            stack: process.env.NODE_ENV !== 'production' ? e?.stack || null : null
        });
    }
}

//////////////////////////////
// CRUD: update_data (multi-row support + batching, Supabase JS v2)
//////////////////////////////
async function tool_update_data(args) {
    try {
        const t0 = Date.now();
        const {
            schema: sArg,
            table: tArg,
            data,
            where,
            orderBy,
            limit,
            idempotency_key,
            expected_updated_at,
            select,
            expand,
            count,
            // Keys / options
            pk,                 // string | string[]  (required for per-row multi-update)
            upsert_on_conflict  // string[] | string (optional; defaults to pk if provided)
        } = args || {};

        const { schema, table } = parseTable(tArg, sArg);
        const fqtn = `${schema}.${table}`;

            // ============================================================================
            // Guardrail: require WHERE for set-based updates (but allow per-row mode without WHERE)
            // STRENGTHENED: Now always requires WHERE or PK, regardless of env var
            // ============================================================================
            if (!Array.isArray(data)) {
                // Set-based update mode
                const hasWhere = where && typeof where === "object" && Object.keys(where).length > 0;
                const hasPk = pk && (Array.isArray(pk) ? pk.length > 0 : true);

                // ALWAYS require either WHERE or PK (env var only makes error message stricter)
                if (!hasWhere && !hasPk) {
                    const envMsg = REQUIRE_WHERE_UPD_DEL
                        ? " (DBWRITE_REQUIRE_WHERE_FOR_UPDATE_DELETE=1)"
                        : "";
                    throw new Error(
                        `unsafe_write: Set-based update requires non-empty 'where' clause OR 'pk' parameter${envMsg}. ` +
                        `Received: where=${JSON.stringify(where)}, pk=${pk}`
                    );
                }
            }
            // ============================================================================

        enforceWriteTableAllow(fqtn);


        // Normalize pk columns (never assume 'id')
        const pkCols = Array.isArray(pk) ? pk : (pk ? [pk] : []);

        // Utility: build WHERE from PK(s)
        function buildPkWhere(rowOrWhere) {
            if (!pkCols.length) return null;
            const w = {};
            for (const c of pkCols) {
                let v = rowOrWhere?.[c] ?? where?.[c];
                // Unwrap {op, value} filter objects to get the scalar
                if (v && typeof v === 'object' && 'value' in v) v = v.value;
                if (v === undefined || v === null) return null;
                w[c] = v;
            }
            return w;
        }

        // Utility: projection string
        const projection = buildProjection(select || "*", expand);

        // Idempotency
        const hit = await idempoCheck(idempotency_key, "update", fqtn, args);
        if (hit) return asJsonContent(hit);

        // =========================
        // BRANCH 1: PER-ROW MULTI-UPDATE
        // =========================
        if (Array.isArray(data)) {
            if (!pkCols.length) throw new Error("multi-row update requires 'pk' (string or string[])");
            // Validate PK presence on each row
            for (const row of data) {
            for (const c of pkCols) {
                if (row[c] == null) throw new Error(`multi-row update: missing PK field '${c}'`);
            }
            }

            enforceWriteColsAllow(fqtn, data);

            const DBWRITE_MAX_BATCH = Number(process.env.DBWRITE_MAX_BATCH || 200);
            const collected = [];

            for (const chunk of chunkBy(data, DBWRITE_MAX_BATCH)) {
            for (const row of chunk) {
                // Build PK where
                const wherePK = {};
                for (const c of pkCols) wherePK[c] = row[c];

                // Build patch (exclude PK fields)
                const patch = {};
                for (const [k, v] of Object.entries(row)) {
                if (!pkCols.includes(k)) {
                    if (v === undefined) patch[k] = null;
                    else if (typeof v === "string" && v.trim() === "") patch[k] = null;
                    else if (typeof v === "string" && !isNaN(v)) patch[k] = Number(v);
                    else patch[k] = v;
                }
                }

                // Skip no-op patches
                if (!Object.keys(patch).length) continue;

                // v2: update().select() to get rows back
                let q = supabase.schema(schema).from(table).update(patch).select(projection);
                for (const c of pkCols) q = q.eq(c, wherePK[c]);
                const r = await execOrThrow(q);
                const rows = r?.data ?? [];
                if (rows.length) collected.push(...rows);
            }
            }

            // Optional verification by the first PK (best-effort)
            if (collected.length && pkCols.length === 1) {
            const ids = collected.map(r => r?.[pkCols[0]]).filter(v => v != null);
            if (ids.length) await verifyRowsChanged(schema, table, pkCols[0], ids, "update_data");
            }

            const result = {
            rows: collected,
            ...(buildWriteMeta(collected, count) ? { meta: buildWriteMeta(collected, count) } : {})
            };

            await idempoStore(idempotency_key, "update", fqtn, args, result);
            await logEventRow("update", fqtn, Date.now() - t0, collected.length);
            return asJsonContent(result);
        }

        // =========================
        // BRANCH 2: SET-BASED SINGLE PATCH (your advanced logic, v2-correct)
        // =========================

        // Helper: detect math ops
        function hasMathOpsLocal(p) {
            return Object.values(p || {}).some(
            v => v && typeof v === "object" && (v.op === "inc" || v.op === "dec")
            );
        }

        // Normalize data payload (single object)
        const normData = {};
        for (const [k, v] of Object.entries(data || {})) {
            if (v === null || v === undefined) { normData[k] = null; continue; }
            else if (typeof v === "string" && v.trim() === "") normData[k] = null;
            else if (typeof v === "string" && !isNaN(v)) normData[k] = Number(v);
            else if (typeof v === "object" && (v.op === "inc" || v.op === "dec")) {
            // Math ops handled only in two-phase below
            normData[k] = v;
            } else normData[k] = v;
        }

        enforceWriteColsAllow(fqtn, normData);

        // Two-phase needed for math ops / order / limit / optimistic lock
        const needTwoPhase = !!limit || !!orderBy || hasMathOpsLocal(normData);

        // Pre-fetch candidate rows if needed (deterministic)
        let targetRows = [];
        if (needTwoPhase) {
            let rq = supabase.schema(schema).from(table).select("*");
            if (where) rq = applyWhere(rq, where);

            const seq = Array.isArray(orderBy) ? orderBy : orderBy ? [orderBy] : [];
            for (const item of seq) {
            const col = item.column || item.field;
            const desc = item.ascending === false || item.desc === true;
            rq = rq.order(col, { ascending: !desc });
            }
            if (limit) rq = rq.limit(Number(limit));

            const pre = await execOrThrow(rq);
            targetRows = pre?.data ?? [];
            if (!targetRows.length) {
            const empty = { rows: [], meta: buildWriteMeta([], count) };
            await idempoStore(idempotency_key, "update", fqtn, args, empty);
            await logEventRow("update", fqtn, Date.now() - t0, 0);
            return asJsonContent({ rows: [], meta: buildWriteMeta([], count) });
            }
        } else {
            // If no prefetch, still try to gather PKs for verification
            if (pkCols.length) {
            const w = buildPkWhere(where);
                if (!w) {
                    // SAFETY: if no PK can be built from where, require that where is non-empty
                    // to avoid fetching all rows as update candidates
                    if (!where || Object.keys(where).length === 0) {
                        throw new Error(
                            `SAFETY_VIOLATION: cannot resolve PK from args and no WHERE clause provided. ` +
                            `This would update every row in ${fqtn}. ` +
                            `Provide 'where' containing PK columns, or pass the PK value inside 'where', not as a top-level arg.`
                        );
                    }
                    let rq = supabase.schema(schema).from(table).select(pkCols.join(","));
                    if (where) rq = applyWhere(rq, where);
                    const pre = await execOrThrow(rq);
                    targetRows = pre?.data ?? [];
                } else {
                targetRows = [w]; // carry just PKs forward
            }
            }
        }

        // Helper to apply the patch to a single row scope, with verification and optional upsert fallback
        async function updateOne(pkeyValues, baseRow) {
            // Resolve math ops using baseRow when available
            const patch = {};
            for (const [k, v] of Object.entries(normData)) {
            if (v && typeof v === "object" && (v.op === "inc" || v.op === "dec")) {
                const current = baseRow ? Number(baseRow[k] ?? 0) : 0;
                const delta = Number(v.value || 0) * (v.op === "dec" ? -1 : 1);
                patch[k] = current + delta;
            } else patch[k] = v;
            }
            if (baseRow && "updated_at" in baseRow) patch.updated_at = nowIso();

            // ============================================================================
            // CRITICAL SAFETY: Verify we have a filter before executing update
            // ============================================================================
            const hasPkFilter = pkCols.length > 0 && pkeyValues &&
                pkCols.every(c => pkeyValues[c] !== undefined && pkeyValues[c] !== null);
            const hasWhereFilter = where && typeof where === "object" && Object.keys(where).length > 0;

            if (!hasPkFilter && !hasWhereFilter) {
                throw new Error(
                    `SAFETY_VIOLATION: updateOne() called without valid PK or WHERE filter. ` +
                    `This would update all rows in ${fqtn}. ` +
                    `pkeyValues=${JSON.stringify(pkeyValues)}, where=${JSON.stringify(where)}`
                );
            }
            // ============================================================================

            // Execute update (v2: chain .select())
            let q = supabase.schema(schema).from(table).update(patch).select();
            if (pkCols.length && pkeyValues) {
            for (const c of pkCols) q = q.eq(c, pkeyValues[c]);
            } else if (where) {
            q = applyWhere(q, where);
            }
            const r = await execOrThrow(q);
            let rows = r?.data ?? [];

            // Deterministic verification by PK(s) when available
            const pkWhere = pkCols.length ? (pkeyValues || buildPkWhere(where)) : null;
            let verified = false;
            if (pkWhere) {
            const proj = await execOrThrow(
                supabase.schema(schema).from(table).select("*").match(pkWhere)
            );
            const got = proj?.data ?? [];
            if (got.length === 1) {
                const g = got[0];
                verified = Object.entries(patch).every(([k, v]) =>
                (v && typeof v === "object") ? true : (g[k] === v || v === null)
                );
                rows = got;
            }
            } else {
            verified = Array.isArray(rows) && rows.length > 0;
            }

            // Fallback: upsert if not verified
            if (!verified) {
            const conflictCols = upsert_on_conflict
                ? (Array.isArray(upsert_on_conflict) ? upsert_on_conflict : [upsert_on_conflict])
                : pkCols;

            if (conflictCols.length) {
                // SAFETY: never upsert without valid PK values — would cause full-table update
                if (!pkWhere || conflictCols.some(c => pkWhere[c] == null)) {
                    throw new Error(
                        `SAFETY_VIOLATION: upsert fallback aborted — missing PK values in pkWhere. ` +
                        `pkWhere=${JSON.stringify(pkWhere)}, conflictCols=${JSON.stringify(conflictCols)}`
                    );
                }
                const upsertRec = { ...patch, ...(pkWhere || {}) };
                const u = await execOrThrow(
                supabase
                    .schema(schema)
                    .from(table)
                    .upsert(upsertRec, { onConflict: conflictCols.join(",") })
                    .select()
                );
                let urows = u?.data ?? [];

                if (pkWhere) {
                const proj2 = await execOrThrow(
                    supabase.schema(schema).from(table).select("*").match(pkWhere)
                );
                urows = proj2?.data ?? urows;
                const g2 = (proj2?.data ?? [])[0];
                verified = !!g2 && Object.entries(patch).every(([k, v]) =>
                    (v && typeof v === "object") ? true : (g2[k] === v || v === null)
                );
                } else {
                verified = Array.isArray(urows) && urows.length > 0;
                }

                if (!verified) {
                throw structuredError("verify_failed", {
                    tool: "update_data",
                    table: fqtn,
                    pkWhere: pkWhere || where,
                    patch
                });
                }
                return urows;
            } else {
                throw structuredError("verify_failed_no_conflict_keys", {
                tool: "update_data",
                table: fqtn,
                pkWhere: pkWhere || where,
                patch
                });
            }
            }

            return rows;
        }

        // Execute with ≤5 batch groups, per-row verification
        const collected = [];
        if (needTwoPhase) {
            for (const grp of chunkBy(targetRows, 5)) {
            for (const base of grp) {
                const pkWhere = pkCols.length ? Object.fromEntries(pkCols.map(c => [c, base[c]])) : null;
                const rows = await updateOne(pkWhere, base);
                if (rows?.length) collected.push(...rows);
            }
            }
        } else {
            const candidates = targetRows.length ? targetRows : [buildPkWhere(where) || null];

            // ============================================================================
            // CRITICAL SAFETY: Verify candidates are valid before batch execution
            // ============================================================================
            if (candidates.some(c => c === null || (typeof c === "object" && Object.keys(c).length === 0))) {
                throw new Error(
                    `SAFETY_VIOLATION: Generated null or empty candidate for update. ` +
                    `This indicates missing PK values in where clause. ` +
                    `Provide 'pk' parameter or ensure 'where' contains all PK columns. ` +
                    `where=${JSON.stringify(where)}, pk=${JSON.stringify(pk)}`
                );
            }
            // ============================================================================

            for (const grp of chunkBy(candidates, 5)) {
            for (const pkw of grp) {
                const rows = await updateOne(pkw, null);
                if (rows?.length) collected.push(...rows);
            }
            }
        }

        // Optional projection (expand/select)
        let finalRows = collected;
        if (finalRows.length && (expand || select)) {
            const sel = projection;
            if (pkCols.length) {
            // dedupe by composite PK key
            const keys = new Map();
            for (const r of finalRows) {
                const key = pkCols.map(c => r?.[c]).join("::");
                if (!keys.has(key)) keys.set(key, r);
            }
            const anyPk = pkCols[0];
            const anyVals = Array.from(keys.values()).map(r => r?.[anyPk]).filter(v => v != null);
            const proj = await execOrThrow(
                supabase.schema(schema).from(table).select(sel).in(anyPk, anyVals)
            );
            finalRows = proj?.data ?? finalRows;
            } else {
            let rq = supabase.schema(schema).from(table).select(sel);
            if (where) rq = applyWhere(rq, where);
            const proj = await execOrThrow(rq);
            finalRows = proj?.data ?? finalRows;
            }
        }

        const result = {
            rows: finalRows,
            ...(buildWriteMeta(finalRows, count) ? { meta: buildWriteMeta(finalRows, count) } : {})
        };

        await idempoStore(idempotency_key, "update", fqtn, args, result);
        await logEventRow("update", fqtn, Date.now() - t0, finalRows.length);

        return asJsonContent(result);
    } catch (e) {
        const msg = (e && typeof e === 'object' && e.message) ? e.message : String(e);
        const details = (e && typeof e === 'object' && e.details) ? e.details : null;
        return asJsonContent({
            error: e?.code || "update_error",
            message: msg,
            details: details,
            hint: details?.hint || null,
            stack: process.env.NODE_ENV !== 'production' ? e?.stack || null : null
        });
    }
}

async function tool_delete_data(args) {
    try {
        const t0 = Date.now();
        const {
            schema: sArg,
            table: tArg,
            where,
            orderBy,
            limit,
            count,
            pk,
            idempotency_key
        } = args || {};

        const { schema, table } = parseTable(tArg, sArg);
        const fqtn = `${schema}.${table}`;
        const pkCol = pk || 'id';

        // Always require where or pk regardless of env var
        const hasWhere = where && typeof where === 'object' && Object.keys(where).length > 0;
        const hasPk = pk && (Array.isArray(pk) ? pk.length > 0 : true);
        if (!hasWhere && !hasPk) {
            throw new Error(
                'unsafe_write: delete requires a non-empty where clause or pk parameter to prevent full-table deletion'
            );
        }

        enforceWriteTableAllow(fqtn);

        // Idempotency
        const hit = await idempoCheck(idempotency_key, 'delete', fqtn, args);
        if (hit) return asJsonContent(hit);

        const needTwoPhase = !!limit || !!orderBy;
        let out = [];

        if (needTwoPhase) {
            let rq = supabase.schema(schema).from(table).select(pkCol);
            if (where) rq = applyWhere(rq, where);
            const seq = Array.isArray(orderBy) ? orderBy : orderBy ? [orderBy] : [];
            for (const item of seq) {
                const col = item.column || item.field;
                const desc = item.ascending === false || item.desc === true;
                rq = rq.order(col, { ascending: !desc });
            }
            if (limit) rq = rq.limit(Number(limit));

            const pre = await execOrThrow(rq);
            const ids = (pre?.data ?? []).map(r => r[pkCol]).filter(v => v != null);

            if (ids.length) {
                const dq = await execOrThrow(
                    supabase.schema(schema).from(table).delete().in(pkCol, ids).select()
                );
                out = dq?.data ?? [];

                // Post-delete verification
                const verify = await supabase.schema(schema).from(table).select(pkCol).in(pkCol, ids);
                if (verify?.data?.length) {
                    throw new Error(`delete_verify_failed: ${verify.data.length} rows still exist after delete`);
                }
            }
        } else {
            let q = supabase.schema(schema).from(table).delete().select();
            if (where) q = applyWhere(q, where);
            const resp = await execOrThrow(q);
            out = resp?.data ?? [];
        }

        const result = {
            rows: out,
            ...(buildWriteMeta(out, count) ? { meta: buildWriteMeta(out, count) } : {})
        };

        await idempoStore(idempotency_key, 'delete', fqtn, args, result);
        await logEventRow('delete', fqtn, Date.now() - t0, out.length);

        return asJsonContent(result);

    } catch (e) {
        const msg = (e && typeof e === 'object' && e.message) ? e.message : String(e);
        const details = (e && typeof e === 'object' && e.details) ? e.details : null;
        return asJsonContent({
            error: e?.code || 'delete_error',
            message: msg,
            details: details,
            hint: details?.hint || null,
            stack: process.env.NODE_ENV !== 'production' ? e?.stack || null : null
        });
    }
}

//////////////////////////////
// Discovery: list_* tools
//////////////////////////////
async function tool_list_schemas(){
  try{
    const {data,error}=await supabase.rpc('list_schemas');
    if(error) throw error;
    if(Array.isArray(data)) return asJsonContent(data);
  }catch(e){
    console.warn('[list_schemas] RPC failed; falling back:', e?.message||e);
  }
  const schemas=parseAllowedSchemas().map(s=>({schema_name:s}));
  return asJsonContent(schemas);
}

async function tool_list_tables(args){
  const schema=args?.schema; if(!schema) throw new Error('Missing "schema"');
  try{
    const {data,error}=await supabase.rpc('list_tables',{p_schema:schema});
    if(!error&&Array.isArray(data)) return asJsonContent(data);
    if(error) throw error;
  }catch(e){
    console.warn('[list_tables] RPC failed; trying information_schema:', e?.message||e);
  }
  try{
    const {data,error}=await supabase.from('information_schema.tables')
      .select('table_schema,table_name')
      .eq('table_schema',schema)
      .eq('table_type','BASE TABLE')
      .order('table_name',{ascending:true});
    if(error) throw error;
    return asJsonContent(data ?? []);
  }catch(e){
    const msg=`[list_tables] fallback failed: ${e?.message||String(e)}. Hint: create RPC public.list_tables(p_schema) or expose information_schema.`;
    console.error(msg); return asJsonContent({ error: msg });
  }
}

async function tool_list_columns(args){
  const schema=args?.schema, table=args?.table; if(!schema||!table) throw new Error('Missing "schema" and/or "table"');
  try{
    const {data,error}=await supabase.rpc('list_columns',{p_schema:schema,p_table:table});
    if(!error&&Array.isArray(data)) return asJsonContent(data);
    if(error) throw error;
  }catch(e){
    console.warn('[list_columns] RPC failed; trying information_schema:', e?.message||e);
  }
  try{
    const {data,error}=await supabase.from('information_schema.columns')
      .select('table_schema,table_name,column_name,data_type,is_nullable,column_default,ordinal_position')
      .eq('table_schema',schema).eq('table_name',table)
      .order('ordinal_position',{ascending:true});
    if(error) throw error;
    return asJsonContent(data ?? []);
  }catch(e){
    const msg=`[list_columns] fallback failed: ${e?.message||String(e)}. Hint: create RPC public.list_columns(p_schema,p_table) or expose information_schema.`;
    console.error(msg); return asJsonContent({ error: msg });
  }
}

async function tool_list_rpcs(args) {
    const p_schema = args?.p_schema ?? null;
    const { data, error } = await supabase.rpc('list_rpcs', { p_schema });
    if (error) throw error;
    // Ensure data is not null/undefined
    const result = Array.isArray(data) && data.length > 0 ? data : [];
    return asJsonContent(result);
}

async function tool_get_function_definition(args) {
    const p_schema = args?.p_schema ?? args?.schema;
    const p_name = args?.p_name ?? args?.name;
    const p_arg_types = args?.p_arg_types ?? args?.arg_types ?? null;

    if (!p_schema || !p_name)
        throw new Error("Missing required parameters: p_schema and p_name");

    const { data, error } = await supabase.rpc('get_function_definition', {
        p_schema,
        p_name,
        p_arg_types
    });

    if (error) {
        console.error("[get_function_definition] RPC error:", error);
        throw error;
    }

    // More defensive handling
    const result = data ? (Array.isArray(data) ? data : [data]) : [];
    console.error("[get_function_definition] returning:", JSON.stringify(result).slice(0, 200));
    return asJsonContent(result);
}


async function tool_list_functions(args) {
    const p_schema = args?.p_schema ?? null;
    const { data, error } = await supabase.rpc('list_functions', { p_schema });
    if (error) throw error;
    return asJsonContent(Array.isArray(data) ? data : []);
}

async function tool_list_triggers(args) {
    const p_schema = args?.p_schema ?? null;
    const { data, error } = await supabase.rpc('list_triggers', { p_schema });
    if (error) throw error;
    return asJsonContent(Array.isArray(data) ? data : []);
}

async function tool_list_event_triggers(_args) {
    const { data, error } = await supabase.rpc('list_event_triggers');
    if (error) throw error;
    return asJsonContent(Array.isArray(data) ? data : []);
}

async function tool_list_views(args) {
    const p_schema = args?.p_schema ?? args?.schema ?? null;
    const { data, error } = await supabase.rpc('list_views', { p_schema });
    if (error) throw error;
    return asJsonContent(Array.isArray(data) ? data : []);
}

async function tool_list_matviews(args) {
    const p_schema = args?.p_schema ?? args?.schema ?? null;
    const { data, error } = await supabase.rpc('list_matviews', { p_schema });
    if (error) throw error;
    return asJsonContent(Array.isArray(data) ? data : []);
}

async function tool_get_view_definition(args) {
    const p_schema = args?.p_schema ?? args?.schema ?? null;
    const p_view = args?.p_view ?? args?.view ?? args?.name ?? null;
    if (!p_schema || !p_view) throw new Error('get_view_definition requires p_schema and p_view/name');
    const { data, error } = await supabase.rpc('get_view_definition', { p_schema, p_view });
    if (error) throw error;
    return asJsonContent({ schema: p_schema, view: p_view, definition: data ?? null });
}

async function tool_get_trigger_definition(args) {
    const p_schema = args?.p_schema ?? args?.schema ?? null;
    const p_trigger = args?.p_trigger ?? args?.trigger ?? args?.name ?? null;
    if (!p_schema || !p_trigger) throw new Error('get_trigger_definition requires p_schema and p_trigger/name');
    const { data, error } = await supabase.rpc('get_trigger_definition', { p_schema, p_trigger });
    if (error) throw error;
    // data is a row or null; normalize to object or null
    const row = Array.isArray(data) ? (data[0] ?? null) : data ?? null;
    return asJsonContent(row ?? { schema: p_schema, trigger: p_trigger, definition: null });
}

//////////////////////////////
// tool_send_email (WITH ATTACHMENT SUPPORT - FIXED)
// ✅ Creates drafts with tracking headers (X-Agent-Tag)
// ✅ Sends emails directly with full tracking
// ✅ Supports file attachments (PDF, etc.)
// ❌ NO send_draft mode - use Gmail API directly to send drafts
// ✅ Ingest cron handles all tracking activation
// 🔧 FIX: No longer defaults To: field to user's own email
// 🔧 FIX: Parses attachments if received as JSON string
// 🔧 FIX: HTML formatting now works correctly with attachments
//////////////////////////////
async function tool_send_email(args) {
    const {
        to,
        cc,
        bcc,
        subject,
        body,
        track = true, // Always default to tracking
        mode = "draft", // "send" | "draft" (send_draft removed)
        attachments = [], // NEW: Array of {filename, filepath, mimeType} or {filename, content, mimeType}
        signature_template = "none",  // ← NEW
        include_signature = false,             // ← NEW
        sender_template = "basic",
        create_followup_event = false,
        followup_days = 7,
        followup_time = "16:00:00"        
        
    } = args || {};

    if (mode === "send") throw new Error("Direct send temporarily disabled");

    // 🔧 FIX: Parse attachments if it comes as a JSON string
    let parsedAttachments = attachments;
    if (typeof attachments === 'string') {
        try {
            parsedAttachments = JSON.parse(attachments);
        } catch (e) {
            console.error('[send_email] Failed to parse attachments:', e);
            parsedAttachments = [];
        }
    }

    console.log('[send_email] DEBUG: received attachments =', JSON.stringify(parsedAttachments, null, 2));
    if (parsedAttachments.length > 0) {
        console.log('[send_email] DEBUG: first attachment =', parsedAttachments[0]);
        console.log('[send_email] DEBUG: filepath =', parsedAttachments[0]?.filepath);
        console.log('[send_email] DEBUG: content =', parsedAttachments[0]?.content);
    }

    // ✅ Validate send_draft is not used
    if (mode === "send_draft") {
        throw new Error(
            "send_draft mode is deprecated. Use Gmail API directly to send drafts:\n" +
            "http_fetch with url: 'https://gmail.googleapis.com/gmail/v1/users/me/drafts/send' and body: {id: 'draft_id'}"
        );
    }

    // ✅ Validate input based on mode
    if (
        ((mode === "send" && (!to || !subject || !body)) ||
            (mode === "draft" && !subject && !body))
    ) {
        throw new Error("Missing required fields: subject, body (and to if sending)");
    }

    console.log(`[send_email] Preparing email (${mode}): ${to || "(no recipient)"} (${subject})`);

    // --- Load Gmail token ---
    const tokenFile = "/opt/supabase-mcp/secrets/gmail_token.json";
    if (!fs.existsSync(tokenFile)) throw new Error("Missing Gmail token file at " + tokenFile);

    const tokenData = JSON.parse(fs.readFileSync(tokenFile, "utf8"));
    const accessToken = tokenData.access_token || tokenData.token;
    if (!accessToken) throw new Error("No access_token in Gmail token file");

    // --- Build unique tag ---
    const tag = "REQ-" + Math.random().toString(36).slice(2, 8).toUpperCase();

    // --- From Address Templates ---
    const SENDER_TEMPLATES = {
        consulting: {
            name: "Steve Elliott (Harvard University, Statistics)",
            email: process.env.SIGNATURE_EMAIL
        },
        professional: {
            name: "Steve Elliott",
            email: process.env.MCP_FROM_EMAIL_HARVARD
        },
        basic: {
            name: "Steve Elliott",
            email: process.env.MCP_FROM_EMAIL_GMAIL
        }
    };

    // --- Select from address template ---
    let fromName, fromEmail;

    const senderTemplate = (sender_template || 'professional').toLowerCase().trim();

    if (SENDER_TEMPLATES.hasOwnProperty(senderTemplate)) {
        fromName = SENDER_TEMPLATES[senderTemplate].name;
        fromEmail = SENDER_TEMPLATES[senderTemplate].email;
    } else {
        console.warn(`[send_email] Unknown from template: "${senderTemplate}", using professional`);
        fromName = SENDER_TEMPLATES.professional.name;
        fromEmail = SENDER_TEMPLATES.professional.email;
    }

    console.log(`[send_email] Using from address: ${fromName} <${fromEmail}>`);

    // --- Subject sanitization utility (prevents UTF-8/Latin-1 dash issues) ---
    function sanitizeSubject(text = "") {
        // First, do the basic sanitization
        const sanitized = text
            .normalize("NFKC")
            .replace(/[–—]/g, "-") // replace en dash / em dash
            .replace(/[""]/g, '"') // replace curly quotes
            .replace(/['']/g, "'") // replace curly apostrophes
            .replace(/\s+/g, " ")  // collapse double spaces
            .trim();

        // Then, check if it contains non-ASCII characters
        const hasNonAscii = /[^\x00-\x7F]/.test(sanitized);

        if (hasNonAscii) {
            // Encode using RFC 2047 format: =?UTF-8?B?base64?=
            const base64 = Buffer.from(sanitized, 'utf8').toString('base64');
            return `=?UTF-8?B?${base64}?=`;
        }

        return sanitized;
    }

    // --- Body sanitization utility (HTML-safe) ---
    function sanitizeBody(html = "") {
        if (!html) return "";

        // Normalize Unicode
        let sanitized = html.normalize("NFKC");

        // Replace problematic punctuation
        sanitized = sanitized
            .replace(/[–—]/g, "-")
            .replace(/[""]/g, '"')
            .replace(/['']/g, "'");

        // Optional: fix any non-breaking spaces that Gmail often misreads
        sanitized = sanitized.replace(/\u00A0/g, " ");

        return sanitized.trim();
    }

    // --- Signature Templates ---
    const SIGNATURE_TEMPLATES = {
        professional: `
<br><br>
--<br>
<b style="font-family: Georgia, serif; font-size: 11pt; line-height: 1.3;">Steve Elliott</b><br>
<i style="font-family: Arial, Helvetica, sans-serif; font-size: 10.5pt; line-height: 1.3;">Independent Statistical Consultant — Research & Evaluation</i><br>
<span style="font-family: Georgia, serif; font-size: 10.5pt; line-height: 1.3;">A.B., Harvard University — Statistics</span><br>
<span style="font-family: Arial, Helvetica, sans-serif; font-size: 10pt; line-height: 1.3;">
📧 <a href="mailto:${process.env.SIGNATURE_EMAIL}" style="color:#1a0dab; text-decoration:underline;">${process.env.SIGNATURE_EMAIL}</a><br>
🔗 <a href="${process.env.SIGNATURE_LINKEDIN}" target="_blank" style="color:#1a0dab; text-decoration:underline;">LinkedIn</a>
</span>
`,

        basic: `
<br>Best,
<br>
Steve Elliott
`,

        none: ''  // Allow signature-less emails
    };

    // --- Tracking logic ---
    const shouldTrack = track !== false; // Respect explicit false

    // ✅ Enable reply tracking by default
    const shouldMarkReplyTracked = args.reply_tracking !== false;

    // Mark as reply-to-sent only if explicitly passed
    const shouldMarkReplySent = args.reply_to_sent === true;

    // 5️⃣ Add subject/body sanitization before MIME
    const originalSubject = subject || "(no subject)"; // ← ADD THIS LINE
    const safeSubject = sanitizeSubject(subject || "(no subject)");
    const safeBody = sanitizeBody(body || "(empty message)");
    let finalBody = safeBody;
    // Ensure the sanitized body remains valid UTF-8 text
    finalBody = Buffer.from(finalBody, "utf8").toString("utf8");

    // --- Select signature template ---
    let selectedSignature = '';

    if (include_signature !== false) {
        const template = (signature_template || 'professional').toLowerCase().trim();

        if (SIGNATURE_TEMPLATES.hasOwnProperty(template)) {
            selectedSignature = SIGNATURE_TEMPLATES[template];
        } else {
            console.warn(`[send_email] Unknown signature template: "${template}", using professional`);
            selectedSignature = SIGNATURE_TEMPLATES.professional;
        }
    }

    // --- Detect or wrap HTML content ---
    let isHtml = /<\/?[a-z][\s\S]*>/i.test(finalBody);

    // If not already HTML, wrap body and convert paragraphs properly
    if (!isHtml) {
        // Split on double newlines to get paragraphs
        const paragraphs = finalBody.split(/\n\n+/);
        // Wrap each paragraph in <p> tags, converting single newlines to <br>
        const htmlParagraphs = paragraphs
            .map(p => `<p>${p.trim().replace(/\n/g, "<br>")}</p>`)
            .join('\n');
        finalBody = `<html><body>${htmlParagraphs}</body></html>`;
        isHtml = true;
    }

    const contentType = "text/html; charset=UTF-8";

    // --- Inject signature (only if not empty) ---
    if (selectedSignature) {
        if (/<\/body>/i.test(finalBody)) {
            finalBody = finalBody.replace(/<\/body>/i, `${selectedSignature}</body>`);
        } else {
            finalBody += selectedSignature;
        }
    }

    // --- Add tracking pixel if tracking is enabled ---
    if (shouldTrack) {
        const pixelUrl = `https://${process.env.SUPABASE_PROJECT_REF}.supabase.co/functions/v1/track_email_open?tag=${tag}`;
        const pixelHtml = `<img src="${pixelUrl}" width="1" height="1" style="display:none;" alt="">`;

        if (/<\/body>/i.test(finalBody)) {
            finalBody = finalBody.replace(/<\/body>/i, `${pixelHtml}</body>`);
        } else {
            finalBody += `\n\n${pixelHtml}`;
        }
    }



    // --- Thread + reply association ---
    const threadIdArg = args.thread_id || args.threadId || null;
    const inReplyTo = args.in_reply_to || args.inReplyTo || null;

    // --- Process attachments ---
    const processedAttachments = [];

    for (const att of parsedAttachments) {
        let content;

        if (att.content) {
            // Content already provided (base64)
            content = att.content;
        } else if (att.filepath) {
            // Read file and convert to base64
            if (!fs.existsSync(att.filepath)) {
                throw new Error(`Attachment file not found: ${att.filepath}`);
            }
            const fileBuffer = fs.readFileSync(att.filepath);
            content = fileBuffer.toString("base64");
        } else {
            throw new Error("Attachment must have either 'content' or 'filepath'");
        }

        processedAttachments.push({
            filename: att.filename,
            mimeType: att.mimeType || "application/octet-stream",
            content: content
        });

        console.log(`[send_email] Attached: ${att.filename} (${att.mimeType || "application/octet-stream"})`);
    }

    // --- Build MIME message ---
    const boundary = "----=_Part_" + Math.random().toString(36).slice(2, 15);
    const hasAttachments = processedAttachments.length > 0;

    // 🔧 FIX: Build headers conditionally - only add To: if provided
    const mimeLines = [
        `From: "${fromName}" <${fromEmail}>`,
        `Reply-To: "${fromName}" <${fromEmail}>`,
    ];

    // Only add To: header if recipient is provided
    if (to) {
        mimeLines.push(`To: ${to}`);
    }

    // Add CC header if provided
    if (cc) {
        mimeLines.push(`Cc: ${cc}`);
    }

    // Add BCC header if provided
    if (bcc) {
        mimeLines.push(`Bcc: ${bcc}`);
    }

    mimeLines.push(`Subject: ${safeSubject}`);

    // ✅ Always add X-Agent-Tag header if tracking is enabled (even for drafts)
    if (shouldTrack) {
        mimeLines.push(`X-Agent-Tag: ${tag}`);
    }

    if (create_followup_event) {
        mimeLines.push(`X-Followup-Enabled: true`);
        mimeLines.push(`X-Followup-Days: ${followup_days}`);
        mimeLines.push(`X-Followup-Time: ${followup_time}`);
        mimeLines.push(`X-Sender-Template: ${senderTemplate}`);
        mimeLines.push(`X-Signature-Template: ${signature_template || 'none'}`);
    }

    // Add reply headers if in reply mode
    if (threadIdArg && inReplyTo) {
        mimeLines.push(`In-Reply-To: <${inReplyTo}>`);
        mimeLines.push(`References: <${inReplyTo}>`);
    }

    if (hasAttachments) {
        // Multipart MIME with attachments - 🔧 FIXED HTML encoding
        mimeLines.push(`MIME-Version: 1.0`);
        mimeLines.push(`Content-Type: multipart/mixed; boundary="${boundary}"`);
        mimeLines.push("");
        mimeLines.push(`--${boundary}`);
        mimeLines.push(`Content-Type: ${contentType}`);
        mimeLines.push("");
        mimeLines.push(finalBody);

        // Add each attachment
        for (const att of processedAttachments) {
            mimeLines.push("");
            mimeLines.push(`--${boundary}`);
            mimeLines.push(`Content-Type: ${att.mimeType}; name="${att.filename}"`);
            mimeLines.push(`Content-Disposition: attachment; filename="${att.filename}"`);
            mimeLines.push(`Content-Transfer-Encoding: base64`);
            mimeLines.push(`Content-ID: <${Math.random().toString(36).slice(2, 12)}>`);
            mimeLines.push("");

            // Split base64 into 76-char lines (RFC 2045)
            const lines = att.content.match(/.{1,76}/g) || [];
            mimeLines.push(...lines);
        }

        mimeLines.push(`--${boundary}--`);
    } else {
        // Simple MIME without attachments
        mimeLines.push(`Content-Type: ${contentType}`);
        mimeLines.push("");
        mimeLines.push(finalBody);
    }

    const mime = mimeLines.join("\r\n");

    const raw = Buffer.from(mime)
        .toString("base64")
        .replace(/\+/g, "-")
        .replace(/\//g, "_")
        .replace(/=+$/, "");

    // --- Determine Gmail endpoint ---
    const endpoint =
        mode === "draft"
            ? "https://gmail.googleapis.com/gmail/v1/users/me/drafts"
            : "https://gmail.googleapis.com/gmail/v1/users/me/messages/send";

    // --- Build payload for Gmail API ---
    const payload =
        mode === "draft"
            ? {
                message: {
                    raw,
                    ...(threadIdArg ? { threadId: threadIdArg } : {}),
                },
            }
            : {
                raw,
                ...(threadIdArg ? { threadId: threadIdArg } : {}),
            };

    // --- Send or create draft ---
    const res = await fetch(endpoint, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${accessToken}`,
            "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
    });

    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(`Gmail API error: ${res.status} ${JSON.stringify(json)}`);

    // --- Extract IDs from response ---
    let messageId = json.id || json.message?.id || null;
    let draftId = mode === "draft" ? json.id : null;
    let threadId =
        mode === "draft"
            ? json.message?.threadId || threadIdArg || null
            : json.threadId || threadIdArg || null;
    if (!threadId && json.thread?.id) threadId = json.thread.id;

    console.log(`[send_email] ✅ ${mode === "draft" ? "Draft created" : "Sent"} (tag: ${shouldTrack ? tag : "none"}, message_id: ${messageId})`);

    // --- Tracking & logging (ONLY for direct sends, NOT drafts) ---
    async function insertWithRetry(toolName, args, retries = 2, delayMs = 800) {
        for (let attempt = 1; attempt <= retries + 1; attempt++) {
            try {
                return await callTool(toolName, args);
            } catch (err) {
                if (attempt <= retries) {
                    const backoff = delayMs * Math.pow(2, attempt - 1);
                    console.warn(`[send_email] ⚠️ ${toolName} attempt ${attempt} failed: ${err.message}. Retrying in ${backoff}ms...`);
                    await new Promise((r) => setTimeout(r, backoff));
                } else {
                    throw err;
                }
            }
        }
    }

    // ✅ ONLY log tracking for direct sends (mode === "send")
    // ✅ For drafts, the ingest cron will handle tracking when sent from Gmail UI
    if (shouldTrack && mode === "send") {
        try {
            const now = new Date().toISOString();

            console.log(`[send_email] 🧩 Logging tracking data for direct send (tag: ${tag})...`);

            await insertWithRetry("upsert_data", {
                schema: "gmail",
                table: "all_emails",
                data: {
                    message_id: messageId,
                    thread_id: threadId,
                    subject: originalSubject,
                    from_email: fromEmail,
                    to_email: to,
                    cc_email: cc || null,             
                    bcc_email: bcc || null,            
                    body_html: finalBody, // ← ADD THIS
                    body_text: safeBody.replace(/<[^>]+>/g, ''), // ← ADD THIS (strip HTML)
                    tracked_tag: tag,
                    gmail_date: now,
                    created_at: now,
                    updated_at: now,

                    // Tracking + reply logic
                    is_tracked: true,
                    tracking_active: true, // Active immediately for direct sends
                    is_reply_to_tracked: shouldMarkReplyTracked || false,
                    is_reply_to_sent: shouldMarkReplySent || false,
                    last_checked_at: null,
                    is_read: false,
                    is_starred: false,
                    is_important: false,
                    labels: ["SENT"],
                    opened_at: null,
                    message_type: "sent",
                    source: "tool_send_email",
                },
                on_conflict: "message_id",
                pk: "message_id",
                returning: "representation",
            });

            console.log(`[send_email] ✅ Tracking logged successfully (tag: ${tag}, message_id: ${messageId})`);
        } catch (err) {
            console.error(`[send_email] ⚠️ Tracking logging failed: ${err.message}`);
            console.error(`[send_email] ⚠️ Error stack:`, err.stack);
            // Don't throw - email was sent successfully, tracking can be picked up by ingest cron
        }
    } else if (shouldTrack && mode === "draft") {
        console.log(`[send_email] 📝 Draft created with tracking tag ${tag}. Ingest cron will activate tracking when sent.`);
    }

    // --- Return comprehensive metadata ---
    return asJsonContent({
        ok: true,
        mode,
        message_id: messageId,
        draft_id: draftId, // Only set for drafts
        thread_id: threadId,
        tag: shouldTrack ? tag : null,
        to,
        cc: cc || null,                          
        bcc: bcc || null,                         
        subject: originalSubject,
        from: fromEmail,
        from_name: fromName,                    
        sender_template_used: senderTemplate,       
        tracked: shouldTrack,
        tracking_active: shouldTrack && mode === "send", // Only active for direct sends
        is_reply_to_tracked: shouldMarkReplyTracked || false,
        is_reply_to_sent: shouldMarkReplySent || false,
        attachments_count: processedAttachments.length,
        signature_used: include_signature !== false ? (signature_template || 'professional') : 'none',
        followup_event_enabled: create_followup_event, 
        followup_days: create_followup_event ? followup_days : null, 
    });
}



/* ========================= PART 2/4 END =========================
   Next: PART 3/4 will implement the Advanced HTTP fetch adapter:
   - per-host token bucket, retries/backoff, redirect policy
   - host allow/deny + local/priv IP blocking
   - max_bytes clamp, streaming save, destination saving (GitHub/Supabase)
   - pagination helpers, conditional headers/auth tokens
================================================================== */
/**
 * MCP Supabase HTTP Server — Extended (PART 3/4, Completed)
 * Concatenate parts 1–4 into: /opt/supabase-mcp/runtime/index-http.js
 *
 * This part implements the Advanced HTTP Fetch adapter:
 *  - Per-host token bucket rate limiting
 *  - Retries with exponential backoff + jitter
 *  - Redirect policy (any | same_host | same_site | allow_hosts_only)
 *  - Host allow/deny lists + localhost/private-IP blocking
 *  - Response size clamping (max_bytes)
 *  - Streaming save to file
 *  - Destinations: GitHub, Supabase Storage, Supabase Table
 *  - Destination chains (sequential saves w/ templating)
 *  - Pagination via Link rel=next, page numbers, cursors
 *  - Cache/auth headers (ETag, If-Modified-Since, Authorization per host)
 *  - UA/Accept-Language pools
 *  - Trace/debug metadata
 */

/* ---------- Local imports from previous parts ---------- */
/* (We assume the following are available from PART 1):
   - supabase (client)
   - SUPABASE_URL, SUPABASE_SERVICE_ROLE, SUPABASE_STORAGE_BUCKET
   - GITHUB_TOKEN, GITHUB_REPO, GITHUB_BRANCH
   - FETCH_* env knobs
   - encodeToken, nowIso, splitFqtn, safelyParseJSON
*/

//////////////////////////////
// Host/IP safety helpers
//////////////////////////////
function isPrivateHost(host){
  const ipVer = net.isIP(host);
  if (ipVer) {
    if (host === '127.0.0.1' || host === '::1') return true;
    const parts = host.split('.').map(Number);
    if (parts.length === 4) {
      if (parts[0] === 10) return true;
      if (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) return true;
      if (parts[0] === 192 && parts[1] === 168) return true;
    }
  }
  return false;
}
async function resolveHostIPs(host){
  try { const recs = await dns.lookup(host, { all:true }); return recs.map(r=>r.address); }
  catch { return []; }
}
function sameSite(u1,u2){
  try{
    const a=new URL(u1), b=new URL(u2);
    const tailA=a.hostname.split('.').slice(-2).join('.');
    const tailB=b.hostname.split('.').slice(-2).join('.');
    return tailA===tailB;
  }catch{return false;}
}
function canRedirect(fromUrl, toUrl, policy, allowRedirectHosts=[]){
  if (policy==='any') return true;
  try{
    const a=new URL(fromUrl), b=new URL(toUrl);
    if (policy==='same_host') return a.host===b.host;
    if (policy==='same_site') return sameSite(fromUrl,toUrl);
    if (policy==='allow_hosts_only') return allowRedirectHosts.includes(b.hostname);
    return true;
  }catch{return false;}
}

//////////////////////////////
// Per-host token-bucket
//////////////////////////////
const __hostBuckets = new Map();
function getBucket(host){
  let b = __hostBuckets.get(host);
  if (!b) {
    b = { tokens: FETCH_PER_HOST_CAPACITY, last: Date.now(), cap: FETCH_PER_HOST_CAPACITY, refill: FETCH_PER_HOST_REFILL };
    __hostBuckets.set(host, b);
  }
  return b;
}
function bucketTake(host, maxWaitMs){
  const b = getBucket(host);
  const now = Date.now();
  const elapsedSec = (now - b.last) / 1000;
  b.tokens = Math.min(b.cap, b.tokens + elapsedSec * b.refill);
  b.last = now;
  if (b.tokens >= 1) { b.tokens -= 1; return 0; }
  const needMs = ((1 - b.tokens) / b.refill) * 1000;
  if (needMs > maxWaitMs) return -1;
  return needMs;
}

//////////////////////////////
// Redirect policy helpers
//////////////////////////////
function normalizeHostsList(v){ if (Array.isArray(v)) return v; if (!v) return []; return [v]; }

//////////////////////////////
// Simple in-memory HTTP cache
//////////////////////////////
const __fetchCache = new Map(); // url -> { etag, last_modified }

//////////////////////////////
// Destinations (GitHub / Supabase)
//////////////////////////////
// ============================================================
// Destinations helper (Supabase Storage, Supabase Table, GitHub)
// ============================================================
async function saveToDestination(dest, bytes, opts = {}) {
    // ---- entry log (we always print one line when called)
    console.error('[storage ENTRY]', {
        dest,
        hasBytes: !!bytes,
        byteType: Object.prototype.toString.call(bytes),
        opts
    });

    try {
        // No destination specified
        if (!dest || dest === 'none') {
            console.error('[storage BYPASS] no destination provided');
            return { saved: false };
        }

        // --------------------------------------------------------
        // Destination: Supabase Storage
        // --------------------------------------------------------
        if (dest === 'supabase_storage') {
            // bucket may come from opts.bucket OR env
            const bucket =
                opts.bucket ||
                (typeof SUPABASE_STORAGE_BUCKET !== 'undefined' ? SUPABASE_STORAGE_BUCKET : undefined);

            // NOTE: key (i.e. “path” inside bucket) defaults to a dated file name
            const key =
                opts.path ||
                `uploads/${Date.now()}-${Math.random().toString(36).slice(2)}.png`;

            if (!bucket) {
                console.error('[storage ERROR] missing bucket', {
                    envBucket: typeof SUPABASE_STORAGE_BUCKET !== 'undefined' ? SUPABASE_STORAGE_BUCKET : null
                });
                throw new Error('supabase_storage destination requires bucket');
            }

            console.error('[storage DEBUG] destination handler reached', { bucket, key });

            // Prefer service-role client for uploads (bypasses RLS)
            const sb = SUPABASE_SERVICE_ROLE
                ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
                : supabase;

            const buf = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes || []);
            const bytesLength = buf.length || 0;
            console.error('[storage DEBUG] uploading to Supabase', {
                bucket,
                key,
                bytesLength
            });

            const { data, error } = await sb
                .storage
                .from(bucket)
                .upload(key, buf, {
                    upsert: true,
                    contentType: opts.contentType || 'image/png'
                });

            if (error) {
                console.error('[storage ERROR]', {
                    message: error.message,
                    name: error.name,
                    status: error.status,
                    statusText: error.statusText
                });
                throw error;
            }

            // Default public-style object URL; optionally return a signed URL
            let url = `${SUPABASE_URL}/storage/v1/object/${bucket}/${key}`;

            if (opts.signedSeconds) {
                const { data: signed, error: signErr } = await sb
                    .storage
                    .from(bucket)
                    .createSignedUrl(key, Number(opts.signedSeconds));

                if (!signErr && signed?.signedUrl) {
                    url = signed.signedUrl;
                    console.error('[storage DEBUG] signed URL generated');
                } else if (signErr) {
                    console.error('[storage WARN] signed URL failed', { signErr: signErr.message || signErr });
                }
            }

            console.error('[storage SUCCESS]', { bucket, key, url });
            return { saved: true, bucket, key, url };
        }

        // --------------------------------------------------------
        // Destination: Supabase Table (store bytes into a row)
        //   opts.table:  'schema.table'  or just 'table' (defaults to 'public')
        //   opts.row:    additional columns to store
        // --------------------------------------------------------
        if (dest === 'supabase_table') {
            const fqtn = opts.table;
            if (!fqtn) throw new Error('supabase_table destination requires table (schema.table)');

            const [schema, table] = fqtn.includes('.')
                ? fqtn.split('.', 2)
                : ['public', fqtn];

            const sb = SUPABASE_SERVICE_ROLE
                ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
                : supabase;

            const row = {
                ...(opts.row || {}),
                data: Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes || []),
                created_at: new Date().toISOString()
            };

            console.error('[storage DEBUG] table insert', { schema, table, rowKeys: Object.keys(row) });

            const { data, error } = await sb
                .schema(schema)
                .from(table)
                .insert(row)
                .select();

            if (error) {
                console.error('[storage ERROR] table insert', {
                    message: error.message,
                    name: error.name,
                    status: error.status
                });
                throw error;
            }

            console.error('[storage SUCCESS] table insert', { rows: Array.isArray(data) ? data.length : 0 });
            return { saved: true, rows: data };
        }

        // --------------------------------------------------------
        // Destination: GitHub (disabled here by default)
        //   If you want to enable, make sure GITHUB_TOKEN & GITHUB_REPO are set,
        //   then implement your upload call below.
        // --------------------------------------------------------
        if (dest === 'github') {
            console.error('[storage INFO] GitHub destination reached');
            if (!GITHUB_TOKEN || !GITHUB_REPO) {
                throw new Error('GitHub destination requires GITHUB_TOKEN and GITHUB_REPO');
            }
            const path = opts.path || `uploads/${Date.now()}-${Math.random().toString(36).slice(2)}.bin`;
            const message = opts.message || 'upload artifact';
            const content = Buffer.isBuffer(bytes)
                ? bytes.toString('base64')
                : Buffer.from(bytes || []).toString('base64');

            const [owner, repo] = GITHUB_REPO.split('/');
            const res = await fetch(`https://api.github.com/repos/${owner}/${repo}/contents/${encodeURIComponent(path)}`, {
                method: 'PUT',
                headers: {
                    'Authorization': `Bearer ${GITHUB_TOKEN}`,
                    'Content-Type': 'application/json',
                    'Accept': 'application/vnd.github+json'
                },
                body: JSON.stringify({ message, content, branch: GITHUB_BRANCH || 'main' })
            });

            const j = await res.json().catch(() => ({}));
            if (!res.ok) {
                console.error('[storage ERROR] github upload failed', { status: res.status, body: j });
                throw new Error(`github upload failed: ${res.status}`);
            }
            console.error('[storage SUCCESS] github upload', { path: j?.content?.path, html: j?.content?.html_url });
            return { saved: true, path: j?.content?.path, html_url: j?.content?.html_url };
        }

        // --------------------------------------------------------
        // Unknown destination
        // --------------------------------------------------------
        console.error('[storage ERROR] unknown destination', { dest });
        return { saved: false, error: `Unknown destination: ${dest}` };

    } catch (err) {
        // one last safety-net log
        console.error('[storage FATAL]', err?.message || err);
        return { saved: false, error: err?.message || String(err) };
    }
}

//////////////////////////////
// Streaming save to file
//////////////////////////////
async function streamToFile(stream, filepath, maxBytes){
  await fsp.mkdir(path.dirname(filepath), { recursive:true });
  const tmp = filepath + '.part-' + Date.now();
  const out = fs.createWriteStream(tmp, { flags:'w', mode:0o600 });
  let written = 0;
  await new Promise((resolve, reject)=>{
    stream.on('data', chunk=>{
      written += chunk.length;
      if (maxBytes && written > maxBytes){
        out.destroy(); try{ fs.unlinkSync(tmp); }catch{}; return reject(new Error(`max_bytes exceeded (${written})`));
      }
      out.write(chunk);
    });
    stream.on('end', ()=>{ out.end(); resolve(); });
    stream.on('error', e=>{ out.destroy(); try{ fs.unlinkSync(tmp); }catch{} reject(e); });
  });
  await fsp.rename(tmp, filepath);
  return { path: filepath, bytes: written };
}

//////////////////////////////
// Helpers for pagination & JSON parsing
//////////////////////////////
function parseLinkNext(linkHeader){ if (!linkHeader) return null;
  const parts=linkHeader.split(',').map(s=>s.trim());
  for (const p of parts){ const m=p.match(/<([^>]+)>;\s*rel="([^"]+)"/i); if(m && m[2]==='next') return m[1]; }
  return null;
}
function tryParseJSON(buf){ try{ return JSON.parse(buf.toString('utf-8')); }catch{return null;} }

//////////////////////////////
// Host allow/deny helper
//////////////////////////////
function hostAllowed(host, allowHosts, denyHosts){ if (allowHosts.length && !allowHosts.includes(host)) return false; if (denyHosts.length && denyHosts.includes(host)) return false; return true; }

//////////////////////////////
// HTTP Fetch tool
//////////////////////////////
async function tool_http_fetch(args){
  args = await maybeInjectGmailToken(args);
  args = await maybeInjectGithubToken(args);
  args = await maybeInjectAviasalesToken(args);

  const started = Date.now();
  const {
    url, method='GET', headers={}, body, timeout_ms=15000, response_type='text',
    allow_hosts, deny_hosts, redirect_policy=FETCH_REDIRECT_POLICY, allow_redirect_hosts,
    max_bytes=FETCH_MAX_BYTES_DEFAULT, save_to, paginate=false,
    destination='none', destination_opts={}, destination_chain=[],
    ua_pool=[], lang_pool=[], trace=false
  } = args || {};

  if (!url) throw new Error('Missing url');

  // Parse URL and safety checks
  const u = new URL(url);
  const allowHosts = normalizeHostsList(allow_hosts?.length ? allow_hosts : FETCH_ALLOW_HOSTS);
  const denyHosts  = normalizeHostsList(deny_hosts?.length  ? deny_hosts  : FETCH_DENY_HOSTS );
  if (!hostAllowed(u.hostname, allowHosts, denyHosts)) throw new Error(`host not allowed: ${u.hostname}`);

  if (BROWSER_DENY_LOCALHOST) {
    if (u.hostname==='localhost' || isPrivateHost(u.hostname)) throw new Error('localhost/private IPs are blocked');
    const resolved = await resolveHostIPs(u.hostname);
    if (resolved.some(isPrivateHost)) throw new Error('resolved to private IP — blocked');
  }

  // Apply per-host token bucket
  const wait = bucketTake(u.hostname, FETCH_PER_HOST_MAX_WAIT);
  if (wait < 0) throw new Error('rate limited (bucket)');
  if (wait > 0) await new Promise(r=>setTimeout(r, wait));

  // Build request headers (handle headers arriving as a JSON string from some MCP clients)
  const parsedHeaders = typeof headers === 'string' ? (() => { try { return JSON.parse(headers); } catch { return {}; } })() : (headers || {});
  const h = { ...parsedHeaders };
  if (ua_pool.length) h['User-Agent'] = ua_pool[Math.floor(Math.random()*ua_pool.length)];
  if (lang_pool.length) h['Accept-Language'] = lang_pool[Math.floor(Math.random()*lang_pool.length)];
  if (FETCH_CACHE_ENABLED){
    const c = __fetchCache.get(url);
    if (c?.etag) h['If-None-Match'] = c.etag;
    if (c?.last_modified) h['If-Modified-Since'] = c.last_modified;
  }
  const token = FETCH_HOST_TOKENS[u.hostname];
  if (token && !h['Authorization']) h['Authorization'] = token;

  let reqBody;
  if (body !== undefined) {
    reqBody = (typeof body === 'string') ? body : JSON.stringify(body);
    if (!h['Content-Type']) h['Content-Type'] = 'application/json';
  }

  // State
  let currentUrl = url;
  let redirects = 0;
  let lastResMeta = null;
  let aggregatePages = [];
  let totalBytes = 0;
  let savedFile = null;

  const controller = new AbortController();
  const softTimeout = setTimeout(()=>controller.abort(), timeout_ms);

  try {
    while (true) {
      const res = await fetch(currentUrl, { method, headers:h, body:reqBody, redirect:'manual', signal: controller.signal });
      const meta = { ok: res.ok, status: res.status, url: currentUrl, headers: Object.fromEntries(res.headers) };
      lastResMeta = meta;

      if (trace) console.error('[http_fetch trace]', meta);

      // 3xx redirects
      if (res.status >= 300 && res.status < 400) {
        const loc = res.headers.get('location');
        if (!loc) break;
        const toUrl = new URL(loc, currentUrl).toString();
        const policyList = normalizeHostsList(allow_redirect_hosts);
        if (!canRedirect(currentUrl, toUrl, redirect_policy, policyList)) throw new Error(`redirect blocked: ${toUrl}`);
        currentUrl = toUrl; redirects++; if (redirects > 10) throw new Error('too many redirects'); continue;
      }

      // 304 cache hit
      if (res.status === 304 && FETCH_CACHE_ENABLED) {
        return asJsonContent({ meta, data:null, cached:true });
      }

      // Retries
      let attempt=0, finalRes=res;
      while (!finalRes.ok && FETCH_RETRY_STATUS.includes(finalRes.status) && attempt < FETCH_RETRY_MAX) {
        const jitter=Math.floor(Math.random()*FETCH_BACKOFF_JITTER_MS);
        await new Promise(r=>setTimeout(r, FETCH_BACKOFF_BASE_MS * Math.pow(2, attempt)+jitter));
        attempt++;
        const again = await fetch(currentUrl, { method, headers:h, body:reqBody, redirect:'manual', signal: controller.signal });
        finalRes = again;
        meta.ok=finalRes.ok; meta.status=finalRes.status; meta.headers=Object.fromEntries(finalRes.headers);
      }
        if (!finalRes.ok) {
            // ✅ Capture Duffel or other API error body for debugging
            let errorBody = null;
            try {
                errorBody = await finalRes.text();
                console.error("[http_fetch] ❌ Non-OK response:", finalRes.status, errorBody);
            } catch (err) {
                console.error("[http_fetch] ❌ Failed to read error body:", err.message);
            }

            // Return structured info instead of throwing
            return asJsonContent({
                meta,
                data: errorBody || null,
                ok: false,
                status: finalRes.status,
                headers: meta.headers,
            });
        }


      if (FETCH_CACHE_ENABLED) {
        const et = finalRes.headers.get('etag'); const lm = finalRes.headers.get('last-modified');
        if (et || lm) __fetchCache.set(currentUrl, { etag:et, last_modified:lm });
      }

        // ✅ Read body with Node-compatible fallback
        let bytes;
        {
            const maxB = max_bytes || FETCH_MAX_BYTES_DEFAULT;
            const chunks = [];
            let readBytes = 0;

            try {
                // If running in Node (ReadableStream or polyfilled fetch)
                if (typeof finalRes.text === "function") {
                    const text = await finalRes.text();
                    bytes = Buffer.from(text, "utf-8");
                    readBytes = bytes.length;
                }
                // If running in environments where body is an async iterable (Node fetch)
                else if (finalRes.body && typeof finalRes.body[Symbol.asyncIterator] === "function") {
                    for await (const chunk of finalRes.body) {
                        readBytes += chunk.length;
                        if (maxB && readBytes > maxB)
                            throw new Error(`max_bytes exceeded (${readBytes})`);
                        chunks.push(chunk);
                    }
                    bytes = Buffer.concat(chunks);
                }
                // If running in Deno / browser-like environment
                else if (finalRes.body && typeof finalRes.body.getReader === "function") {
                    const reader = finalRes.body.getReader();
                    while (true) {
                        const { done, value } = await reader.read();
                        if (done) break;
                        readBytes += value.byteLength;
                        if (maxB && readBytes > maxB)
                            throw new Error(`max_bytes exceeded (${readBytes})`);
                        chunks.push(value);
                    }
                    bytes = Buffer.concat(chunks.map((c) => Buffer.from(c)));
                } else {
                    throw new Error("Unsupported response body type");
                }
            } catch (err) {
                throw new Error(`Failed to read response body: ${err.message}`);
            }

            totalBytes += readBytes;
        }


      // Pagination
      if (paginate) {
        aggregatePages.push({ meta, bytesLen: bytes.length });
        const next = parseLinkNext(finalRes.headers.get('link'));
        if (next && canRedirect(currentUrl, next, redirect_policy, normalizeHostsList(allow_redirect_hosts))) {
          currentUrl=next; redirects++; if (redirects > 20) break; continue;
        }
      }

      if (save_to) {
        const destPath = path.resolve(save_to.replace(/^~\//, os.homedir() + '/'));
        await fsp.mkdir(path.dirname(destPath), { recursive:true });
        const tmp=destPath+'.part-'+Date.now(); await fsp.writeFile(tmp, bytes, {mode:0o600}); await fsp.rename(tmp, destPath);
        savedFile=destPath;
      }

      let destResult = null;
      if (destination && destination!=='none') {
        destResult=await saveToDestination(destination, bytes, { ...destination_opts, contentType: finalRes.headers.get('content-type')||'application/octet-stream' });
      }

      // Destination chain
      let chainResults = [];
      if (Array.isArray(destination_chain) && destination_chain.length) {
        let prevCtx={};
        for (const step of destination_chain) {
          const templated=step.path?step.path.replace(/\{(\w+)\}/g,(m,k)=>prevCtx[k]||m):undefined;
          const resChain=await saveToDestination(step.type, bytes, { ...step, path:templated });
          chainResults.push(resChain); prevCtx={...prevCtx, ...resChain};
        }
      }

      // Prepare response
      let dataOut;
      if (response_type==='json') dataOut=tryParseJSON(bytes);
      else if (response_type==='bytes') dataOut=bytes.toString('base64');
      else dataOut=bytes.toString('utf-8');

      const out={ meta, data:dataOut, savedFile, destination:destResult, destination_chain:chainResults, totalBytes, trace:trace?meta:undefined };
      return asJsonContent(out);
    }
  } finally { clearTimeout(softTimeout); }
}

/* ========================= PART 3/4 END =========================
   Next: PART 4/4 will implement:
   - notify_push tool
   - Playwright browser_flow (multi-step) with safety & context
   - SSE endpoints and JSON-RPC dispatch
================================================================== */
/**
 * MCP Supabase HTTP Server — Extended (PART 4/4, Completed)
 * Concatenate parts 1–4 into: /opt/supabase-mcp/runtime/index-http.js
 *
 * This part implements:
 *  - notify_push tool (Slack, Pushover, generic webhook + destinations)
 *  - Playwright browser_flow (multi-step scripted automation with stealth, context, artifacts, jitter)
 *  - /health and SSE endpoints
 *  - JSON-RPC dispatch
 */

//////////////////////////////
// notify_push
//////////////////////////////
async function tool_notify_push(args){
  const { provider='slack.webhook', webhook_url, channel, title, body, message, level='info',
          meta={}, data, destination='none', destination_opts={} } = args||{};

  let url;
  let payload;
  let headers = {};

  if (provider === 'slack.webhook' || provider === 'webhook') {
    url = webhook_url || process.env.NOTIFY_WEBHOOK_URL;
    if (!url) throw new Error('Missing webhook_url and NOTIFY_WEBHOOK_URL not set. Did you mean to use provider: "pushover"? Required fields: provider, category, title, body.');
    payload = {
      text: `${title||''}\n${body||message||''}` +
            (meta && Object.keys(meta).length ? `\n\`\`\`meta=${JSON.stringify(meta)}\`\`\`` : '')
    };
    headers['Content-Type'] = 'application/json';
  } else if (provider === 'pushover') {
      const userKey = args.user_key || process.env.PUSHOVER_USER_KEY;
      const apiToken = args.api_token || (
          args.category === 'gmail' ? process.env.PUSHOVER_TOKEN_GMAIL :
              args.category === 'calendar_events' ? process.env.PUSHOVER_TOKEN_CALENDAR_EVENTS :
                  args.category === 'calendar_recurring' ? process.env.PUSHOVER_TOKEN_CALENDAR_RECURRING :
                      args.category === 'purchases' ? process.env.PUSHOVER_TOKEN_PURCHASES :
                          // TODAY
                          args.category === 'today_quick' ? process.env.PUSHOVER_TOKEN_TODAY_QUICK :
                              args.category === 'today_medium' ? process.env.PUSHOVER_TOKEN_TODAY_MEDIUM :
                                  args.category === 'today_deep' ? process.env.PUSHOVER_TOKEN_TODAY_DEEP :

                                      // NON-TODAY
                                      args.category === 'quick' ? process.env.PUSHOVER_TOKEN_QUICK :
                                          args.category === 'medium' ? process.env.PUSHOVER_TOKEN_MEDIUM :
                                              args.category === 'deep' ? process.env.PUSHOVER_TOKEN_DEEP :
                                                  args.category === 'projects' ? process.env.PUSHOVER_TOKEN_PROJECTS :
                      process.env.PUSHOVER_API_TOKEN
      );
      if (!userKey || !apiToken) throw new Error('notify_push: missing Pushover user_key or api_token');
      url = "https://api.pushover.net/1/messages.json";
      payload = { token: apiToken, user: userKey, message: body || message || '', title, priority: args.priority || 0 };
      headers['Content-Type'] = 'application/x-www-form-urlencoded';
  } else {
    url = webhook_url || process.env.NOTIFY_WEBHOOK_URL;
    if (!url) throw new Error('Missing webhook_url and NOTIFY_WEBHOOK_URL not set');
    payload = { channel, title, body: body||message, data, level, ts: Date.now() };
    headers['Content-Type'] = 'application/json';
  }

  const res = await fetch(url, {
    method:'POST',
    headers,
    body: headers['Content-Type'] === 'application/x-www-form-urlencoded'
            ? new URLSearchParams(payload).toString()
            : JSON.stringify(payload)
  });

  const text = await res.text().catch(()=>'');

  // Auto-log to calendar.notifications
  if (!args.no_log) try {
    const sb = (typeof SUPABASE_SERVICE_ROLE !== 'undefined' && SUPABASE_SERVICE_ROLE)
      ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
      : supabase;
    await sb.schema('calendar').from('notifications').insert({
      title: title || null,
      description: body || message || null,
      category: args.category || null,
    });
  } catch(e) {
    console.error('[notify_push] failed to log to calendar.notifications:', e?.message || e);
  }

  let destResult=null;
  if (destination && destination!=='none') {
    destResult = await saveToDestination(destination, Buffer.from(text,'utf-8'), { ...destination_opts, contentType:'text/plain' });
  }

  return asJsonContent({ ok:res.ok, status:res.status, body:text, provider, destination:destResult });
}

//////////////////////////////
// Playwright browser_flow
//////////////////////////////
async function loadPlaywright(){
  try{ return await import('playwright'); }
  catch { throw new Error('playwright is not installed (npm i playwright)'); }
}
function denyLocal(urlStr){
  const u=new URL(urlStr);
  if (u.hostname==='localhost' || isPrivateHost(u.hostname)) {
    throw new Error('Blocked localhost/private host');
  }
}

async function tool_browser_flow(args) {
    const { steps = [], options = {} } = args || {};
    if (!Array.isArray(steps) || !steps.length) throw new Error('steps[] is required');

    const pw = await loadPlaywright();
    const launchOpts = { headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] };
    const chromePath = resolveChromiumPath();

    console.error('[browser_flow] resolved Chrome path:', chromePath || '(none)');

    const browser = await pw.chromium.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
        executablePath: chromePath || undefined
    });

    console.error('[browser_flow] Chromium launched ✅');



    // context options
    const contextOpts = {};
    if (options.proxy) contextOpts.proxy = options.proxy;
    if (options.geolocation) contextOpts.geolocation = options.geolocation;
    if (options.timezone) contextOpts.timezoneId = options.timezone;
    if (options.locale) contextOpts.locale = options.locale;
    if (options.user_agent) contextOpts.userAgent = options.user_agent;
    if (options.viewport) contextOpts.viewport = options.viewport;
    if (options.storage_state_in) contextOpts.storageState = options.storage_state_in;

    const context = await browser.newContext(contextOpts);

    // stealth mode
    if (options.stealth) {
        await context.addInitScript(() => {
            Object.defineProperty(navigator, 'webdriver', { get: () => false });
            window.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        });
    }

    const page = await context.newPage();
    const jitter = Number(options.jitter_ms || 0);
    const results = [];

    // --- Node.js dynamic multi-page screenshot helper ---
    async function dynamicPageLoop({ waitSelector, screenshotOpts, safetyLimit = 50 }) {
        let pageNum = 1;

        while (true) {
            // Scroll to bottom until all elements are loaded
            await page.evaluate(async () => {
                let previousHeight;
                while (true) {
                    const currentHeight = document.body.scrollHeight;
                    if (previousHeight === currentHeight) break;
                    previousHeight = currentHeight;
                    window.scrollTo(0, document.body.scrollHeight);
                    await new Promise(r => setTimeout(r, 2000));
                }
            });

            // Wait for job cards to appear
            if (waitSelector) await page.waitForSelector(waitSelector, { timeout: 10000 }).catch(() => { });

            // Take screenshot and upload to Supabase
            if (screenshotOpts) {
                const buf = await page.screenshot({ fullPage: !!screenshotOpts.full_page });
                console.log('Uploading page', pageNum, 'to Supabase...');
                await saveToDestination(
                    { type: 'supabase_storage' },
                    buf,
                    {
                        bucket: screenshotOpts.destination_opts?.bucket || 'screenshots',
                        path: screenshotOpts.destination_opts?.path?.replace('${pageNum}', pageNum) || `uploads/page_${pageNum}.png`,
                        contentType: 'image/png'
                    }
                );
            }

            // Check for Next button
            const nextBtn = await page.$('button[aria-label*="Next"]:not([disabled]), a[aria-label*="Next"]:not([aria-disabled="true"])');
            if (!nextBtn) break;

            await nextBtn.evaluate(btn => btn.scrollIntoView());
            await page.waitForTimeout(1000);
            await nextBtn.click();
            await page.waitForTimeout(5000);

            pageNum++;
            if (pageNum > safetyLimit) break;
        }
    }


    try {
        for (const step of steps) {
            const op = (step.op || '').toLowerCase();

            if (op === 'goto') {
                if (!step.url) throw new Error('goto requires url');
                if (BROWSER_DENY_LOCALHOST) denyLocal(step.url);
                await page.goto(step.url, { waitUntil: step.wait_until || 'load', timeout: step.timeout_ms || 20000 });
                results.push({ op: 'goto', url: step.url, title: await page.title() });

            } else if (op === 'click') {
                if (!step.selector) throw new Error('click requires selector');
                if (jitter) await new Promise(r => setTimeout(r, Math.random() * jitter));
                await page.click(step.selector, { timeout: step.timeout_ms || 15000 });
                results.push({ op: 'click', selector: step.selector });

            } else if (op === 'fill') {
                if (!step.selector) throw new Error('fill requires selector');
                if (jitter) await new Promise(r => setTimeout(r, Math.random() * jitter));
                await page.fill(step.selector, String(step.value ?? ''));
                results.push({ op: 'fill', selector: step.selector });

            } else if (op === 'type') {
                if (!step.selector) throw new Error('type requires selector');
                if (jitter) await new Promise(r => setTimeout(r, Math.random() * jitter));
                await page.type(step.selector, String(step.text ?? ''), { delay: step.delay_ms || 20 });
                results.push({ op: 'type', selector: step.selector });

            } else if (op === 'wait_for') {
                if (step.selector) await page.waitForSelector(step.selector, { timeout: step.timeout_ms || 15000 });
                if (step.ms) await new Promise(r => setTimeout(r, step.ms));
                results.push({ op: 'wait_for' });

            } else if (op === 'screenshot') {
                const wantInline =
                    (step.return && String(step.return).toLowerCase() === 'data') ||
                    (step.output && String(step.output).toLowerCase() === 'inline');

                const shotOpts = { fullPage: !!step.full_page };

                if (wantInline) {
                    const buf = await page.screenshot(shotOpts);
                    const base64 = buf.toString('base64');
                    let dest = null;
                    if (step.destination) {
                        dest = await saveToDestination(step.destination, buf, {
                            ...step.destination_opts,
                            contentType: 'image/png',
                            path: step.destination?.path,
                        });
                    }

                    results.push({
                        type: 'bytes',
                        bytes: base64,
                        encoding: 'base64',
                        mime: 'image/png',
                        meta: {
                            kind: 'screenshot',
                            full_page: !!step.full_page,
                            destination: dest,
                        }
                    });
                } else {
                    const file = path.join(os.tmpdir(), `shot-${Date.now()}.png`);
                    await page.screenshot({ ...shotOpts, path: file });

                    let dest = null;
                    if (step.destination) {
                        const bytes = fs.readFileSync(file);
                        dest = await saveToDestination(step.destination, bytes, {
                            ...step.destination_opts,
                            contentType: 'image/png',
                            path: step.destination?.path,
                        });
                    }

                    results.push({
                        type: 'screenshot',
                        path: file,
                        full_page: !!step.full_page,
                        destination: dest
                    });
                }

            } else if (op === 'pdf') {
                const file = step.path || path.join(os.tmpdir(), `page-${Date.now()}.pdf`);
                await page.pdf({ path: file, format: step.format || 'A4', printBackground: true });
                let dest = null;
                if (step.destination) dest = await saveToDestination(step.destination, fs.readFileSync(file), { ...step.destination_opts, contentType: 'application/pdf' });
                results.push({ op: 'pdf', path: file, destination: dest });

            } else if (op === 'evaluate') {

                // --- GENERAL DYNAMIC EXTRACTION ---
                if (step.dynamicExtract) {
                    // Determine target: iframe or top-level page
                    let target = page;
                    if (step.frameUrlPattern) {
                        target = page.frames().find(f => f.url().match(new RegExp(step.frameUrlPattern)));
                        if (!target) throw new Error(`Frame matching ${step.frameUrlPattern} not found`);
                    }

                    // Optional wait before extraction
                    if (step.wait_ms) await new Promise(r => setTimeout(r, step.wait_ms));

                    // Polling function for dynamic elements
                    async function pollElements(frame, selector, timeout = 30000) {
                        const start = Date.now();
                        while (Date.now() - start < timeout) {
                            const els = await frame.$$(selector);
                            if (els.length > 0) return els;
                            await new Promise(r => setTimeout(r, 1000));
                        }
                        return [];
                    }

                    // Wait/poll for target elements
                    const elements = await pollElements(target, step.selector, step.timeout_ms || 30000);

                    // Extract data
                    const items = await target.evaluate((els, filterRegex) => {
                        return els.map(el => ({
                            title: el.innerText,
                            link: el.href || null,
                            extra: el.dataset || {}
                        })).filter(item => !filterRegex || filterRegex.test(item.title));
                    }, elements, step.textFilter ? new RegExp(step.textFilter, 'i') : null);

                    results.push({ op: 'dynamicExtract', items });
                    if (step.saveAs) saveVar(chainId, step.saveAs, items);
                    continue; // skip normal evaluate
                }

                // Normal evaluate
                const fnBody = String(step.script || 'return null;');
                const result = await page.evaluate(new Function(fnBody));
                results.push({ op: 'evaluate', result });

                // --- Node.js multi-page screenshot loop ---
                await dynamicPageLoop({
                    waitSelector: '.wd-card',
                    screenshotOpts: {
                        full_page: true,
                        destination_opts: {
                            bucket: 'screenshots',
                            path: `uploads/wustl_assistant_jobs_page_${pageNum}.png`
                        }
                    },
                    safetyLimit: 50
                });

            } else if (op === 'extract') {
                const sel = step.selector;
                const kind = (step.kind || 'text').toLowerCase();
                let data = null;
                if (kind === 'text') data = await page.$eval(sel, el => el.innerText);
                else if (kind === 'html') data = await page.$eval(sel, el => el.innerHTML);
                results.push({ op: 'extract', selector: sel, kind, data });

            } else if (op === 'set_files') {
                const sel = step.selector;
                const files = Array.isArray(step.files) ? step.files : [step.files];
                await page.setInputFiles(sel, files.map(f => ({
                    name: path.basename(f),
                    mimeType: 'application/octet-stream',
                    buffer: fs.readFileSync(f)
                })));
                results.push({ op: 'set_files', selector: sel, count: files.length });

            } else {
                results.push({ op, skipped: true });
            }

        }

        if (options.storage_state_out) {
            await context.storageState({ path: options.storage_state_out });
            results.push({ op: 'save_storage_state', path: options.storage_state_out });
        }

    } finally {
        await browser.close();
    }

    return asJsonContent({ ok: true, results });
}

//////////////////////////////
// Health
//////////////////////////////
app.get('/gpt/health', (_req, res) => {
    res.json({ ok: true, server: 'Supabase MCP', ts: Date.now() });
});

// ==========================================================
// Safe Editing Orchestration Endpoint
// ==========================================================

/**
 * POST /edit/perform_safe
 * Body:
 *  {
 *    "relpath": "index-http.js",
 *    "anchor": "async function callOneToolByName(",
 *    "new_lines": [ "..." ],
 *    "commit_message": "Add new branch"
 *  }
 *
 * This endpoint automates:
 *   find_anchor → get_function_definition → dry_run → edit → check → commit/rollback
 */
app.post("/edit/perform_safe", async (req, res) => {
    try {
        const { relpath, anchor, new_lines, commit_message } = req.body || {};
        if (!relpath || !anchor || !Array.isArray(new_lines)) {
            return res.status(400).json({ error: "Missing relpath, anchor, or new_lines[]" });
        }

        console.log(`[SAFE EDIT] Request for ${relpath} @ ${anchor}`);

        // Step 1️⃣ Locate anchor
        const anchorRes = await tool_find_anchor({ relpath, anchor, context_lines: 10 });
        const { start, end } = anchorRes?.[0]?.json || {};
        if (typeof start !== "number" || typeof end !== "number") {
            throw new Error(`Anchor not found: ${anchor}`);
        }

        // Step 2️⃣ Fetch full definition
        const defRes = await tool_get_function_definition({ relpath, anchor });
        const { startLine, endLine, snippet } = defRes?.[0]?.json || {};
        if (!snippet) throw new Error("Could not fetch function definition");

        // Step 3️⃣ Dry-run preview
        const dry = await tool_dry_run_edit({
            relpath,
            start_line: startLine,
            end_line: endLine,
            new_lines,
        });
        const diff = dry?.[0]?.json?.diff;
        console.log("[SAFE EDIT] Proposed diff:\n" + diff);

        // Optional: Require manual approval via ENV
        if (process.env.EDIT_REQUIRE_APPROVAL === "1") {
            return res.json({
                ok: true,
                status: "pending_approval",
                diff,
                message: "Set EDIT_REQUIRE_APPROVAL=0 to apply automatically",
            });
        }

        // Step 4️⃣ Apply edit
        const editRes = await tool_edit_slice({
            relpath,
            start_line: startLine,
            end_line: endLine,
            new_lines,
        });
        const backup = editRes?.[0]?.json?.backup;
        console.log(`[SAFE EDIT] Written backup: ${backup}`);

        // Step 5️⃣ Validate syntax
        const check = await tool_run_check({ relpath });
        if (!check?.[0]?.json?.ok) {
            console.error("[SAFE EDIT] Syntax check failed:", check[0].json.stderr);
            await tool_revert_file({ relpath, backup });
            return res.status(400).json({
                ok: false,
                error: "Syntax validation failed. Rolled back to backup.",
                stderr: check[0].json.stderr,
            });
        }

        // Step 6️⃣ Commit
        const commit = await tool_commit_file({
            relpath,
            message: commit_message || `Automated edit for ${anchor}`,
        });

        return res.json({
            ok: true,
            diff,
            backup,
            validation: check[0].json,
            commit: commit[0].json,
        });
    } catch (err) {
        console.error("[SAFE EDIT ERROR]", err);
        res.status(500).json({ ok: false, error: err.message });
    }
});



/* ========================= Shortcuts: Steps ingest (update-then-insert) =========================
   POST /shortcuts/steps or /health/steps
   Headers:
     Authorization: Bearer <HEALTHKIT_INGEST_TOKEN>
     Content-Type: application/json
   Body:
     { "date": "YYYY-MM-DD", "steps": <int> }
================================================================================================= */


app.post(['/health/steps', '/shortcuts/steps'], async (req, res) => {
    try {
        // --- Auth check (Bearer) ---
        const auth = (req.get('authorization') || '').trim();
        const token = auth.replace(/^Bearer\s+/i, '');

        // Only enforce token if HEALTHKIT_INGEST_TOKEN is set
        if (HEALTHKIT_INGEST_TOKEN) {
            if (!token || token !== HEALTHKIT_INGEST_TOKEN) {
                return res.status(401).json({ error: 'Invalid token' });
            }
        }

        // --- Validate body ---
        const { date, steps } = req.body || {};
        if (!date || typeof date !== 'string') {
            return res.status(400).json({ error: "Missing/invalid 'date' (YYYY-MM-DD)" });
        }
        if (Number.isNaN(Number(steps))) {
            return res.status(400).json({ error: "Missing/invalid 'steps' (number)" });
        }

        // --- Supabase client ---
        const sb = SUPABASE_SERVICE_ROLE
            ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
            : supabase;

        // Hard-coded target table
        const schema = 'health';
        const table = 'step_logs';
        const stepsNum = Number(steps);

        // ===== 1) UPDATE existing row for this date =====
        const upd = await sb
            .schema(schema)
            .from(table)
            .update({ steps: stepsNum, updated_at: new Date().toISOString() })
            .eq('date', date)
            .select();

        if (upd.error) {
            console.error('[steps update error]', JSON.stringify(upd.error, null, 2));
            return res.status(500).json({ error: upd.error });
        }

        if (Array.isArray(upd.data) && upd.data.length > 0) {
            return res.json({ ok: true, rows: upd.data });
        }

        // ===== 2) INSERT if no row was updated =====
        const ins = await sb
            .schema(schema)
            .from(table)
            .insert({ date, steps: stepsNum })
            .select();

        if (ins.error) {
            console.error('[steps insert error]', JSON.stringify(ins.error, null, 2));
            return res.status(500).json({ error: ins.error });
        }

        return res.json({ ok: true, rows: ins.data || [] });
    } catch (e) {
        console.error('[steps handler exception]', e);
        return res.status(500).json({ error: e?.message || String(e) });
    }
});


/* ========================= Shortcuts: Steps AGG ingest (update-then-insert) =========================
   POST /shortcuts/steps_agg or /health/steps_agg
   Headers:
     Authorization: Bearer <HEALTHKIT_INGEST_TOKEN>
     Content-Type: application/json
   Body:
     { "date": "YYYY-MM-DD", "steps_agg": <int> }
================================================================================================= */

app.post(['/health/steps_agg', '/shortcuts/steps_agg'], async (req, res) => {
    try {
        // --- Auth check (Bearer) ---
        const auth = (req.get('authorization') || '').trim();
        const token = auth.replace(/^Bearer\s+/i, '');

        if (HEALTHKIT_INGEST_TOKEN && token !== HEALTHKIT_INGEST_TOKEN) {
            return res.status(401).json({ error: 'Invalid token' });
        }

        // --- Validate body ---
        const { date, steps_agg } = req.body || {};
        if (!date || typeof date !== 'string') {
            return res.status(400).json({ error: "Missing/invalid 'date' (YYYY-MM-DD)" });
        }

        const stepsAggNum = Number(steps_agg);
        if (Number.isNaN(stepsAggNum)) {
            return res.status(400).json({ error: "Missing/invalid 'steps_agg' (number)" });
        }

        // --- Supabase client ---
        const sb = SUPABASE_SERVICE_ROLE
            ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
            : supabase;

        const schema = 'health';
        const table = 'step_logs';

        // ===== 1) UPDATE existing row for this date =====
        const upd = await sb
            .schema(schema)
            .from(table)
            .update({
                steps_agg: stepsAggNum,
                updated_at: new Date().toISOString(),
            })
            .eq('date', date)
            .select();

        if (upd.error) {
            console.error('[steps_agg update error]', JSON.stringify(upd.error, null, 2));
            return res.status(500).json({ error: upd.error });
        }

        if (Array.isArray(upd.data) && upd.data.length > 0) {
            return res.json({ ok: true, rows: upd.data });
        }

        // ===== 2) INSERT if no row was updated =====
        const ins = await sb
            .schema(schema)
            .from(table)
            .insert({
                date,
                steps_agg: stepsAggNum,
            })
            .select();

        if (ins.error) {
            console.error('[steps_agg insert error]', JSON.stringify(ins.error, null, 2));
            return res.status(500).json({ error: ins.error });
        }

        return res.json({ ok: true, rows: ins.data || [] });
    } catch (e) {
        console.error('[steps_agg handler exception]', e);
        return res.status(500).json({ error: e?.message || String(e) });
    }
});



// 🧠 Natural Language Command Handler for Gmail via MCP
// =======================================================
// Handles alias send, HTML + text, attachments, CRLF, Base64URL-safe
async function buildGmailMime({ to, from, subject, bodyText, bodyHtml = null, attachments = [] }) {
    const boundary = 'mcp-boundary-' + crypto.randomUUID();

    // --- Headers ---
    let mime = '';
    mime += `To: ${to}\r\n`;
    mime += `From: ${from}\r\n`;
    mime += `Subject: =?UTF-8?B?${Buffer.from(subject, 'utf8').toString('base64')}?=\r\n`;
    mime += `MIME-Version: 1.0\r\n`;
    mime += `Content-Type: multipart/mixed; boundary="${boundary}"\r\n\r\n`;

    // --- Text part ---
    mime += `--${boundary}\r\n`;
    mime += `Content-Type: text/plain; charset="UTF-8"\r\n`;
    mime += `Content-Transfer-Encoding: 7bit\r\n\r\n`;
    mime += `${bodyText}\r\n\r\n`;

    // --- HTML part (optional) ---
    if (bodyHtml) {
        mime += `--${boundary}\r\n`;
        mime += `Content-Type: text/html; charset="UTF-8"\r\n`;
        mime += `Content-Transfer-Encoding: 7bit\r\n\r\n`;
        mime += `${bodyHtml}\r\n\r\n`;
    }

    // --- Attachments (optional) ---
    for (const attachment of attachments) {
        let filename;
        let encodedData;
        let mimeType = 'application/octet-stream';

        if (attachment.startsWith('http://') || attachment.startsWith('https://')) {
            // 🔹 Remote file
            console.log(`[ATTACH] Downloading remote file: ${attachment}`);
            const resp = await fetch(attachment);
            if (!resp.ok) throw new Error(`Failed to fetch attachment: ${attachment}`);
            const buf = Buffer.from(await resp.arrayBuffer());
            encodedData = buf.toString('base64');
            const ct = resp.headers.get('content-type');
            if (ct) mimeType = ct;
            filename = path.basename(new URL(attachment).pathname);
        }
        else if (attachment.startsWith('data:')) {
            // 🔹 Already base64 inline (e.g. data URI)
            console.log(`[ATTACH] Using inline data URI`);
            const match = attachment.match(/^data:(.+?);base64,(.+)$/);
            if (match) {
                mimeType = match[1];
                encodedData = match[2];
                filename = `attachment-${Date.now()}`;
            } else {
                console.warn('[ATTACH] Invalid data URI format');
                continue;
            }
        }
        else if (fs.existsSync(attachment)) {
            // 🔹 Local file
            filename = path.basename(attachment);
            const buf = fs.readFileSync(attachment);
            encodedData = buf.toString('base64');
        }
        else {
            console.warn(`[ATTACH] Skipping unknown source: ${attachment}`);
            continue;
        }

        mime += `--${boundary}\r\n`;
        mime += `Content-Type: ${mimeType}; name="${filename}"\r\n`;
        mime += `Content-Disposition: attachment; filename="${filename}"\r\n`;
        mime += `Content-Transfer-Encoding: base64\r\n\r\n`;
        mime += `${encodedData}\r\n\r\n`;
    }



    // --- End boundary ---
    mime += `--${boundary}--\r\n`;

    // --- Force CRLF and Base64URL encoding ---
    const fixedMime = mime.replace(/\r?\n/g, '\r\n').trimEnd() + '\r\n';

    return Buffer.from(fixedMime, 'ascii')
        .toString('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/, '');
}


app.post('/nl-command', async (req, res) => {
    try {
        const text = (req.body?.text || '').trim().toLowerCase();
        console.log('[NL COMMAND]', text);

        // --- Helper: simple date phrase parser ---
        function parseDatePhrase(phrase) {
            const now = new Date();
            if (/today/.test(phrase)) return now;
            if (/yesterday/.test(phrase)) return new Date(now.setDate(now.getDate() - 1));
            if (/last week/.test(phrase)) return new Date(now.setDate(now.getDate() - 7));
            if (/last month/.test(phrase)) return new Date(now.setMonth(now.getMonth() - 1));
            if (/this month/.test(phrase)) return new Date(now.getFullYear(), now.getMonth(), 1);
            const parsed = new Date(phrase);
            return isNaN(parsed) ? null : parsed;
        }

        // --- 1️⃣ Check last N emails ---
        if (/check( the)? last \d+ emails?/.test(text)) {
            const count = parseInt(text.match(/last (\d+)/)?.[1] || '5', 10);
            const args = await maybeInjectGmailToken({
                url: `https://gmail.googleapis.com/gmail/v1/users/me/messages?maxResults=${count}`,
                method: 'GET',
                response_type: 'json',
                timeout_ms: 10000,
            });
            const result = await tool_http_fetch(args);
            const messages = result?.data?.messages || [];
            const details = [];

            for (const msg of messages) {
                const detailArgs = await maybeInjectGmailToken({
                    url: `https://gmail.googleapis.com/gmail/v1/users/me/messages/${msg.id}?format=metadata&metadataHeaders=Subject&metadataHeaders=From`,
                    method: 'GET',
                    response_type: 'json',
                });
                const detail = await tool_http_fetch(detailArgs);
                const headers = detail?.data?.payload?.headers || [];
                const subject = headers.find(h => h.name === 'Subject')?.value || '(no subject)';
                const from = headers.find(h => h.name === 'From')?.value || '(unknown sender)';
                details.push({ id: msg.id, from, subject });
            }

            return res.json({ ok: true, command: 'check_last_emails', count, messages: details });
        }

        // --- 2️⃣ Filter / summarize / group emails ---
        else if (/emails? (from|about|with|since|before|after|summary|summarize|group|by)/i.test(text)) {
            const wantSummary = /summary|summarize/.test(text);
            const groupBySender = /by sender/.test(text);
            const groupByTopic = /by topic|by subject/.test(text);

            let q = '';
            if (/from (.+?)(?: since| before| after|$)/i.test(text))
                q = `from:${text.match(/from (.+?)(?: since| before| after|$)/i)[1].trim()}`;
            else if (/about (.+?)(?: since| before| after|$)/i.test(text))
                q = `subject:${text.match(/about (.+?)(?: since| before| after|$)/i)[1].trim()}`;
            else if (/with (.+?)(?: since| before| after|$)/i.test(text))
                q = text.match(/with (.+?)(?: since| before| after|$)/i)[1].trim();

            const sinceMatch = text.match(/since (.+)/i) || text.match(/after (.+)/i);
            const beforeMatch = text.match(/before (.+)/i);
            if (sinceMatch) {
                const d = parseDatePhrase(sinceMatch[1]);
                if (d)
                    q += ` after:${d.getFullYear()}/${(d.getMonth() + 1).toString().padStart(2, "0")}/${d.getDate().toString().padStart(2, "0")}`;
            }
            if (beforeMatch) {
                const d = parseDatePhrase(beforeMatch[1]);
                if (d)
                    q += ` before:${d.getFullYear()}/${(d.getMonth() + 1).toString().padStart(2, "0")}/${d.getDate().toString().padStart(2, "0")}`;
            }

            if (!q) return res.status(400).json({ error: 'Could not parse filter or time range.' });
            console.log(`[FILTER EMAILS] Query: ${q}`);

            // Fetch messages
            let messages = [];
            let pageToken = null;
            const maxPerPage = 100;
            const maxTotal = 300;

            do {
                const args = await maybeInjectGmailToken({
                    url: `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${encodeURIComponent(q)}&maxResults=${maxPerPage}${pageToken ? `&pageToken=${pageToken}` : ''}`,
                    method: 'GET', response_type: 'json', timeout_ms: 40000
                });
                const result = await tool_http_fetch(args);
                if (result?.data?.messages?.length) messages.push(...result.data.messages);
                pageToken = result?.data?.nextPageToken || null;
                if (pageToken) await new Promise(r => setTimeout(r, 400));
                if (messages.length >= maxTotal) break;
            } while (pageToken);

            if (!messages.length) return res.json({ ok: true, query: q, summary: 'No matching emails found.' });

            // Fetch details
            const details = [];
            for (const msg of messages.slice(0, maxTotal)) {
                try {
                    const detailArgs = await maybeInjectGmailToken({
                        url: `https://gmail.googleapis.com/gmail/v1/users/me/messages/${msg.id}?format=metadata&metadataHeaders=Subject&metadataHeaders=From`,
                        method: 'GET', response_type: 'json', timeout_ms: 15000
                    });
                    const detail = await tool_http_fetch(detailArgs);
                    const headers = detail?.data?.payload?.headers || [];
                    const subject = headers.find(h => h.name === 'Subject')?.value || '(no subject)';
                    const from = headers.find(h => h.name === 'From')?.value || '(unknown sender)';
                    const snippet = detail?.data?.snippet || '';
                    details.push({ id: msg.id, from, subject, snippet });
                } catch (err) {
                    console.error('[FILTER EMAILS] Detail fetch error:', err.message);
                }
            }

            // Group results
            let groups = {};
            if (groupBySender) {
                for (const d of details) {
                    const key = d.from.toLowerCase();
                    if (!groups[key]) groups[key] = [];
                    groups[key].push(d);
                }
            } else if (groupByTopic) {
                function extractTopic(subject) {
                    const words = subject.split(/\s+/).filter(w => w.length > 3);
                    return words[0]?.toLowerCase() || 'misc';
                }
                for (const d of details) {
                    const key = extractTopic(d.subject);
                    if (!groups[key]) groups[key] = [];
                    groups[key].push(d);
                }
            }

            const summaryLines = [];
            if (Object.keys(groups).length) {
                for (const [group, msgs] of Object.entries(groups)) {
                    summaryLines.push(`\n📬 ${group} — ${msgs.length} emails`);
                    for (const m of msgs.slice(0, 5)) summaryLines.push(`  • ${m.subject}`);
                    if (msgs.length > 5) summaryLines.push(`  ... (${msgs.length - 5} more)`);
                }
            } else {
                for (const m of details) summaryLines.push(`• ${m.subject} — ${m.from}`);
            }

            if (wantSummary) {
                const summarizePrompt = `Summarize these grouped emails into a concise report. Each group should be described in 1–2 sentences:\n---\n${summaryLines.join('\n')}`;
                const summaryResponse = await fetch('http://localhost:3000/tools/call', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        name: 'openai_chat',
                        arguments: { model: 'gpt-5', messages: [{ role: 'user', content: summarizePrompt }], max_tokens: 400 }
                    })
                }).then(r => r.json()).catch(() => null);
                const aiSummary = summaryResponse?.content?.[0]?.json?.choices?.[0]?.message?.content || '(Could not summarize)';
                return res.json({ ok: true, query: q, totalFound: messages.length, groups: Object.keys(groups).length || 1, summary: summaryLines.join('\n'), aiSummary });
            }

            return res.json({ ok: true, query: q, totalFound: messages.length, groups: Object.keys(groups).length || 1, summary: summaryLines.join('\n') });
        }

        // --- 3️⃣ Summarize Gmail conversation threads ---
        else if (/conversation|thread/.test(text)) {
            console.log('[THREAD SUMMARIZE] Intent detected:', text);
            let q = '';
            if (/with (.+?)(?: since| before| after|$)/i.test(text))
                q = `from:${text.match(/with (.+?)(?: since| before| after|$)/i)[1].trim()}`;
            else if (/about (.+?)(?: since| before| after|$)/i.test(text))
                q = `subject:${text.match(/about (.+?)(?: since| before| after|$)/i)[1].trim()}`;
            else if (/conversation (.+)/i.test(text))
                q = text.match(/conversation (.+)/i)[1].trim();

            const sinceMatch = text.match(/since (.+)/i) || text.match(/after (.+)/i);
            const beforeMatch = text.match(/before (.+)/i);
            if (sinceMatch) {
                const d = parseDatePhrase(sinceMatch[1]);
                if (d) q += ` after:${d.getFullYear()}/${(d.getMonth() + 1).toString().padStart(2, "0")}/${d.getDate().toString().padStart(2, "0")}`;
            }
            if (beforeMatch) {
                const d = parseDatePhrase(beforeMatch[1]);
                if (d) q += ` before:${d.getFullYear()}/${(d.getMonth() + 1).toString().padStart(2, "0")}/${d.getDate().toString().padStart(2, "0")}`;
            }

            if (!q) return res.status(400).json({ error: 'Could not parse conversation filter.' });

            // Fetch messages -> group by threadId
            let messages = [];
            let pageToken = null;
            const maxPerPage = 100;
            const maxTotal = 200;
            do {
                const args = await maybeInjectGmailToken({
                    url: `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${encodeURIComponent(q)}&maxResults=${maxPerPage}${pageToken ? `&pageToken=${pageToken}` : ''}`,
                    method: 'GET', response_type: 'json', timeout_ms: 40000
                });
                const result = await tool_http_fetch(args);
                if (result?.data?.messages?.length) messages.push(...result.data.messages);
                pageToken = result?.data?.nextPageToken || null;
                if (pageToken) await new Promise(r => setTimeout(r, 400));
                if (messages.length >= maxTotal) break;
            } while (pageToken);
            if (!messages.length) return res.json({ ok: true, query: q, summary: 'No matching conversations found.' });

            const threads = {};
            for (const msg of messages) {
                const tid = msg.threadId;
                if (!threads[tid]) threads[tid] = [];
                threads[tid].push(msg.id);
            }

            const threadSummaries = [];
            for (const [threadId] of Object.entries(threads)) {
                try {
                    const threadArgs = await maybeInjectGmailToken({
                        url: `https://gmail.googleapis.com/gmail/v1/users/me/threads/${threadId}`,
                        method: 'GET', response_type: 'json', timeout_ms: 40000
                    });
                    const thread = await tool_http_fetch(threadArgs);
                    const msgs = thread?.data?.messages || [];
                    const subject = msgs[0]?.payload?.headers?.find(h => h.name === 'Subject')?.value || '(no subject)';
                    const participants = Array.from(new Set(
                        msgs.flatMap(m => (m.payload?.headers || [])
                            .filter(h => ['From', 'To', 'Cc'].includes(h.name))
                            .map(h => h.value))
                    ));
                    const snippets = msgs.map(m => m.snippet).filter(Boolean).join('\n');
                    const prompt = `Summarize this Gmail conversation between: ${participants.join(', ')}. Subject: ${subject}\nMessages:\n${snippets}`;
                    const summaryResponse = await fetch('http://localhost:3000/tools/call', {
                        method: 'POST', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            name: 'openai_chat',
                            arguments: { model: 'gpt-5', messages: [{ role: 'user', content: prompt }], max_tokens: 400 }
                        })
                    }).then(r => r.json()).catch(() => null);
                    const aiSummary = summaryResponse?.content?.[0]?.json?.choices?.[0]?.message?.content || '(Could not summarize)';
                    threadSummaries.push({ threadId, subject, participants, messageCount: msgs.length, summary: aiSummary });
                } catch (err) {
                    console.error('[THREAD SUMMARY ERROR]', err.message);
                }
            }

            return res.json({ ok: true, query: q, threadCount: threadSummaries.length, threads: threadSummaries });
        }

        // --- 4️⃣ Send email with optional attachments & alias support ---
        else if (/send email|compose email|draft email/.test(text)) {
            console.log('[SEND EMAIL]', text);

            // Extract email fields from natural language
            const toMatch = text.match(/to ([^\s]+@[^\s]+)/i);
            const subjectMatch = text.match(/subject (.+?)(?: with| including| and|$)/i);
            const bodyMatch = text.match(/(?:saying|body|content|message) (.+)/i);
            const aliasMatch = text.match(/from ([^\s]+@[^\s]+)/i);  // 👈 Detect alias

            const to = toMatch ? toMatch[1] : null;
            const subject = subjectMatch ? subjectMatch[1] : 'No subject';
            const bodyText = bodyMatch ? bodyMatch[1] : 'Hello!';
            const sendAs = aliasMatch ? aliasMatch[1] : process.env.SIGNATURE_EMAIL; // 👈 default Gmail

            if (!to) return res.status(400).json({ error: 'Missing recipient (to:)' });

            // Optional attachment path (supports local file)
            const attachmentPaths = [];
            const attachMatch = text.match(/attach(ed|ment)? (.+)/i);
            if (attachMatch) {
                const pathStr = attachMatch[2];
                if (fs.existsSync(pathStr)) attachmentPaths.push(pathStr);
            }

            // --- Build RFC-compliant MIME message ---
            const encodedMessage = buildGmailMime({
                to,
                from: sendAs,
                subject,
                bodyText,
                bodyHtml: `<p>${bodyText}</p>`,      // 👈 adds HTML version automatically
                attachments: attachmentPaths
            });


            // Optional manual approval lock
            const LOCK_FILE = '/opt/supabase-mcp/secrets/send_lock.flag';
            if (fs.existsSync(LOCK_FILE)) {
                const args = await maybeInjectGmailToken({
                    url: 'https://gmail.googleapis.com/gmail/v1/users/me/drafts',
                    method: 'POST',
                    body: {
                        sendAsEmail: sendAs,        // 👈 tells Gmail which alias to use
                        message: { raw: encodedMessage },
                    },
                    response_type: 'json'
                });
                const result = await tool_http_fetch(args);
                return res.json({
                    ok: true,
                    status: 'draft_created',
                    from: sendAs,
                    message: 'Manual approval required before sending.',
                    draft: result?.data,
                });
            }

            // --- Send directly using native fetch (bypass MCP connector) ---
            try {
                console.log('[GMAIL SEND] Sending via direct Gmail API...');

                const tokenData = JSON.parse(fs.readFileSync('/opt/supabase-mcp/secrets/gmail_token.json', 'utf8'));
                const accessToken = tokenData.access_token || tokenData.token;

                const gmailResponse = await fetch("https://gmail.googleapis.com/gmail/v1/users/me/messages/send", {
                    method: "POST",
                    headers: {
                        "Authorization": `Bearer ${accessToken}`,
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify({
                        sendAsEmail: sendAs,
                        message: { raw: encodedMessage }
                    })
                });


                if (!gmailResponse.ok) {
                    const errText = await gmailResponse.text();
                    console.error("[GMAIL SEND ERROR]", gmailResponse.status, errText);
                    return res.status(400).json({
                        ok: false,
                        error: `Gmail API returned ${gmailResponse.status}`,
                        details: errText
                    });
                }

                const result = await gmailResponse.json();
                console.log('[GMAIL SEND SUCCESS]', result.id);

                return res.json({
                    ok: true,
                    status: "sent",
                    from: sendAs,
                    messageId: result.id,
                    threadId: result.threadId
                });

            } catch (error) {
                console.error("[GMAIL SEND ERROR]", error);
                return res.status(500).json({ ok: false, error: error.message });
            }

        }


        // --- 5️⃣ Reply to an existing email or thread ---
        else if (/reply to/i.test(text)) {
            console.log('[REPLY EMAIL]', text);
            const bodyMatch = text.match(/saying (.+)/i);
            const replyText = bodyMatch ? bodyMatch[1].trim() : '(no message)';
            const targetMatch = text.match(/reply to (.+?)(?: saying|$)/i);
            const targetQuery = targetMatch ? targetMatch[1].trim() : null;
            if (!targetQuery) return res.status(400).json({ error: 'Could not determine which email to reply to.' });

            const searchArgs = await maybeInjectGmailToken({
                url: `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${encodeURIComponent(targetQuery)}&maxResults=1`,
                method: 'GET', response_type: 'json'
            });
            const searchRes = await tool_http_fetch(searchArgs);
            const msg = searchRes?.data?.messages?.[0];
            if (!msg) return res.json({ ok: false, message: 'No matching email found to reply to.' });

            const msgId = msg.id;
            const detailArgs = await maybeInjectGmailToken({
                url: `https://gmail.googleapis.com/gmail/v1/users/me/messages/${msgId}?format=metadata&metadataHeaders=Message-ID&metadataHeaders=Subject&metadataHeaders=From&metadataHeaders=To`,
                method: 'GET', response_type: 'json'
            });
            const detail = await tool_http_fetch(detailArgs);
            const headers = detail?.data?.payload?.headers || [];
            const subject = headers.find(h => h.name === 'Subject')?.value || '(no subject)';
            const from = headers.find(h => h.name === 'From')?.value || '(unknown sender)';
            const messageIdHeader = headers.find(h => h.name === 'Message-ID')?.value || null;
            const threadId = detail?.data?.threadId;
            if (!messageIdHeader || !threadId) return res.json({ ok: false, message: 'Missing headers for reply.' });

            const boundary = 'mcp-reply-' + Date.now();
            let mime = `To: ${from}\r\nSubject: Re: ${subject}\r\nIn-Reply-To: ${messageIdHeader}\r\nReferences: ${messageIdHeader}\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset="UTF-8"\r\n\r\n${replyText}\r\n`;

            const encodedMessage = Buffer.from(mime).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
            const LOCK_FILE = '/opt/supabase-mcp/secrets/send_lock.flag';
            if (fs.existsSync(LOCK_FILE)) {
                const args = await maybeInjectGmailToken({
                    url: 'https://gmail.googleapis.com/gmail/v1/users/me/drafts',
                    method: 'POST', body: { message: { raw: encodedMessage, threadId } }, response_type: 'json'
                });
                const result = await tool_http_fetch(args);
                return res.json({ ok: true, status: 'draft_created', threadId, subject, replyText, message: 'Manual approval required before sending.', draft: result?.data });
            }

            const args = await maybeInjectGmailToken({
                url: 'https://gmail.googleapis.com/gmail/v1/users/me/messages/send',
                method: 'POST', body: { raw: encodedMessage, threadId }, response_type: 'json'
            });
            const result = await tool_http_fetch(args);
            return res.json({ ok: true, status: 'reply_sent', threadId, subject, to: from, message: result?.data });
        }

        else {
            return res.json({ ok: false, message: 'Unrecognized natural language command.' });
        }

    } catch (err) {
        console.error('[NL COMMAND ERROR]', err);
        res.status(500).json({ error: err.message });
    }
});
/* ========================= End Natural Language Command Handler ========================= */


app.use((err, req, res, next) => {
    console.error('[UNCAUGHT ERROR]', err);
    if (!res.headersSent) {
        res.status(500).json({
            error: 'Internal Server Error',
            message: err.message,
            stack: err.stack
        });
    }
});


/* ========================= Start server ========================= */

// Catch unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    console.error('[FATAL] Unhandled Rejection at:', promise, 'reason:', reason);
    process.exit(1);
});

// Catch uncaught exceptions
process.on('uncaughtException', (err) => {
    console.error('[FATAL] Uncaught Exception:', err);
    process.exit(1);
});

// Catch graceful exit signals
process.on('exit', (code) => {
    console.error(`[EXIT] Node process exiting with code ${code}`);
}); 
