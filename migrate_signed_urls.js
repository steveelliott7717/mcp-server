// /opt/supabase-mcp/custom/migrate_signed_urls.js
// Replace public storage URLs with signed URLs across all tables
// Run once to migrate, then re-run as a cron every ~10 months to refresh before expiry

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;

// 1 year expiry — re-run this script every ~10 months via cron
const SIGNED_EXPIRY_SECONDS = 365 * 24 * 60 * 60;

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

const TARGETS = [
    { schema: "legal",            table: "tax_forms",                  urlCol: "file_url",                 bucket: "legal_tax_forms" },
    { schema: "genealogy",        table: "documents",                  urlCol: "file_url",                 bucket: "genealogy_documents" },
    { schema: "finance",          table: "purchases",                  urlCol: "receipt_image_url",        bucket: "finance_purchases" },
    { schema: "finance",          table: "recurring_purchase_charges", urlCol: "receipt_image_url",        bucket: "finance_recurring_purchase_charges" },
    { schema: "calendar",         table: "events",                     urlCol: "file_url",                 bucket: "calendar.events" },
    { schema: "health",           table: "dental_xray_records",        urlCol: "image_url",                bucket: "health_dental_x_rays" },
    { schema: "germany",          table: "vocabulary_chunks",          urlCol: "file_url",                 bucket: "germany_vocabulary" },
    { schema: "professional_profile", table: "work_experience",        urlCol: "url",                      bucket: "professional_profile_work_experience" },
    { schema: "professional_profile", table: "publications",           urlCol: "url",                      bucket: "professional_profile_publications" },
    { schema: "gmail",            table: "all_emails",                 urlCol: "attachments_archive_url",  bucket: "gmail_all_emails" },
];

function extractFilePath(url, bucket) {
    if (!url) return null;
    // Strip any existing query string (signed URL tokens) then extract the object path
    const base = url.split("?")[0];
    const patterns = [
        `/storage/v1/object/public/${bucket}/`,
        `/storage/v1/object/sign/${bucket}/`,
        `/storage/v1/object/${bucket}/`,
    ];
    for (const prefix of patterns) {
        const idx = base.indexOf(prefix);
        if (idx !== -1) return base.slice(idx + prefix.length);
    }
    return null;
}

async function migrateTable({ schema, table, urlCol, bucket }) {
    console.log(`\n[${schema}.${table}] Starting...`);

    const { data: rows, error } = await sb
        .schema(schema)
        .from(table)
        .select(`id, ${urlCol}`)
        .not(urlCol, "is", null);

    if (error) {
        console.error(`[${schema}.${table}] ❌ Query failed: ${error.message}`);
        return { updated: 0, errors: 1 };
    }

    if (!rows?.length) {
        console.log(`[${schema}.${table}] ✅ No rows with URLs`);
        return { updated: 0, errors: 0 };
    }

    console.log(`[${schema}.${table}] Found ${rows.length} rows`);

    // Build path → [rowId] map (multiple rows may share the same file)
    const pathMap = new Map();
    for (const row of rows) {
        const path = extractFilePath(row[urlCol], bucket);
        if (!path) {
            console.warn(`[${schema}.${table}] ⚠️ Could not extract path from: ${row[urlCol]?.slice(0, 80)}`);
            continue;
        }
        if (!pathMap.has(path)) pathMap.set(path, []);
        pathMap.get(path).push(row.id);
    }

    const paths = [...pathMap.keys()];
    if (!paths.length) {
        console.log(`[${schema}.${table}] ✅ No valid paths found`);
        return { updated: 0, errors: 0 };
    }

    let updated = 0, errors = 0;

    // createSignedUrls accepts max 100 paths at a time
    for (let i = 0; i < paths.length; i += 100) {
        const batch = paths.slice(i, i + 100);

        const { data: signed, error: signErr } = await sb.storage
            .from(bucket)
            .createSignedUrls(batch, SIGNED_EXPIRY_SECONDS);

        if (signErr) {
            console.error(`[${schema}.${table}] ❌ createSignedUrls error: ${signErr.message}`);
            errors += batch.length;
            continue;
        }

        for (const item of signed) {
            if (!item.signedUrl) {
                console.warn(`[${schema}.${table}] ⚠️ No signedUrl for path: ${item.path}`);
                errors++;
                continue;
            }

            for (const rowId of (pathMap.get(item.path) || [])) {
                const { error: updateErr } = await sb
                    .schema(schema)
                    .from(table)
                    .update({ [urlCol]: item.signedUrl })
                    .eq("id", rowId);

                if (updateErr) {
                    console.error(`[${schema}.${table}] ❌ Update id=${rowId}: ${updateErr.message}`);
                    errors++;
                } else {
                    updated++;
                    console.log(`[${schema}.${table}] ✅ ${item.path.slice(0, 60)}`);
                }
            }
        }
    }

    console.log(`[${schema}.${table}] Done: ${updated} updated, ${errors} errors`);
    return { updated, errors };
}

async function main() {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
        console.error("[FATAL] SUPABASE_URL or SUPABASE_SERVICE_ROLE not set");
        process.exit(1);
    }

    console.log(`[${new Date().toISOString()}] Starting signed URL migration (expiry: 1 year)`);

    let totalUpdated = 0, totalErrors = 0;

    for (const target of TARGETS) {
        const { updated, errors } = await migrateTable(target);
        totalUpdated += updated;
        totalErrors += errors;
    }

    console.log(`\n[${new Date().toISOString()}] Migration complete: ${totalUpdated} updated, ${totalErrors} errors`);
}

main().catch((err) => {
    console.error(`[FATAL] ${err.message}`);
    process.exit(1);
});
