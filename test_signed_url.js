// One-time test: converts 1 row in legal.tax_forms to a signed URL
import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import { createClient } from "@supabase/supabase-js";

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE);
const SIGNED_EXPIRY_SECONDS = 365 * 24 * 60 * 60;

const { data: rows } = await sb.schema("legal").from("tax_forms")
    .select("id, file_url")
    .not("file_url", "is", null)
    .limit(1);

const row = rows?.[0];
if (!row) { console.log("No rows found"); process.exit(0); }

console.log("Before:", row.file_url);

const base = row.file_url.split("?")[0];
const prefix = base.indexOf("/storage/v1/object/public/legal_tax_forms/");
const filePath = base.slice(prefix + "/storage/v1/object/public/legal_tax_forms/".length);

console.log("File path:", filePath);

const { data: signed, error } = await sb.storage
    .from("legal_tax_forms")
    .createSignedUrl(filePath, SIGNED_EXPIRY_SECONDS);

if (error) { console.error("Error:", error.message); process.exit(1); }

console.log("Signed URL:", signed.signedUrl);

const { error: updateErr } = await sb.schema("legal").from("tax_forms")
    .update({ file_url: signed.signedUrl })
    .eq("id", row.id);

if (updateErr) { console.error("Update failed:", updateErr.message); process.exit(1); }

console.log("✅ Updated row", row.id);
