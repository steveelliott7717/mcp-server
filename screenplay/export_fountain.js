// screenplay/export_fountain.js
// Exports a script as a .fountain file from the screenplay schema
// Usage: node export_fountain.js <script_id>

import dotenv from "dotenv";
dotenv.config({ path: "/opt/supabase-mcp/custom/.env" });

import fs from "fs";
import { callTool } from "../gmail/mcp.js";

function parseRows(res) {
    const content = res?.content?.[0] || res?.result?.content?.[0];
    if (!content) return [];
    try {
        const parsed = JSON.parse(content.text);
        if (Array.isArray(parsed)) return parsed;
        return parsed?.data || parsed?.rows || [];
    } catch { return []; }
}

async function exportFountain(scriptId) {
    // Fetch script
    const scriptRes = await callTool("query_table", {
        schema: "screenplay", table: "scripts",
        select: ["id", "title"],
        where: { id: { eq: scriptId } },
        limit: 1,
    });
    const script = parseRows(scriptRes)[0];
    if (!script) throw new Error(`Script ${scriptId} not found`);

    // Fetch scenes ordered by position
    const scenesRes = await callTool("query_table", {
        schema: "screenplay", table: "scenes",
        select: ["id", "position", "scene_header_id"],
        where: { script_id: { eq: scriptId } },
        orderBy: [{ column: "position", ascending: true }],
    });
    const scenes = parseRows(scenesRes);

    // Fetch scene headers
    const headersRes = await callTool("query_table", {
        schema: "screenplay", table: "scene_headers",
        select: ["id", "int_ext", "location", "sub_location", "time_of_day", "modifier"],
        where: { script_id: { eq: scriptId } },
    });
    const headers = Object.fromEntries(parseRows(headersRes).map(h => [h.id, h]));

    // Fetch all elements ordered by position
    const elementsRes = await callTool("query_table", {
        schema: "screenplay", table: "script_elements",
        select: ["id", "scene_id", "type", "content", "character_id", "parenthetical", "dialogue_modifier", "position"],
        where: { script_id: { eq: scriptId } },
        orderBy: [{ column: "position", ascending: true }],
    });
    const allElements = parseRows(elementsRes);

    // Fetch characters
    const charsRes = await callTool("query_table", {
        schema: "screenplay", table: "characters",
        select: ["id", "script_name"],
        where: { script_id: { eq: scriptId } },
    });
    const chars = Object.fromEntries(parseRows(charsRes).map(c => [c.id, c]));

    // Group elements by scene
    const elementsByScene = {};
    for (const el of allElements) {
        if (!elementsByScene[el.scene_id]) elementsByScene[el.scene_id] = [];
        elementsByScene[el.scene_id].push(el);
    }

    const lines = [];

    // Title page
    lines.push(`Title: ${script.title}`);
    lines.push("");
    lines.push("");

    for (const scene of scenes) {
        const header = headers[scene.scene_header_id];
        if (!header) continue;

        // Scene heading
        let heading = `${header.int_ext} ${header.location}`;
        if (header.sub_location) heading += ` - ${header.sub_location}`;
        if (header.time_of_day) heading += ` - ${header.time_of_day}`;
        if (header.modifier) heading += ` - ${header.modifier}`;
        lines.push(heading);
        lines.push("");

        for (const el of (elementsByScene[scene.id] || [])) {
            switch (el.type) {
                case "action":
                    lines.push(el.content);
                    lines.push("");
                    break;

                case "dialogue": {
                    const char = chars[el.character_id];
                    let cue = char?.script_name || "UNKNOWN";
                    if (el.dialogue_modifier) cue += ` (${el.dialogue_modifier})`;
                    lines.push(cue);
                    if (el.parenthetical) lines.push(`(${el.parenthetical})`);
                    lines.push(el.content);
                    lines.push("");
                    break;
                }

                case "transition":
                    lines.push(`> ${el.content}`);
                    lines.push("");
                    break;

                case "sub_heading":
                    lines.push(`.${el.content}`);
                    lines.push("");
                    break;

                case "close_on":
                case "insert":
                case "pov":
                case "series_of_shots":
                case "series_of_scenes":
                case "intercut":
                case "back_to_scene":
                case "back_to_present":
                case "montage":
                case "flashback":
                case "title_card":
                    lines.push(`.${el.content}`);
                    lines.push("");
                    break;

                default:
                    lines.push(el.content);
                    lines.push("");
            }
        }
    }

    const slug = script.title.replace(/[^a-z0-9]+/gi, "_").toLowerCase();
    const timestamp = new Date().toISOString().slice(0, 10);
    const filename = `${slug}_${timestamp}.fountain`;
    const outPath = `/opt/supabase-mcp/custom/screenplay/exports/${filename}`;

    fs.writeFileSync(outPath, lines.join("\n"), "utf8");
    console.log(`[EXPORT] ${outPath}`);
    return outPath;
}

const scriptId = parseInt(process.argv[2]);
if (!scriptId) {
    console.error("Usage: node export_fountain.js <script_id>");
    process.exit(1);
}

exportFountain(scriptId)
    .then(path => { console.log(`[DONE] ${path}`); process.exit(0); })
    .catch(err => { console.error(`[ERROR] ${err.message}`); process.exit(1); });
