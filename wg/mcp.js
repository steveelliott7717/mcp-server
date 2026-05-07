/**
 * Unified MCP callTool helper
 * ✅ Deep-clones all args to prevent in-place mutation (fixes {eq:{eq:...}} bug)
 * ✅ Works both inside MCP runtime and via local HTTP endpoint
 * ✅ Provides clean structured error handling and safe JSON parsing
 */

async function callTool(name, args = {}) {
    // 🧩 Deep clone arguments to break shared reference mutation
    // This prevents the MCP bridge or PostgREST builder from modifying nested objects
    const safeArgs = JSON.parse(JSON.stringify(args || {}));

    // ✅ In-process dispatcher (runtime plugin)
    if (globalThis.mcp_mcp_server_fyi__jit_plugin?.callTool) {
        try {
            return await globalThis.mcp_mcp_server_fyi__jit_plugin.callTool({
                name,
                arguments: safeArgs,
            });
        } catch (err) {
            console.error(`[MCP] callTool(${name}) runtime error:`, err);
            throw err;
        }
    }

    // ✅ Otherwise, fallback to HTTP call to local MCP endpoint
    const MCP_SERVER_URL =
        process.env.MCP_URL ||
        process.env.MCP_SERVER_URL ||
        `${process.env.MCP_PUBLIC_URL}/sse`;

    const TRUST_TOKEN =
        process.env.MCP_TRUST_TOKEN ||
        process.env.TRUST_TOKEN ||
        process.env.SUPABASE_SERVICE_ROLE_KEY ||
        "";

    const payload = { name, arguments: safeArgs };

    let res;
    try {
        res = await fetch(MCP_SERVER_URL, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "X-MCP-Trust": TRUST_TOKEN,
            },
            body: JSON.stringify(payload),
        });
    } catch (netErr) {
        console.error(`[MCP] Network error calling ${name}:`, netErr);
        throw new Error(`Network error calling MCP tool ${name}: ${netErr.message}`);
    }

    if (!res.ok) {
        const text = await res.text().catch(() => "(no body)");
        throw new Error(`MCP tool ${name} failed: ${res.status} ${text}`);
    }

    try {
        const json = await res.json();
        return json;
    } catch (parseErr) {
        console.error(`[MCP] Invalid JSON response from ${name}:`, parseErr);
        throw new Error(`MCP tool ${name} returned invalid JSON`);
    }
}

// ✅ Define globally for convenience
globalThis.callTool = callTool;

// ✅ Export properly for ESM imports
export { callTool };
