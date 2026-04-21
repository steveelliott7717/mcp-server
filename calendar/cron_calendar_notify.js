// /opt/supabase-mcp/custom/calendar/cron_calendar_notify.js
// 📅 Calendar Event Notification Cron
// ✅ Queries calendar.events AND calendar.recurring_event_instances for notifications
// ✅ Handles three notification types:
//    1. "On the day" notifications (sent at specified time on event day)
//    2. "Before event" notifications (X minutes before start_time)
//    3. "At start" notifications (when event starts)
// ✅ Updates notified_*_at timestamps to prevent duplicates

import { callTool } from "./mcp.js";

/* -------------------------------------------------------------------------- */
/* 🧠 Utility Functions                                                       */
/* -------------------------------------------------------------------------- */

function parseToolResponse(res) {
    const content = res?.content?.[0] || res?.result?.content?.[0];
    if (!content) return null;

    if (content.type === "text" && content.text) {
        try {
            const parsed = JSON.parse(content.text);
            // Handle { rows: [...] }, { data: [...] }, or raw array
            if (Array.isArray(parsed)) return parsed;
            if (Array.isArray(parsed.rows)) return parsed.rows;
            if (Array.isArray(parsed.data)) return parsed.data;
            return parsed;
        } catch {
            return null;
        }
    }

    if (content.json) {
        const j = content.json;
        if (Array.isArray(j)) return j;
        if (Array.isArray(j.rows)) return j.rows;
        if (Array.isArray(j.data)) return j.data;
        return j;
    }

    if (typeof content === "object" && !content.type) {
        if (Array.isArray(content.rows)) return content.rows;
        if (Array.isArray(content.data)) return content.data;
        return content;
    }

    return null;
}

/** Format event time for notification display */
function formatEventTime(start_time) {
    // Parse the UTC timestamp and format directly in Central Time
    const date = new Date(start_time);

    // Format the event time in Central Time
    const timeStr = date.toLocaleTimeString("en-US", {
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
        timeZone: "America/Chicago"
    });

    // Get the event date in Central Time for comparison
    const eventDateStr = date.toLocaleDateString("en-US", {
        timeZone: "America/Chicago",
        year: "numeric",
        month: "2-digit",
        day: "2-digit"
    });

    // Get today's date in Central Time
    const nowCentral = new Date().toLocaleDateString("en-US", {
        timeZone: "America/Chicago",
        year: "numeric",
        month: "2-digit",
        day: "2-digit"
    });

    // Get tomorrow's date in Central Time
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    const tomorrowDateStr = tomorrow.toLocaleDateString("en-US", {
        timeZone: "America/Chicago",
        year: "numeric",
        month: "2-digit",
        day: "2-digit"
    });

    // Check if today
    if (eventDateStr === nowCentral) {
        return `today at ${timeStr}`;
    }

    // Check if tomorrow
    if (eventDateStr === tomorrowDateStr) {
        return `tomorrow at ${timeStr}`;
    }

    // Otherwise show full date
    const dateStr = date.toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        timeZone: "America/Chicago"
    });
    return `${dateStr} at ${timeStr}`;
}

/** Get current time in ISO format */
function nowISO() {
    return new Date().toISOString();
}

/** Query both events and recurring_event_instances tables */
async function queryBothEventTables(selectFields, whereConditions, runId) {
    let allRows = [];

    // Query regular events table
    try {
        const res1 = await callTool("query_table", {
            schema: "calendar",
            table: "events",
            select: selectFields,
            where: whereConditions,
            orderBy: { column: "start_time", ascending: true },
            limit: 50,
        });
        const rows1 = parseToolResponse(res1) || [];
        // Mark these as from events table
        rows1.forEach(row => row._source_table = "events");
        allRows = allRows.concat(rows1);
    } catch (err) {
        console.error(`[${runId}] ❌ Query failed for events table: ${err.message}`);
    }

    // Query recurring_event_instances table
    try {
        const res2 = await callTool("query_table", {
            schema: "calendar",
            table: "recurring_event_instances",
            select: selectFields,
            where: whereConditions,
            orderBy: { column: "start_time", ascending: true },
            limit: 50,
        });
        const rows2 = parseToolResponse(res2) || [];
        // Mark these as from recurring_event_instances table
        rows2.forEach(row => row._source_table = "recurring_event_instances");
        allRows = allRows.concat(rows2);
    } catch (err) {
        console.error(`[${runId}] ❌ Query failed for recurring_event_instances table: ${err.message}`);
    }

    // Sort combined results by start_time
    allRows.sort((a, b) => new Date(a.start_time) - new Date(b.start_time));

    return allRows;
}

/* -------------------------------------------------------------------------- */
/* 🔔 Notification Type Handlers                                             */
/* -------------------------------------------------------------------------- */

/** Send "on the day" notification */
async function sendOnTheDayNotification(event, runId) {
    const eventTime = formatEventTime(event.start_time);
    const title = `📅 Today: ${event.title}`;
    const body = event.location
        ? `${eventTime} • ${event.location}}`
        : eventTime;

    try {
        await callTool("notify_push", {
            provider: "pushover",
            category: event._source_table === "recurring_event_instances" ? "calendar_recurring" : "calendar_events",
            title: title,
            message: body,
            no_log: true,
        });
        console.log(`[${runId}] 📲 Sent "on the day" notification for event ${event.id}: ${event.title}`);

        // Update pushover_sent_on_the_day in the correct table
        const tableName = event._source_table || "events";
        await callTool("update_data", {
            schema: "calendar",
            table: tableName,
            pk: "id",
            where: { id: event.id },
            data: {
                pushover_sent_on_the_day: nowISO(),
            },
        });
        console.log(`[${runId}] ✅ Marked "on the day" notification sent for event ${event.id} in ${tableName}`);

        return true;
    } catch (err) {
        console.error(`[${runId}] ❌ Failed to send "on the day" notification for event ${event.id}: ${err.message}`);
        return false;
    }
}

/** Send "before event" notification */
async function sendBeforeEventNotification(event, runId) {
    const eventTime = formatEventTime(event.start_time);
    const minutes = event.notify_before_event_minutes;
    const title = `⏰ Reminder: ${event.title}`;
    const body = event.location
        ? `In ${minutes} min • ${eventTime} • ${event.location}}`
        : `In ${minutes} min • ${eventTime}`;

    try {
        await callTool("notify_push", {
            provider: "pushover",
            category: event._source_table === "recurring_event_instances" ? "calendar_recurring" : "calendar_events",
            title: title,
            message: body,
            no_log: true,
        });
        console.log(`[${runId}] 📲 Sent "before event" notification for event ${event.id}: ${event.title} (${minutes} min)`);

        // Update pushover_sent_before_event in the correct table
        const tableName = event._source_table || "events";
        await callTool("update_data", {
            schema: "calendar",
            table: tableName,
            pk: "id",
            where: { id: event.id },
            data: {
                pushover_sent_before_event: nowISO(),
            },
        });
        console.log(`[${runId}] ✅ Marked "before event" notification sent for event ${event.id} in ${tableName}`);

        return true;
    } catch (err) {
        console.error(`[${runId}] ❌ Failed to send "before event" notification for event ${event.id}: ${err.message}`);
        return false;
    }
}

/** Send "at start" notification */
async function sendAtStartNotification(event, runId) {
    const eventTime = formatEventTime(event.start_time);
    const title = `🔔 Starting Now: ${event.title}`;
    const body = event.description || event.location || `Event starts ${eventTime}`;

    try {
        await callTool("notify_push", {
            provider: "pushover",
            category: event._source_table === "recurring_event_instances" ? "calendar_recurring" : "calendar_events",
            title: title,
            message: body,
            no_log: true,
        });
        console.log(`[${runId}] 📲 Sent "at start" notification for event ${event.id}: ${event.title}`);

        // Update pushover_sent_at_start in the correct table
        const tableName = event._source_table || "events";
        await callTool("update_data", {
            schema: "calendar",
            table: tableName,
            pk: "id",
            where: { id: event.id },
            data: {
                pushover_sent_at_start: nowISO(),
            },
        });
        console.log(`[${runId}] ✅ Marked "at start" notification sent for event ${event.id} in ${tableName}`);

        return true;
    } catch (err) {
        console.error(`[${runId}] ❌ Failed to send "at start" notification for event ${event.id}: ${err.message}`);
        return false;
    }
}

/* -------------------------------------------------------------------------- */
/* 🚀 Main Notification Functions                                            */
/* -------------------------------------------------------------------------- */

/** Process "on the day" notifications */
async function processOnTheDayNotifications(runId) {
    console.log(`[${runId}] 🔍 Checking for "on the day" notifications...`);

    // Get current time in Central Time
    const nowCentral = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Chicago" }));

    // Calculate today's date range in Central Time
    const todayStartCentral = new Date(nowCentral.getFullYear(), nowCentral.getMonth(), nowCentral.getDate());

    // Get current time in HH:MM:SS format (Central Time)
    const currentTimeCentral = nowCentral.toTimeString().split(" ")[0]; // "HH:MM:SS"

    // Query both tables
    const rows = await queryBothEventTables(
        ["id", "title", "description", "start_time", "location", "notify_on_the_day_time"],
        {
            notify_on_the_day: { eq: true },
            pushover_sent_on_the_day: { eq: null },
        },
        runId
    );

    console.log(`[${runId}] Found ${rows.length} potential "on the day" events from both tables (before filtering)`);

    let sent = 0;
    for (const event of rows) {
        // Get event's date in Central Time
        const eventDate = new Date(event.start_time);
        const eventDateCentral = eventDate.toLocaleDateString("en-US", {
            timeZone: "America/Chicago",
            year: "numeric",
            month: "2-digit",
            day: "2-digit"
        });
        const todayDateCentral = todayStartCentral.toLocaleDateString("en-US", {
            year: "numeric",
            month: "2-digit",
            day: "2-digit"
        });

        // Only process if event is actually today in Central Time
        if (eventDateCentral !== todayDateCentral) {
            continue;
        }

        // Check if current time >= notification time
        const notifyTime = event.notify_on_the_day_time || "08:00:00";
        if (currentTimeCentral >= notifyTime) {
            const success = await sendOnTheDayNotification(event, runId);
            if (success) sent++;
            await new Promise((r) => setTimeout(r, 250)); // Rate limiting
        }
    }

    console.log(`[${runId}] ✅ Sent ${sent}/${rows.length} "on the day" notifications`);
    return sent;
}

/** Process "before event" notifications */
async function processBeforeEventNotifications(runId) {
    console.log(`[${runId}] 🔍 Checking for "before event" notifications...`);

    // Get current time
    const now = new Date();
    const lookAheadWindow = new Date(now.getTime() + 24 * 60 * 60 * 1000); // 24 hours ahead

    // Query both tables
    const rows = await queryBothEventTables(
        ["id", "title", "description", "start_time", "location", "notify_before_event_minutes"],
        {
            notify_before_event: { eq: true },
            pushover_sent_before_event: { eq: null },
        },
        runId
    );

    console.log(`[${runId}] Found ${rows.length} potential "before event" notifications from both tables (before filtering)`);

    let sent = 0;
    for (const event of rows) {
        // Get event start time and calculate notification time
        const startTime = new Date(event.start_time);
        const notifyMinutes = event.notify_before_event_minutes || 30;
        const notifyTime = new Date(startTime.getTime() - notifyMinutes * 60 * 1000);

        // Check if we've reached the notification time and event is within lookahead window
        if (now >= notifyTime && startTime <= lookAheadWindow) {
            const success = await sendBeforeEventNotification(event, runId);
            if (success) sent++;
            await new Promise((r) => setTimeout(r, 250)); // Rate limiting
        }
    }

    console.log(`[${runId}] ✅ Sent ${sent}/${rows.length} "before event" notifications`);
    return sent;
}

/** Process "at start" notifications */
async function processAtStartNotifications(runId) {
    console.log(`[${runId}] 🔍 Checking for "at start" notifications...`);

    // Get current time in Central Time
    const nowCentral = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Chicago" }));
    const oneMinuteAgo = new Date(nowCentral.getTime() - 1 * 60 * 1000);
    const oneMinuteAhead = new Date(nowCentral.getTime() + 1 * 60 * 1000);

    // Query both tables
    const rows = await queryBothEventTables(
        ["id", "title", "description", "start_time", "location"],
        {
            notify_at_start: { eq: true },
            pushover_sent_at_start: { eq: null },
        },
        runId
    );

    console.log(`[${runId}] Found ${rows.length} potential "at start" notifications from both tables (before filtering)`);

    let sent = 0;
    for (const event of rows) {
        // Convert event start time to Central Time for comparison
        const eventStartCentral = new Date(new Date(event.start_time).toLocaleString("en-US", { timeZone: "America/Chicago" }));

        // Check if event start time is within ±1 minute of current Central Time
        if (eventStartCentral >= oneMinuteAgo && eventStartCentral < oneMinuteAhead) {
            const success = await sendAtStartNotification(event, runId);
            if (success) sent++;
            await new Promise((r) => setTimeout(r, 250)); // Rate limiting
        }
    }

    console.log(`[${runId}] ✅ Sent ${sent}/${rows.length} "at start" notifications`);
    return sent;
}

/* -------------------------------------------------------------------------- */
/* 🏁 Main Entry Point                                                        */
/* -------------------------------------------------------------------------- */

async function notifyCalendarEvents() {
    const runId = Date.now().toString(36);
    console.log(`[${new Date().toISOString()}] 📅 [${runId}] Starting calendar notifications`);

    let totalSent = 0;

    try {
        // Process all three notification types
        totalSent += await processOnTheDayNotifications(runId);
        totalSent += await processBeforeEventNotifications(runId);
        totalSent += await processAtStartNotifications(runId);

        console.log(`[${new Date().toISOString()}] ✅ [${runId}] Calendar notifications complete: ${totalSent} total sent`);
        process.exitCode = 0;
    } catch (err) {
        console.error(`[${runId}] ❌ Fatal error in calendar notifications: ${err.message}`);
        process.exitCode = 1;
    }
}

/* -------------------------------------------------------------------------- */
/* 🏁 CLI Entrypoint                                                          */
/* -------------------------------------------------------------------------- */

notifyCalendarEvents().catch((err) => {
    console.error(`[notifyCalendarEvents] Fatal error: ${err.message}`);
    process.exitCode = 1;
});