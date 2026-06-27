import dotenv from 'dotenv';
dotenv.config({ path: '/opt/supabase-mcp/custom/.env' });
import { callTool } from './mcp.js';

// Try inserting a row similar to what the scraper sends
const res = await callTool('insert_data', {
    schema: 'finance',
    table: 'wg_gesucht_listings',
    data: {
        listing_id: '13382313',
        url: 'https://www.wg-gesucht.de/wohnungen-in-Leipzig-Kleinzschocher.13382313.html',
        title: 'Nachmieter für 3 Raum Wohnung in Kleinzschocher gesucht',
        size_m2: 70,
        rent: 475,
        additional_costs: 165,
        other_costs: 115,
        has_balcony: false,
        has_elevator: false,
        has_washing_machine: false,
        has_dishwasher: false,
        has_basement: false,
        has_bathroom: false,
        has_own_kitchen: false,
        furnished: false,
        evaluated: false,
        message_sent: false,
    }
});
const body = res?.result?.content?.[0]?.text || res?.content?.[0]?.text || '';
console.log('Raw response body:', body.slice(0, 500));
