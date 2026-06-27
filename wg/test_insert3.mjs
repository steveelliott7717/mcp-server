import dotenv from 'dotenv';
dotenv.config({ path: '/opt/supabase-mcp/custom/.env' });
import { callTool } from './mcp.js';

const res = await callTool('insert_data', {
    schema: 'finance',
    table: 'wg_gesucht_listings',
    data: {
        listing_id: '13288404',
        url: 'https://www.wg-gesucht.de/wohnungen-in-Leipzig-Kleinzschocher.13288404.html',
        title: 'Zwischenmiete für ein Jahr: schöne möblierte 3-Raum-Wohnung',
        size_m2: 65,
        rent: 600,
        additional_costs: 200,
        deposit: 1500,
        description_wohnung: "Die Wohnung liegt im ruhigen Stadtteil Kleinzschocher und ist gut an den ÖPNV angebunden. \n Der nächste Supermarkt ist am Adler (ca. 10 Minuten zu Fuß).",
        description_lage: "Grundsätzlich sollen alle Möbel für die Zwischenmiete in der Wohnung bleiben.",
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
const parsed = JSON.parse(body);
console.log('error?', parsed.error || 'none');
console.log('id:', parsed.rows?.[0]?.id);
