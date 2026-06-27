import dotenv from 'dotenv';
dotenv.config({ path: '/opt/supabase-mcp/custom/.env' });
import { callTool } from './mcp.js';

async function tryInsert(label, data) {
  const res = await callTool('insert_data', { schema: 'finance', table: 'wg_gesucht_listings', data });
  const body = res?.result?.content?.[0]?.text || res?.content?.[0]?.text || '';
  const parsed = JSON.parse(body);
  const ok = !parsed.error;
  console.log(label + ':', ok ? `✅ id=${parsed.rows?.[0]?.id}` : `❌ ${parsed.message}`);
  // clean up if inserted
  if (ok && data.listing_id) await callTool('delete_data', { schema: 'finance', table: 'wg_gesucht_listings', where: { listing_id: { op: 'eq', value: data.listing_id } } });
  return ok;
}

const base = { listing_id: 'BSEARCH', url: 'https://test.com', title: 'Test', evaluated: false, message_sent: false };
const lage = "Grundsätzlich sollen alle Möbel für die Zwischenmiete in der Wohnung bleiben. Einzelne Möbelstücke könnten bei Bedarf aber auch zusammen mit unseren persönlichen Dingen eingelagert werden. \n\n Zeitraum: Die Wohnung ist verfügbar ab Anfang Juni 2026 bis Ende Mai 2027. Die genauen Daten können individuell abgesprochen werden. \n\n Gesamtkosten pro Monat: (Warmmiete, Stromvorauszahlung, WLAN): 900€ \n\n Bei Fragen bitte melden; wir freuen uns auf eure Nachrichten!";
const wohnung = "Die Wohnung liegt im ruhigen Stadtteil Kleinzschocher und ist gut an den ÖPNV angebunden. \n Der nächste Supermarkt ist am Adler (ca. 10 Minuten zu Fuß). Bäckereien, Restaurants und Spielplätze sind innerhalb von 5 Minuten zu Fuß zu erreichen. Mit der Straßenbahn ist man in 25 Minuten im Zentrum. \n Gleich auf der anderen Straßenseite lädt der Volkspark zum Spazieren und Spielen ein. Von hier aus kann man auch schöne Radtouten durchs Grüne machen - z.B. in den Auwald, zum Wildpark (15 Min.), Cospudener oder Kulkwitzer See (jeweils 20-25 Min.).";

await tryInsert('base only', { ...base });
await tryInsert('+ desc_lage', { ...base, description_lage: lage });
await tryInsert('+ desc_wohnung', { ...base, description_wohnung: wohnung });
await tryInsert('+ both descs', { ...base, description_wohnung: wohnung, description_lage: lage });
await tryInsert('+ numerics', { ...base, size_m2: 65, rent: 600, additional_costs: 200, deposit: 1500 });
await tryInsert('+ all bools', { ...base, has_balcony: false, has_elevator: false, has_washing_machine: false, has_dishwasher: false, has_basement: false, has_bathroom: false, has_own_kitchen: false, furnished: false });
await tryInsert('+ district=title', { ...base, district: 'Zwischenmiete für ein Jahr: schöne möblierte 3-Raum-Wohnung' });
await tryInsert('full payload', { ...base, district: 'Zwischenmiete für ein Jahr', size_m2: 65, rent: 600, description_wohnung: wohnung, description_lage: lage, has_balcony: false, has_elevator: false, has_washing_machine: false, has_dishwasher: false, has_basement: false, has_bathroom: false, has_own_kitchen: false, furnished: false });
