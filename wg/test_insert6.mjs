import dotenv from 'dotenv';
dotenv.config({ path: '/opt/supabase-mcp/custom/.env' });
import { callTool } from './mcp.js';

async function tryInsert(label, district) {
  const data = { listing_id: 'DTEST', url: 'https://test.com', title: 'Test', district, evaluated: false, message_sent: false };
  const res = await callTool('insert_data', { schema: 'finance', table: 'wg_gesucht_listings', data });
  const body = res?.result?.content?.[0]?.text || res?.content?.[0]?.text || '';
  const parsed = JSON.parse(body);
  const ok = !parsed.error;
  console.log(label + ':', ok ? `✅` : `❌`);
  if (ok) await callTool('delete_data', { schema: 'finance', table: 'wg_gesucht_listings', where: { listing_id: { op: 'eq', value: 'DTEST' } } });
  return ok;
}

await tryInsert('short text', 'Schleussig');
await tryInsert('with colon', 'Test: something');
await tryInsert('with umlaut', 'Möblierte Wohnung');
await tryInsert('with hyphen', '3-Raum-Wohnung');
await tryInsert('long 50 chars', 'Zwischenmiete für ein Jahr: schöne möblierte ABC');
await tryInsert('exact fail text', 'Zwischenmiete für ein Jahr: schöne möblierte 3-Raum-Wohnung');
