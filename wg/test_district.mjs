import dotenv from 'dotenv';
dotenv.config({ path: '/opt/supabase-mcp/custom/.env' });
import { callTool } from './mcp.js';

// Try district with a completely fresh listing_id
const res = await callTool('insert_data', {
  schema: 'finance', table: 'wg_gesucht_listings',
  data: { listing_id: 'FRESH001', url: 'https://test.com', title: 'Test', district: 'Schleussig', evaluated: false, message_sent: false }
});
const body = res?.result?.content?.[0]?.text || res?.content?.[0]?.text || '';
const parsed = JSON.parse(body);
console.log('error:', parsed.error || 'none');
console.log('id:', parsed.rows?.[0]?.id);
console.log('district:', parsed.rows?.[0]?.district);
