import dotenv from 'dotenv';
dotenv.config({ path: '/opt/supabase-mcp/custom/.env' });
import { callTool } from './mcp.js';

const res = await callTool('insert_data', {
    schema: 'finance',
    table: 'wg_gesucht_listings',
    data: {
        listing_id: 'TEST001',
        url: 'https://test.com/TEST001',
        title: 'Test Listing',
        evaluated: false,
        message_sent: false,
    }
});
console.log('Response:', JSON.stringify(res, null, 2));
