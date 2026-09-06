-- Source: finance.recurring_purchases
with source as (select * from {{ ref('finance__recurring_purchases') }})
select
    id::bigint             as recurring_purchase_id,
    category::varchar      as category,
    vendor::varchar        as vendor,
    item_name::varchar     as item_name,
    base_cost::numeric     as base_cost,
    tax_cost::numeric      as tax_cost,
    other_cost::numeric    as other_cost,
    frequency::varchar     as frequency,
    start_charge_date::date as start_charge_date,
    next_charge_date::date as next_charge_date,
    (active = 'true')      as is_active,
    card_type::varchar     as card_type
from source
