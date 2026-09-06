-- Source: finance.purchases
with source as (select * from {{ ref('finance__purchases') }})
select
    id::bigint            as purchase_id,
    category::varchar     as category,
    vendor::varchar       as vendor,
    item_name::varchar    as item_name,
    quantity::numeric     as quantity,
    base_cost::numeric    as base_cost,
    shipping_cost::numeric as shipping_cost,
    tax_cost::numeric     as tax_cost,
    other_cost::numeric   as other_cost,
    coalesce(base_cost, 0) + coalesce(shipping_cost, 0)
      + coalesce(tax_cost, 0) + coalesce(other_cost, 0) as total_cost,
    purchase_date::date   as purchase_date,
    card_type::varchar    as card_type,
    card_last4::varchar   as card_last4
from source
