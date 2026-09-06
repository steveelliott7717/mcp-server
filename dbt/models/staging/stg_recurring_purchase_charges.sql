-- Source: finance.recurring_purchase_charges
with source as (select * from {{ ref('finance__recurring_purchase_charges') }})
select
    id::bigint                    as charge_id,
    recurring_purchase_id::bigint as recurring_purchase_id,
    vendor::varchar               as vendor,
    item_name::varchar            as item_name,
    charge_date::date             as charge_date,
    invoice_date::date            as invoice_date,
    base_cost::numeric            as base_cost,
    tax_cost::numeric             as tax_cost,
    other_cost::numeric           as other_cost,
    coalesce(base_cost, 0) + coalesce(tax_cost, 0)
      + coalesce(other_cost, 0)   as total_cost,
    (manually_overridden = 'true') as is_manually_overridden
from source
