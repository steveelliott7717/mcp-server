-- Monthly spend, unioning one-off purchases with materialised subscription
-- charges.
--
-- finance.purchases and finance.recurring_purchase_charges are separate tables
-- with separate cost breakdowns, and nothing in the schema reconciles them.
-- Answering "what did I spend in August" therefore requires a union that has
-- to be rewritten by hand every time it is asked. This is that union, once.
with purchases as (
    select
        date_trunc('month', purchase_date)::date as spend_month,
        category,
        vendor,
        'one_off'                                as spend_type,
        total_cost
    from {{ ref('stg_purchases') }}
),

charges as (
    select
        date_trunc('month', c.charge_date)::date as spend_month,
        coalesce(rp.category, 'uncategorised')   as category,
        c.vendor,
        'recurring'                              as spend_type,
        c.total_cost
    from {{ ref('stg_recurring_purchase_charges') }} c
    left join {{ ref('stg_recurring_purchases') }} rp
        on rp.recurring_purchase_id = c.recurring_purchase_id
),

combined as (
    select * from purchases
    union all
    select * from charges
)

select
    spend_month,
    category,
    spend_type,
    count(*)                    as transaction_count,
    round(sum(total_cost), 2)   as total_spend,
    round(avg(total_cost), 2)   as avg_transaction,
    round(max(total_cost), 2)   as largest_transaction
from combined
group by 1, 2, 3
