{{ config(
    materialized = 'dynamic_table',
    target_lag = '10 minutes',
    snowflake_warehouse = 'TRANSFORM_WH'
) }}
with dy as (
select 
c.customer_id as customer_id, 
c.name as customer_name,
c.nation_id as nation_name,
max(o.order_date) as last_ordered_at,
min(o.order_date) as first_ordered_at,
count(o.order_id) as lifetime_orders,
sum(o.total_price) as lifetime_total,
sum(l.extended_price * l.tax_rate) as total_tax,
CASE 
        WHEN COUNT(o.order_id) = 1 THEN 'New Customer'
        ELSE 'Returning Customer'
    END AS cust_type
from {{ ref('stg_customers') }} c
join {{ ref('stg_orders') }} o
on c.customer_id=o.customer_id
join {{ ref('stg_line_items') }} l
on o.order_id=l.order_id
group by c.customer_id,c.name,c.nation_id

    )
select * from dy