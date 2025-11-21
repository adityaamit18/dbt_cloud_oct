with customers as (
    select *
    from {{ ref('stg_customers') }}
),

nations as (
    select *
    from {{ ref('stg_nations') }}
),

regions as (
    select *
    from {{ ref('stg_regions') }}
)

select c.*,
n.name as nation_name,
n.updated_at,
r.region_id,
r.region_name as region_name,
r.region_comment as region_comment
from customers c
join nations n
    on c.nation_id = n.nation_id
join regions r
    on n.region_id = r.region_id