{{ config(
    tags = 'sample'
) }}
with suppliers as (
    select *
    from {{ ref('stg_suppliers') }}
),

nations as (
    select *
    from {{ ref('stg_nations') }}
),

regions as (
    select *
    from {{ ref('stg_regions') }}
)

select
    c.* exclude (nation_id),
    n.name as nation_name,
    r.region_name as region_name
from suppliers c
join nations n
    on c.nation_id = n.nation_id
join regions r
    on n.region_id = r.region_id