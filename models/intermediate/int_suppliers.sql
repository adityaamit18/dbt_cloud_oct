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
    c.*,
    n.* exclude (nation_id),
    r.* exclude (region_id)
from suppliers c
join nations n
    on c.nation_id = n.nation_id
join regions r
    on n.region_id = r.region_id