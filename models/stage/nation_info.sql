with nation_info as (
    SELECT
        n.NATION_ID,
        n.NATION_NAME,
        n.REGION_ID,
        r.REGION_NAME,
        n.NATION_COMMENT,
        r.REGION_COMMENT
    FROM {{ref('stg_nations')}} n JOIN {{ref('stg_regions')}} r ON n.REGION_ID = r.REGION_ID
)
select * from nation_info
