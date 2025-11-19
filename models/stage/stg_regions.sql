{{ 
    config(
        alias='stage_regions'
    ) 
}}
with region as (
    select 
        R_REGIONKEY as region_id,
        R_NAME as region_name,
        R_COMMENT as region_comment
    from {{ source('src','regions')}}
)

select * from region