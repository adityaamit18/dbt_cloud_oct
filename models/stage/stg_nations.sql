{#{ 
    config(
        query_tag='test1'
    ) 
}#}
{{ 
    config(
        alias= this.name+ var('v_id'),
        access='public'
    ) 
}}
with nation as (
    select 
        -- n_nationkey::varchar as nation_id,
        n_nationkey as nation_id,
        n_name as name,
        n_regionkey as region_id,
        -- n_comment as comment,
        {#{ jodo('n_name', 'n_comment') }#} --as jodo_col,
        updated_at,
        -- '{{ invocation_id }}' as invocation_id
        {{ dbt_meta() }}
    from {{ source('src','nations')}}
)

select * from nation
