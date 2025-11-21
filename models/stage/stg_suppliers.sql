{{ config(
    materialized = 'ephemeral'
) }}
with sup as (        
    select      
        -- ids
        c.s_suppkey as supplier_id,
        c.s_nationkey as nation_id,

        -- descriptions
        s.sname as supplier_name,
        c.s_address as supplier_address,
        c.s_phone as phone_number,
        c.s_comment as comment,

        -- amounts
        c.s_acctbal as account_balance,
        c.updated_time
    from {{source('src','suppliers')}} as c join {{ref("suppliers")}} as s 
    on c.s_suppkey=s.skey

)
select * from sup