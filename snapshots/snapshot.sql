{% snapshot scd_orders3 %}

{{
  config(
    target_database='Analytics',
    target_schema='scds',
    unique_key='o_orderkey',
    strategy='check',
    check_cols=['o_orderpriority','o_orderdate','o_comment'],
    hard_deletes='new_record'
  )
}}

    select * from {{ source('src','orders')}}
{% endsnapshot %}