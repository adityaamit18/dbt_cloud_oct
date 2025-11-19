{#{{ config(
    schema='new'
) }}#}
with empl as (

select
        {{ transform_employee('employee_first_name', 'employee_last_name', 'employee_gender', 'employee_fixedline', 'employee_age') }},
        employee_id,
        employee_first_name,
        employee_last_name,
        employee_gender,
        employee_fixedline,
        employee_age
from {{ source('src2','stg_employees') }}
)
select * from empl