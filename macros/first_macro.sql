{% macro jodo(col1, col2) %}
    {{col1}} || ' ' || {{col2}}
{% endmacro %}

{% macro mod_tab(table_name) %}
    {% set sql %}
        ALTER TABLE {{ table_name }}
        ADD COLUMN IF NOT EXISTS updated_as TIMESTAMP;

        UPDATE {{ table_name }}
        SET updated_as = CURRENT_TIMESTAMP();
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}

{% macro show_emps() %}
{%   do run_query("select employee_name from {{ ref('stg_employees') }}")%}
{% endmacro %}

{% macro unload() %}
{% do run_query("create or replace stage stage_analytics") %}
{% do run_query("copy into @stage_analytics from stg_nations partition by (region_id) 
file_format=(type=csv compression = none null_if=(' ')) header=true; ") %}
{% endmacro %}


{% macro emp_trans(first, last, gender, phone, age) %}
(
    select
        -- full name
        {{ first }} || ' ' || {{ last }} as name,
        
        -- gender mapping
        case upper({{ gender }})
            when 'F' then 'Female'
            when 'M' then 'Male'
            else 'Other'
        end as gender,
        
        -- formatted phone
        '(' || substring({{ phone }},1,3) || ') ' ||
        substring({{ phone }},4,3) || '-' ||
        substring({{ phone }},7,4) as phone,
        
        -- age group
        case
            when {{ age }} < 30 then 'Youngest'
            when {{ age }} between 30 and 60 then 'Middle'
            else 'Senior'
        end as agegroup
)
{% endmacro %}

{% macro full_name(first, last) %}
    {{ first }} || ' ' || {{ last }}
{% endmacro %}

{% macro gender_label(gender) %}
    case upper({{ gender }})
        when 'F' then 'Female'
        when 'M' then 'Male'
        else 'Other'
    end
{% endmacro %}

{% macro formatted_phone(phone) %}
    '(' || substring({{ phone }},1,3) || ') ' ||
    substring({{ phone }},4,3) || '-' ||
    substring({{ phone }},7,4)
{% endmacro %}

{% macro age_group(age) %}
    case
        when {{ age }} < 30 then 'Youngest'
        when {{ age }} between 30 and 60 then 'Middle'
        else 'Senior'
    end
{% endmacro %}


{% macro transform_employee(first_name, last_name, gender, phone, age) %}
    {{ first_name }} || ' ' || {{ last_name }} as employee_name,
    case upper({{ gender }})
        when 'F' then 'Female'
        when 'M' then 'Male'
        else 'Other'
    end as gender_label,
    '(' || substring({{ phone }},1,3) || ') ' ||
    substring({{ phone }},4,3) || '-' ||
    substring({{ phone }},7,4) as formatted_phone,
    case
        when {{ age }} < 30 then 'Youngest'
        when {{ age }} between 30 and 60 then 'Middle'
        else 'Senior'
    end as age_group
{% endmacro %}

{% macro generate_schema_name (custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name }}
    {%- endif -%}  
{%- endmacro %}

{% macro unload_stage() %}
   {% do run_query('copy into @mkmotors_dev.staging.s3customers/customers.csv from stg_customers header=true ;') %}
{% endmacro %}






{% macro unload1() %}
{% do run_query("create or replace stage stage_analytics") %}
{% do run_query("copy into @stage_analytics from stg_nations partition by (region_id) 
file_format=(type=csv compression = none null_if=(' ')) header=true; ") %}
{% endmacro %}


{% macro emp_trans1(first, last, gender, phone, age) %}
(
    select
        -- full name
        {{ first }} || ' ' || {{ last }} as name,
        
        -- gender mapping
        case upper({{ gender }})
            when 'F' then 'Female'
            when 'M' then 'Male'
            else 'Other'
        end as gender,
        
        -- formatted phone
        '(' || substring({{ phone }},1,3) || ') ' ||
        substring({{ phone }},4,3) || '-' ||
        substring({{ phone }},7,4) as phone,
        
        -- age group
        case
            when {{ age }} < 30 then 'Youngest'
            when {{ age }} between 30 and 60 then 'Middle'
            else 'Senior'
        end as agegroup
)
{% endmacro %}

{% macro full_name1(first, last) %}
    {{ first }} || ' ' || {{ last }}
{% endmacro %}

{% macro gender_label1(gender) %}
    case upper({{ gender }})
        when 'F' then 'Female'
        when 'M' then 'Male'
        else 'Other'
    end
{% endmacro %}

{% macro formatted_phone1(phone) %}
    '(' || substring({{ phone }},1,3) || ') ' ||
    substring({{ phone }},4,3) || '-' ||
    substring({{ phone }},7,4)
{% endmacro %}

{% macro age_group1(age) %}
    case
        when {{ age }} < 30 then 'Youngest'
        when {{ age }} between 30 and 60 then 'Middle'
        else 'Senior'
    end
{% endmacro %}


{% macro transform_employee1(first_name, last_name, gender, phone, age) %}
    {{ first_name }} || ' ' || {{ last_name }} as employee_name,
    case upper({{ gender }})
        when 'F' then 'Female'
        when 'M' then 'Male'
        else 'Other'
    end as gender_label,
    '(' || substring({{ phone }},1,3) || ') ' ||
    substring({{ phone }},4,3) || '-' ||
    substring({{ phone }},7,4) as formatted_phone,
    case
        when {{ age }} < 30 then 'Youngest'
        when {{ age }} between 30 and 60 then 'Middle'
        else 'Senior'
    end as age_group
{% endmacro %}

{% macro generate_schema_name1 (custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name }}
    {%- endif -%}  
{%- endmacro %}

{% macro unload_stage1() %}
   {% do run_query('copy into @mkmotors_dev.staging.s3customers/customers.csv from stg_customers header=true ;') %}
{% endmacro %}