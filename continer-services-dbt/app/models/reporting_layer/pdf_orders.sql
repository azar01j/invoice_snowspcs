{{
    config(
        materialized='incremental',
        on_schema_change='fail',
        unqiue_key='ID'
        )
}}


{% set results_query %}
select 
    distinct columns_
from {{ ref('order_stg') }}
{% endset %}


{% set results = run_query(results_query) %}

{% if execute %}
    {% set results_list = results['columns'][0].values() %}
{% else %}
    {% set results_list = [] %}
{% endif %}


select
    file_name,
    id,
    {% for columns_in in results_list %}
        {% if columns_in != 'Order_Quantity_' %}
        max(case when columns_ = '{{columns_in}}' then COLUMN_VALUE end) as {{columns_in}}
        {% else %}
        sum(case when columns_ = '{{columns_in}}' then COLUMN_VALUE end) as {{columns_in}}
        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('order_stg') }}
group by file_name,id

{% if is_incremental() %}


having ID > (select max(ID) from {{ this }} )

{% endif %}


