{{
    config(
        materialized='incremental',
        on_schema_change='fail',
        unqiue_key='ID'
        )
}}

{% set results_query %}
select distinct s1.path||'_' as columns
from raw_invoice_pdf ip , 
lateral flatten(input=>ip.json_content) s1,
lateral flatten(input=> s1.value) s2 where
s1.path like '%ID%' or s1.path like '%Quan%'

{% endset %}

{% set results = run_query(results_query) %}

{% if execute %}
    {% set column_names_prod = results['columns'][0].values() %}
{% else %}
    {% set column_names_prod = [] %}
{% endif %}

with ord_det as (
select 
id,
row_num,
{% for columns_in in column_names_prod %}
        max(case when columns_ = '{{columns_in}}' then column_value end) as {{columns_in}}
        {% if not loop.last %},{% endif %}
    {% endfor %}
from 
{{ ref('order_detail_stg') }}
group by ID,row_num
)

select 
id,
nvl(customer_id_,first_value(customer_id_) over (partition by id order by id,row_num)) as customer_id, 
nvl(order_id_,first_value(order_id_) over (partition by id order by id,row_num)) as order_id_,
Order_Quantity_,
Product_ID_
from ord_det

{% if is_incremental() %}


having ID > (select max(ID) from pdf_order_details )

{% endif %}

order by id,row_num