{% set results_query %}
select distinct s1.path as columns
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



{% set full_query_ = stg_macro('%ID%')  %}

{{ full_query_ }}
