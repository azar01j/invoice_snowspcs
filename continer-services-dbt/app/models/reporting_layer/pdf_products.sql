{% set results_query %}
select 
    distinct columns_
from {{ ref('product_stg') }}
{% endset %}

{% set results = run_query(results_query) %}

{% if execute %}
    {% set results_list = results['columns'][0].values() %}
{% else %}
    {% set results_list = [] %}
{% endif %}

select
distinct    {% for columns_in in results_list %}
        max(case when columns_ = '{{columns_in}}' then column_value end) as {{columns_in}}
        {% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('product_stg') }}

group by id,row_num
