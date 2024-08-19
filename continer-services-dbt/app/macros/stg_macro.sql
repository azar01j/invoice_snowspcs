{% macro stg_macro(value) %}

{% if value == '%ID%' %}

select
s1.seq as seq_val,
s1.path,
ip.file_name_ as file_name,
s2.index as index_,
substr(s2.value['value'],0,charindex(':',s2.value['value'])-1) as Detail,
ltrim(substr(s2.value['value'],charindex(':',s2.value['value'])+1,length(s2.value['value'])))::varchar(50) as column_value,
replace(s1.path||'_'||detail,' ','_') as columns_,
substr(file_name, charindex('_', file_name) + 1, charindex('.', file_name) - (charindex('_', file_name) + 1)) as ID,
row_number() over (partition by file_name, s1.path order by index_) as row_num,
from raw_invoice_pdf ip , 
lateral flatten(input=>ip.json_content) s1,
lateral flatten(input=> s1.value) s2
where s1.path like '{{ value }}' or s1.path like '%Quant%'


{% else %}

select
s1.seq as seq_val,
s1.path,
ip.file_name_ as file_name,
s2.index as index_,
substr(s2.value['value'],0,charindex(':',s2.value['value'])-1) as Detail,
ltrim(substr(s2.value['value'],charindex(':',s2.value['value'])+1,length(s2.value['value'])))::varchar(50) as column_value,
replace(s1.path||'_'||detail,' ','_') as columns_,
substr(file_name, charindex('_', file_name) + 1, charindex('.', file_name) - (charindex('_', file_name) + 1)) as ID,
row_number() over (partition by file_name, s1.path order by index_) as row_num,
from raw_invoice_pdf ip , 
lateral flatten(input=>ip.json_content) s1,
lateral flatten(input=> s1.value) s2
where s1.path like '{{ value }}'

{% endif %}


{% endmacro %}