{% macro flatten_json(model_name, json_column) %}
  
{% set survey_methods_query %}
  SELECT DISTINCT(jsonb_object_keys({{json_column}})) 
  AS column_name
  FROM {{model_name}}
{% endset %}

{% set results = run_query(survey_methods_query) %}

{% if execute %}
  {# Return the first column #}
  {% set results_list = results.columns[0].values() %}
{% else %}
  {% set results_list = [] %}
{% endif %}

SELECT
  _airbyte_ab_id,
  {% for column_name in results_list %}
    {{ json_column }}->>'{{ column_name }}' AS {{ column_name | replace('/', '_') | replace('-', '_') }} 
    {% if not loop.last %},{% endif %}
  {% endfor %}
FROM {{model_name}}

{% endmacro %}