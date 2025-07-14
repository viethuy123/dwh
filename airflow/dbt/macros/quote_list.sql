{% macro quote_list(values) %}
  {{ values | map('tojson') | join(', ') | replace('"', "'") }}
{% endmacro %}