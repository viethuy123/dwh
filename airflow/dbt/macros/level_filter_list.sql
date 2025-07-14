{% macro level_filter_list() %}
  {% set filter_list = [
    'JUNIOR',
    'MIDDLE',
    'FRESHER',
    'EXPERT',
    'SENIOR',
    'MANAGER'
] %}
  {{ return(filter_list) }}
{% endmacro %}