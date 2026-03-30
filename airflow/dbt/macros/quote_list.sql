{% macro quote_list(values) %}
  {{ values | map('tojson') | join(', ') | replace('"', "'") }}
{% endmacro %}

{% macro parse_python_json(col) %}
(
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE({{ col }}, '''([^'']*?)''', '"\1"', 'g'),
                '\bTrue\b',  'true',  'g'
            ),
            '\bFalse\b', 'false', 'g'
        ),
        '\bNone\b', 'null', 'g'
    )::JSONB
)
{% endmacro %}