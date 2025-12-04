{% macro generate_sk(columns) %}
    
    {%- set separator = '|||' -%}
    
    {{ hash_function() }}(
        CONCAT(
            {% for col in columns -%}
                COALESCE(CAST({{ col }} AS {{ type_string() }}), 'NULL_VALUE')
                {%- if not loop.last -%}
                    , '{{ separator }}', 
                {%- endif -%}
            {% endfor %}
        )
    )

{% endmacro %}