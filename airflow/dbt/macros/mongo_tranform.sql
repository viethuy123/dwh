{% macro safe_parse_timestamp(col) -%}
case
  when ({{ col }}) is null 
    or trim(({{ col }})::text) = '' 
    or lower(trim(({{ col }})::text)) ~ 'invalid|null|none|n/a'
    then null
  
  -- Chỉ parse khi match đúng format
  when ({{ col }})::text ~ '^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}$' 
    then (
      -- Wrap trong subquery để catch error
      case 
        when TO_TIMESTAMP(trim(({{ col }})::text), 'YYYY-MM-DD HH24:MI:SS') is not null
        then TO_TIMESTAMP(trim(({{ col }})::text), 'YYYY-MM-DD HH24:MI:SS')
        else null
      end
    )
  
  else null
end
{%- endmacro %}

{% macro safe_parse_multiple_dates(col) -%}
case
  -- 1. Xử lý giá trị NULL, Rỗng, Chuỗi không hợp lệ
  when ({{ col }}) is null 
    or trim(({{ col }})::text) = '' 
    or lower(trim(({{ col }})::text)) ~ 'invalid|null|none|n/a'
    then null
  
  -- 2. THỬ ĐỊNH DẠNG DD/MM/YYYY (Regex: d{2}/d{2}/d{4})
  when ({{ col }})::text ~ '^\d{2}/\d{2}/\d{4}$'
    then (
      case 
        when TO_DATE(trim(({{ col }})::text), 'DD/MM/YYYY') is not null
        then TO_DATE(trim(({{ col }})::text), 'DD/MM/YYYY')
        else null
      end
    )
  
  -- 3. THỬ ĐỊNH DẠNG YYYY-MM-DD (Regex: d{4}-d{2}-d{2})
  when ({{ col }})::text ~ '^\d{4}-\d{2}-\d{2}$' 
    then (
      case 
        when TO_DATE(trim(({{ col }})::text), 'YYYY-MM-DD') is not null
        then TO_DATE(trim(({{ col }})::text), 'YYYY-MM-DD')
        else null
      end
    )
  
  -- 4. Các trường hợp khác
  else null
end
{%- endmacro %}

{% macro normalize_phone(col) -%}
/*
  Usage:
    {{ normalize_phone('raw.mobile') }}
  Output: SQL expression that returns digits-only phone or NULL if empty
*/
nullif(
  regexp_replace(coalesce({{ col }}::text, ''), '\D', '', 'g'),
  ''
)
{%- endmacro %}

{% macro normalize_boolean(col) -%}
/*
  Usage:
    {{ normalize_boolean('raw.isDeleted') }}
  Returns: true/false/null normalized from many textual patterns
*/
case
  when lower(trim(coalesce({{ col }}::text, ''))) in ('true','t','1','yes','y') then true
  when lower(trim(coalesce({{ col }}::text, ''))) in ('false','f','0','no','n') then false
  else null
end
{%- endmacro %}


