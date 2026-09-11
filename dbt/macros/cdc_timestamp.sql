{% macro cdc_timestamp(expression) -%}
{# Debezium time.precision.mode=connect emits timestamp integers in milliseconds.
   Silver can also contain formatted timestamps from batch ingestion. #}
case
    when trim(cast({{ expression }} as string)) rlike '^[+-]?[0-9]+$'
        then timestamp_millis(cast({{ expression }} as bigint))
    else cast({{ expression }} as timestamp)
end
{%- endmacro %}
