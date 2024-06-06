{% macro netezza__array_instantiate(data_type) -%}
    {%- set array_type_mapping = {
        "int1": 1,
        "byteint": 1,
        "int2": 2,
        "smallint": 2,
        "int4": 3,
        "int": 3,
        "integer": 3,
        "int8": 4,
        "bigint": 4,
        "date": 5,
        "time": 6,
        "timestamp": 7,
        "varchar": 8,
        "nvarchar": 9,
        "float": 10,
        "double": 11,
        "timetz": 15
    } -%}
    {%- set type_code = array_type_mapping[data_type|lower] -%}
    {%- if not type_code -%}
        {{ exceptions.raise_compiler_error("Array data_type ('" ~ data_type ~ "') must be a valid type (https://www.ibm.com/docs/en/netezza?topic=functions-array).") }}
    {%- else -%}
        array({{ type_code }})
    {%- endif -%}
{%- endmacro %}

{% macro netezza__array_construct(inputs, data_type) -%}
    {% if inputs|length == 0 -%}
        {{netezza__array_instantiate(data_type)}}
    {%- else -%}
        {{array_append(array_construct(inputs[:-1], data_type), inputs[-1])}}
    {%- endif %}
{%- endmacro %}