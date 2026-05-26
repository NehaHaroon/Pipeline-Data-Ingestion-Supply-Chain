{% macro read_parquet(filename) %}
  read_parquet('{{ var("semantic_parquet_dir") }}/{{ filename }}.parquet')
{% endmacro %}
