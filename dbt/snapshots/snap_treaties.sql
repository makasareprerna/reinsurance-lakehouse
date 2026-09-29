{#
  SCD Type 2 history of treaties. Every endorsement (for example a premium
  change) closes the old version (dbt_valid_to) and opens a new one.
  Answers "what did this treaty look like on date X?"
#}
{% snapshot snap_treaties %}
{{
    config(
        target_schema='snapshots',
        unique_key='treaty_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}
select * from {{ ref('stg_treaties') }}
{% endsnapshot %}
