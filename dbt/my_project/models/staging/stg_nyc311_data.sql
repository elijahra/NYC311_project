{{config(materialized='table'
        unique_key='id'
)}}

with raw_data as (

    select * from {{ ref('nyc311_raw') }}

), cleaned_data as 