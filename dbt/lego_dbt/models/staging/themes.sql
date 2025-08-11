{{ config(
    materialized='table'
) }}



with source as (
    select * from {{ source('raw', 'themes') }}
),

cleaned as (
    select
        cast(id as string) as id,
        `name`
    from source
)

select * from cleaned
