{{ config(
    materialized='table'
) }}



with source as (
    select * from {{ source('raw', 'parts') }}
),

cleaned as (
    select
        part_num,
        `name`,
        cast(part_cat_id as string) as part_cat_id
    from source
)

select * from cleaned
