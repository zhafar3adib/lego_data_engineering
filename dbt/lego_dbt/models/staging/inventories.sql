{{ config(
    materialized='table'
) }}

with source as (
    select * from {{ source('raw', 'inventories') }}
),

cleaned as (
    select
        cast(id as string) as id ,
        set_num
    from source
)

select * from cleaned
