{{ config(
    materialized='table'
) }}



with source as (
    select * from {{ source('raw', 'inventory_sets') }}
),

cleaned as (
    select
        cast(inventory_id as string) as inventory_id ,
        set_num,
        quantity
    from source
)

select * from cleaned
