{{ config(
    materialized='table'
) }}


with source as (
    select * from {{ source('raw', 'inventory_parts') }}
),

cleaned as (
    select
        cast(inventory_id as string) as inventory_id ,
        part_num,
        cast(color_id as string) as color_id,
        quantity,
        case 
            when is_spare = 'f' then false
            else true
        end as is_spare
    from source
)

select * from cleaned
