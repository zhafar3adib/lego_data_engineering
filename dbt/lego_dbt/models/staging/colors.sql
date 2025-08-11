{{ config(
    materialized='table'
) }}

with source as (
    select * from {{ source('raw', 'colors') }}
),

cleaned as (
    select
        cast(id as string) as id ,
        `name`,
        rgb,
        case 
            when is_trans = 'f' then false
            else true
        end as is_trans
    from source
)

select * from cleaned
