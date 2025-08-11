{{ config(
    materialized='table'
) }}


with source as (
    select * from {{ source('raw', 'sets') }}
),

cleaned as (
    select
        set_num,
        `name`,
        `year`,
        cast(theme_id as string) as theme_id
    from source
)

select * from cleaned
