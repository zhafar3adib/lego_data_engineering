{{ config(
    materialized='table'
) }}

with fact as (
    select * from {{ ref('fact_inventories') }}
),
parts as (
    select * from {{ ref('dim_parts') }}
)

select 
    fi.id, 
    dp.category_name, 
    dp.is_spare, 
    dp.color_name, 
    dp.is_trans, 
    sum(dp.quantity) as qty_parts
from fact fi
left join parts dp on fi.id = dp.inventory_id
where dp.inventory_id is not null
group by 1, 2, 3, 4, 5
