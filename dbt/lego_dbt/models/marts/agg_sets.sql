{{ config(
    materialized='table'
) }}

with fact as (
    select * from {{ ref('fact_inventories') }}
),
sets as (
    select * from {{ ref('dim_sets') }}
)

select 
    fi.id, 
    ds.set_name, 
    ds.year, 
    ds.themes, 
    sum(ds.quantity) as qty_sets
from fact fi
left join sets ds on fi.id = ds.inventory_id
where ds.inventory_id is not null
group by 1, 2, 3, 4
