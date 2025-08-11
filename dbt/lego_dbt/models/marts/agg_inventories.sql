{{ config(
    materialized='table'
) }}

with fact as (
    select * from {{ ref('fact_inventories') }}
),
parts as (
    select * from {{ ref('dim_parts') }}
),
sets as (
    select * from {{ ref('dim_sets') }}
)

select 
    fi.id, 
    case
      when dp.inventory_id is not null then 'parts'
      when ds.inventory_id is not null then 'sets'
      else 'not_classified'
    end as class,
    case
      when dp.inventory_id is not null then sum(dp.quantity)
      when ds.inventory_id is not null then sum(ds.quantity)
      else 0
    end as quantity
from fact fi
left join parts dp on fi.id = dp.inventory_id
left join sets ds on fi.id = ds.inventory_id
group by fi.id, dp.inventory_id, ds.inventory_id
