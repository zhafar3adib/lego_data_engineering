{{ config(materialized='table') }}

select
    i.inventory_id,
    i.set_num,
    i.quantity,
    s.name as set_name,
    s.year,
    t.name as themes
from {{ ref('inventory_sets') }} i
left join {{ ref('sets') }} s on i.set_num = s.set_num
left join {{ ref('themes') }} t on s.theme_id = t.id
