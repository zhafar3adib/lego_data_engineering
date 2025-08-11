{{ config(materialized='table') }}

select
    p1.inventory_id,
    p1.part_num,
    case when p2.name is null then 'Unknown' else p2.name end as part_name,
    case when pc.name is null then 'Unknown' else pc.name end as category_name,
    p1.quantity,
    p1.is_spare,
    c.name as color_name,
    c.is_trans
from {{ ref('inventory_parts') }} p1
left join {{ ref('colors') }} c on p1.color_id = c.id
left join {{ ref('parts') }} p2 on p1.part_num = p2.part_num
left join {{ ref('part_categories') }} pc on p2.part_cat_id = pc.id
