{{ config(materialized='table') }}

select
    id,
    set_num
from {{ ref('inventories') }}
