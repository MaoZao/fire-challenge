-- dbt_project/models/marts/dim_battalion.sql
-- Creates a dimension table for fire battalions.

with battalions as (
    select distinct
        battalion_code
    from {{ ref('stg_sf_fire_incidents') }}
    where battalion_code is not null -- Exclude nulls from the dimension
)

select
    {{ dbt_utils.generate_surrogate_key(['battalion_code']) }} as battalion_key, -- Generate a surrogate key
    battalion_code
    -- Add any other battalion-related attributes if available/needed in the future
    -- For example: battalion_chief_name, battalion_station_address etc. (if data existed)
from battalions
order by battalion_code -- Optional: order for consistency
