-- dbt_project/models/marts/dim_neighborhood_district.sql
-- Creates a dimension table for neighborhood districts.

with districts as (
    select distinct
        neighborhood_district
    from {{ ref('stg_sf_fire_incidents') }}
    where neighborhood_district is not null -- Exclude nulls
)

select
    {{ dbt_utils.generate_surrogate_key(['neighborhood_district']) }} as neighborhood_district_key,
    neighborhood_district
from districts
order by neighborhood_district
