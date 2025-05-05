-- dbt_project/models/marts/fct_incidents.sql
-- Creates the main fact table by joining staging data with dimension tables.

with stg_incidents as (
    select * from {{ ref('stg_sf_fire_incidents') }}
),

dim_battalion as (
    select * from {{ ref('dim_battalion') }}
),

dim_neighborhood_district as (
    select * from {{ ref('dim_neighborhood_district') }}
),

dim_time as (
    select * from {{ ref('dim_time') }}
)

select
    -- Surrogate Keys from Dimensions
    coalesce(dt.time_key, '-1') as time_key, -- Use -1 or another indicator for missing dimension keys
    coalesce(db.battalion_key, '-1') as battalion_key,
    coalesce(dnd.neighborhood_district_key, '-1') as neighborhood_district_key,

    -- Degenerate Dimensions (unique IDs from the source)
    si.incident_number,
    si.exposure_number,
    si.source_record_id,
    si.call_number,

    -- Measures (Facts)
    si.number_of_alarms,
    coalesce(si.estimated_property_loss, 0) as estimated_property_loss, -- Coalesce null losses to 0
    coalesce(si.estimated_contents_loss, 0) as estimated_contents_loss,
    coalesce(si.fire_fatalities, 0) as fire_fatalities,
    coalesce(si.fire_injuries, 0) as fire_injuries,
    coalesce(si.civilian_fatalities, 0) as civilian_fatalities,
    coalesce(si.civilian_injuries, 0) as civilian_injuries,
    coalesce(si.suppression_units, 0) as suppression_units,
    coalesce(si.suppression_personnel, 0) as suppression_personnel,
    coalesce(si.ems_units, 0) as ems_units,
    coalesce(si.ems_personnel, 0) as ems_personnel,
    coalesce(si.other_units, 0) as other_units,
    coalesce(si.other_personnel, 0) as other_personnel,

    -- Timestamps (can be useful in facts for duration calculations)
    si.alarm_timestamp,
    si.arrival_timestamp,
    si.close_timestamp,

    -- Other potentially relevant attributes (can also be dimensions)
    si.primary_situation,
    si.action_taken_primary,
    si.property_use,
    si.mutual_aid_type,
    si.first_unit_on_scene

    -- Calculate durations if needed
    -- extract(epoch from (si.arrival_timestamp - si.alarm_timestamp)) as response_time_seconds,
    -- extract(epoch from (si.close_timestamp - si.arrival_timestamp)) as time_on_scene_seconds

from stg_incidents si

-- Left join to dimension tables
left join dim_time dt
    on si.incident_date = dt.calendar_date -- Join on the date part

left join dim_battalion db
    on si.battalion_code = db.battalion_code

left join dim_neighborhood_district dnd
    on si.neighborhood_district = dnd.neighborhood_district


-- Optional: Add filter if needed, e.g., exclude records with no date
-- where si.incident_date is not null

-- Add incremental logic based on response_timestamp if optimizing dbt runs
-- {% if is_incremental() %}
-- where si.response_timestamp > (select max(response_timestamp) from {{ this }})
-- {% endif %}
