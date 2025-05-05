-- dbt_project/models/staging/stg_sf_fire_incidents.sql
-- Selects from the raw source table, renames columns, casts types, and performs basic cleaning.

with source_data AS (
    -- Select from the source defined in sources.yml
    select *
    from {{ source('sf_fire_raw', env_var('STAGING_TABLE_NAME')) }} -- Use env_var to get table name
)

select
       -- IDs and Keys
    incident_number::text as incident_number, -- Ensure text type for consistency
    exposure_number::integer as exposure_number,
    id::text as source_record_id, -- Original ID from source
    call_number::text as call_number,

    -- Timestamps and Dates (cast to appropriate types)
    -- Use safe_cast macro (or try_cast in native SQL) for robustness
    -- Assuming dates/timestamps were loaded as text in ISO 8601 format
    incident_date::date as incident_date, -- Keep original for reference if needed
    alarm_dttm::timestamp as alarm_timestamp,
    arrival_dttm::timestamp as arrival_timestamp,
    close_dttm::timestamp as close_timestamp,

    -- Location Information
    address::text as address,
    city::text as city,
    zipcode::text as zipcode,
    battalion::text as battalion_code, -- Rename for clarity
    station_area::text as station_area,
    box::text as box_area,
    supervisor_district::text as supervisor_district,
    neighborhood_district::text as neighborhood_district, -- Dimension candidate
    point::text as location_point, -- Keep as text for now, potentially parse later

    -- Incident Details
    primary_situation::text as primary_situation,
    number_of_alarms::integer as number_of_alarms,
    action_taken_primary::text as action_taken_primary,
    property_use::text as property_use,
    mutual_aid::text as mutual_aid_type, -- Rename

    -- Impact and Loss
    estimated_property_loss::float as estimated_property_loss,
    estimated_contents_loss::float as estimated_contents_loss,
    fire_fatalities::integer as fire_fatalities,
    fire_injuries::integer as fire_injuries,
    civilian_fatalities::integer as civilian_fatalities,
    civilian_injuries::integer as civilian_injuries,

    -- Response Units and Personnel
    suppression_units::integer as suppression_units,
    suppression_personnel::integer as suppression_personnel,
    ems_units::integer as ems_units,
    ems_personnel::integer as ems_personnel,
    other_units::integer as other_units,
    other_personnel::integer as other_personnel,
    first_unit_on_scene::text as first_unit_on_scene,

    -- Fire Details (if applicable)
    area_of_fire_origin::text as area_of_fire_origin,
    ignition_cause::text as ignition_cause,
    fire_spread::text as fire_spread,
    structure_type::text as structure_type,

    -- Detector and Sprinkler Info
    detectors_present::text as detectors_present,
    detector_type::text as detector_type,
    detector_operation::text as detector_operation,
    automatic_extinguishing_system_present::text as extinguishing_system_present-- Rename


from source_data

