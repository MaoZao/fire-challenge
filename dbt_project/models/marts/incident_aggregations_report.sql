-- dbt_project/analysis/incident_aggregations_report.sql
-- Example analysis showing how to use the dimensional model.
-- Aggregates incidents by year, month, battalion, and neighborhood district.

-- You can run this using `dbt compile` and then executing the compiled SQL,
-- or using `dbt run-operation run_analysis --args '{analysis_name: incident_aggregations_report}'` (requires macro)
-- Or simply copy/paste the compiled SQL into a SQL client.

select
    -- Time Dimensions
    dt.year,
    dt.year_month,
    dt.month_name,

    -- Location Dimensions
    db.battalion_code,
    dnd.neighborhood_district,

    -- Aggregated Measures
    count(fi.incident_number) as total_incidents,
    sum(fi.number_of_alarms) as total_alarms,
    sum(fi.estimated_property_loss + fi.estimated_contents_loss) as total_estimated_loss,
    sum(fi.fire_fatalities) as total_fire_fatalities,
    sum(fi.fire_injuries) as total_fire_injuries,
    sum(fi.civilian_fatalities) as total_civilian_fatalities,
    sum(fi.civilian_injuries) as total_civilian_injuries,
    avg(fi.suppression_personnel) as avg_suppression_personnel,
    avg(fi.ems_personnel) as avg_ems_personnel

from {{ ref('fct_incidents') }} fi
join {{ ref('dim_time') }} dt on fi.time_key = dt.time_key
join {{ ref('dim_battalion') }} db on fi.battalion_key = db.battalion_key
join {{ ref('dim_neighborhood_district') }} dnd on fi.neighborhood_district_key = dnd.neighborhood_district_key
-- Optionally join other dimensions like dim_analysis_neighborhood

-- Add filters if needed, e.g., for a specific year or battalion
-- where dt.year = 2023
-- and db.battalion_code = 'B05'

group by
    dt.year,
    dt.year_month,
    dt.month_name,
    db.battalion_code,
    dnd.neighborhood_district
order by
    dt.year_month desc, -- Show recent months first
    db.battalion_code,
    dnd.neighborhood_district,
    total_incidents desc -- Show highest incident counts first within groups
