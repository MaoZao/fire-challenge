-- dbt_project/models/marts/dim_time.sql
-- Creates a time dimension based on the incident date.
-- This creates one row per day found in the incident data.
-- For more comprehensive time dimensions, consider using dbt_date package or generating a full calendar.

with date_spine as (
    -- Get the distinct dates from the incident data
    select distinct incident_date::date as calendar_date
    from {{ ref('stg_sf_fire_incidents') }}
    where incident_date is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['calendar_date']) }} as time_key,
    calendar_date,
    extract(year from calendar_date) as year,
    extract(month from calendar_date) as month,
    extract(day from calendar_date) as day,
    extract(quarter from calendar_date) as quarter,
    extract(week from calendar_date) as week_of_year,
    extract(dow from calendar_date) as day_of_week, -- 0 = Sunday, 6 = Saturday
    extract(doy from calendar_date) as day_of_year,
    to_char(calendar_date, 'YYYY-MM') as year_month,
    to_char(calendar_date, 'Month') as month_name,
    to_char(calendar_date, 'Day') as day_name,
    case
        when extract(dow from calendar_date) in (0, 6) then true
        else false
    end as is_weekend
from date_spine
order by calendar_date
