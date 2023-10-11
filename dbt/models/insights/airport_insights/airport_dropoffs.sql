with taxi_zones_lookup_table as (
  select
    *
  from {{ source('nyc_tlc_raw_taxi_trips', 'taxi_zones_lookup_table') }}
),

trips as (
    select
        *
    from {{ ref('trips') }}
),

airport_lookups as (
  select
    *
  from taxi_zones_lookup_table t
  where t.Zone in ("LaGuardia Airport", "JFK Airport")
),

airport_dropoffs AS (
  select
    t.id,
    t.dropoff_datetime as impression_datetime,
    t.source,
    a.Zone as airport
  from
    trips t
  right join
    airport_lookups a
  on
    t.DOLocationID = a.LocationID
)

select
    *,
    extract(hour from impression_datetime) as hour_of_day,
    extract(DAYOFWEEK from impression_datetime) AS day_of_week
from airport_dropoffs