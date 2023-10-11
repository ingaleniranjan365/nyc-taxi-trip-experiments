with ride_hailing_taxi_trips as (
  select
    pickup_datetime,
    dropoff_datetime,
    PULocationID,
    DOLocationID,
    'ride_hailing' as source
  from {{ source('nyc_tlc_raw_taxi_trips', 'ride_hailing_services_taxi_trips') }}
),

green_taxi_trips as (
  select
    lpep_pickup_datetime as pickup_datetime,
    lpep_dropoff_datetime as dropoff_datetime,
    PULocationID,
    DOLocationID,
    'green' as source
  from {{ source('nyc_tlc_raw_taxi_trips', 'green_taxi_trips') }}
),

yellow_taxi_trips as (
  select
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    PULocationID,
    DOLocationID,
    'yellow' as source
  from {{ source('nyc_tlc_raw_taxi_trips', 'yellow_taxi_trips') }}
),

trips as (
    select * from ride_hailing_taxi_trips
    union all
    select * from green_taxi_trips
    union all
    select * from yellow_taxi_trips
)

select
  *,
  row_number() over () as id
from
  trips
