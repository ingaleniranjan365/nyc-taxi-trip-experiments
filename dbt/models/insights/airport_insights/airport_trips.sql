with airport_pickups AS (
  select * from {{ ref('airport_pickups') }}
),

airport_dropoffs AS (
  select * from {{ ref('airport_dropoffs') }}
),

airport_trips AS (
    select * from airport_pickups
    union all
    select * from airport_dropoffs
)

select * from airport_trips