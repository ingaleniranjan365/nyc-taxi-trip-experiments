with airport_pickups as (
    select * from {{ ref('airport_pickups') }}
),

airport_pickups_by_hod AS (
  select
    airport,
    hour_of_day,
    count(*) AS trip_count
  FROM
    airport_pickups
  GROUP BY
    airport, hour_of_day
  ORDER BY
    airport, hour_of_day
)

select * from airport_pickups_by_hod