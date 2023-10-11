with airport_trips as (
    select * from {{ ref('airport_trips') }}
),

airport_trips_by_hod AS (
  select
    airport,
    hour_of_day,
    count(*) AS trip_count
  FROM
    airport_trips
  GROUP BY
    airport, hour_of_day
  ORDER BY
    airport, hour_of_day
)

select * from airport_trips_by_hod