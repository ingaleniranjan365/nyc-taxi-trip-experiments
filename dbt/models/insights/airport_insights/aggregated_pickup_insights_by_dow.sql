with airport_pickups as (
    select * from {{ ref('airport_pickups') }}
),

airport_pickups_by_dow AS (
  select
    airport,
    day_of_week,
    count(*) AS trip_count
  FROM
    airport_pickups
  GROUP BY
    airport, day_of_week
  ORDER BY
    airport, day_of_week
)

select * from airport_pickups_by_dow