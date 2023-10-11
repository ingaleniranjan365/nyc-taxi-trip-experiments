{% macro haversine_distance(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon) %}
    (3959 * ACOS(
        GREATEST(-1, LEAST(1, (
            COS(3.14159265359 * {{ pickup_lat }}/180.0) * COS(3.14159265359 * {{ dropoff_lat }}/180.0) *
            COS(3.14159265359 * ({{ dropoff_lon }} - {{ pickup_lon }})/180.0) +
            SIN(3.14159265359 * {{ pickup_lat }}/180.0) * SIN(3.14159265359 * {{ dropoff_lat }}/180.0)
        )))
    )) AS distance
{% endmacro %}
