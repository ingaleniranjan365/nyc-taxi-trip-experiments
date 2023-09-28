import logging
import sys
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, lit, monotonically_increasing_id, hour, dayofweek


def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


def parse_args() -> Tuple[str, str, str, str, str]:
    if len(sys.argv) != 6:
        logging.error(
            "Usage: main.py <ride_hailing_taxi_trips_path> <green_taxi_trips_path> <yellow_taxi_trips_path> <output_path>")
        sys.exit(1)
    ride_hailing_taxi_trips_path = sys.argv[1]
    green_taxi_trips_path = sys.argv[2]
    yellow_taxi_trips_path = sys.argv[3]
    taxi_zone_lookup_path = sys.argv[4]
    output_path = sys.argv[5]
    logging.info("Ride hailing trips path: %s", ride_hailing_taxi_trips_path)
    logging.info("Green taxi trips path: %s", green_taxi_trips_path)
    logging.info("Yellow taxi trips path: %s", yellow_taxi_trips_path)
    logging.info("Taxi zone lookup path: %s", taxi_zone_lookup_path)
    logging.info("Output path: %s", output_path)
    return ride_hailing_taxi_trips_path, green_taxi_trips_path, yellow_taxi_trips_path, taxi_zone_lookup_path, output_path


def get_trip_sources(
        ride_hailing_taxi_trips_path,
        green_taxi_trips_path,
        yellow_taxi_trips_path,
        spark: SparkSession
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    ride_hailing_taxi_trips_df = spark.read.parquet(ride_hailing_taxi_trips_path) \
        .select("pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID") \
        .withColumn("source", lit("ride_hailing"))
    green_taxi_trips_df = spark.read.parquet(green_taxi_trips_path) \
        .select("lpep_pickup_datetime", "lpep_dropoff_datetime", "PULocationID", "DOLocationID") \
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
        .withColumn("source", lit("ride_hailing"))
    yellow_taxi_trips_df = spark.read.parquet(yellow_taxi_trips_path) \
        .select("tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID") \
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
        .withColumn("source", lit("ride_hailing"))
    return ride_hailing_taxi_trips_df, green_taxi_trips_df, yellow_taxi_trips_df


def get_lookups_for_airports(spark: SparkSession, taxi_zone_lookup_path: str) -> DataFrame:
    taxi_zone_lookup = spark.read.csv(taxi_zone_lookup_path, header=True)
    filtered_taxi_zone_lookups = taxi_zone_lookup.filter(
        taxi_zone_lookup["Zone"].isin(["LaGuardia Airport", "JFK Airport"])
    )
    return filtered_taxi_zone_lookups


def consolidate_sources(ride_hailing_taxi_trips_df, green_taxi_trips_df, yellow_taxi_trips_df) -> DataFrame:
    trips = (
        ride_hailing_taxi_trips_df
        .unionAll(green_taxi_trips_df)
        .unionAll(yellow_taxi_trips_df)
    )

    trips = trips.withColumn("id", monotonically_increasing_id())
    return trips


def get_airport_trips(trips, airport_lookups) -> DataFrame:
    airport_pickups = trips.join(
        airport_lookups,
        trips["PULocationID"] == airport_lookups["LocationID"],
        "right"
    ).select("id", "pickup_datetime", "source", "Zone") \
        .withColumnRenamed("pickup_datetime", "impression_datetime") \
        .withColumnRenamed("Zone", "airport")

    airport_dropoffs = trips.join(
        airport_lookups,
        trips["DOLocationID"] == airport_lookups["LocationID"],
        "right"
    ).select("id", "dropoff_datetime", "source", "Zone") \
        .withColumnRenamed("dropoff_datetime", "impression_datetime") \
        .withColumnRenamed("Zone", "airport")

    airport_trips = airport_pickups.unionAll(airport_dropoffs).dropDuplicates(["id"])

    return airport_trips


def aggregated_airport_trips(airport_trips: DataFrame) -> DataFrame:
    airport_trips_hod_and_dow = airport_trips.withColumn("hour_of_day", hour("impression_datetime")) \
        .withColumn("day_of_week", dayofweek("impression_datetime")) \
        .groupBy("airport", "hour_of_day").agg(count("*").alias("trip_cont")) \
        .select("airport", "hour_of_day", "trip_cont") \
        .orderBy("airport", "hour_of_day")
    return airport_trips_hod_and_dow


def main():
    ride_hailing_taxi_trips_path, green_taxi_trips_path, yellow_taxi_trips_path, taxi_zone_lookup_path, output_path = parse_args()
    spark = get_spark_session("nyc-taxi-trip-insights")
    ride_hailing_taxi_trips_df, green_taxi_trips_df, yellow_taxi_trips_df = get_trip_sources(
        ride_hailing_taxi_trips_path,
        green_taxi_trips_path,
        yellow_taxi_trips_path,
        spark
    )
    trips = consolidate_sources(ride_hailing_taxi_trips_df, green_taxi_trips_df, yellow_taxi_trips_df)
    airport_lookups = get_lookups_for_airports(spark, taxi_zone_lookup_path)
    airport_trips = get_airport_trips(trips, airport_lookups)
    airport_trips_hod_and_dow = aggregated_airport_trips(airport_trips)
    airport_trips_hod_and_dow.repartition(1).write.csv(output_path)
    spark.stop()


if __name__ == "__main__":
    main()
