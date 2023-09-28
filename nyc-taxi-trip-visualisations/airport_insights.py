from typing import Tuple

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, lit, monotonically_increasing_id, hour, dayofweek


def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.executor.memory", "12g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.default.parallelism", 8) \
        .getOrCreate()


def plot_bar_chart(airport_trips_by_hour_of_day: DataFrame):
    pandas_df = airport_trips_by_hour_of_day.toPandas()
    laguardia_df = pandas_df[pandas_df['airport'] == 'LaGuardia Airport']
    jfk_df = pandas_df[pandas_df['airport'] == 'JFK Airport']

    plt.figure(figsize=(12, 6))
    plt.bar(laguardia_df['hour_of_day'], laguardia_df['trip_cont'], width=0.4, label='LaGuardia', align='center')
    plt.bar(jfk_df['hour_of_day'] + 0.4, jfk_df['trip_cont'], width=0.4, label='JFK Airport', align='center')
    plt.xlabel('Hour of Day')
    plt.ylabel('Trip Count')
    plt.title('Trip Count Comparison between LaGuardia and JFK Airport')
    plt.xticks(pandas_df['hour_of_day'])
    plt.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    plt.show()


def get_trip_sources(spark: SparkSession) -> Tuple[DataFrame, DataFrame, DataFrame]:
    ride_hailing_taxi_trips_df = spark.read.parquet("fhvhv_tripdata_2023-06.parquet") \
        .select("pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID") \
        .withColumn("source", lit("ride_hailing"))
    green_taxi_trips_df = spark.read.parquet("green_tripdata_2023-06.parquet") \
        .select("lpep_pickup_datetime", "lpep_dropoff_datetime", "PULocationID", "DOLocationID") \
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
        .withColumn("source", lit("ride_hailing"))
    yellow_taxi_trips_df = spark.read.parquet("yellow_tripdata_2023-06.parquet") \
        .select("tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID") \
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
        .withColumn("source", lit("ride_hailing"))
    return ride_hailing_taxi_trips_df, green_taxi_trips_df, yellow_taxi_trips_df


def consolidate_sources(ride_hailing_taxi_trips_df, green_taxi_trips_df, yellow_taxi_trips_df) -> DataFrame:
    trips = (
        ride_hailing_taxi_trips_df
        .unionAll(green_taxi_trips_df)
        .unionAll(yellow_taxi_trips_df)
    )

    trips = trips.withColumn("id", monotonically_increasing_id())
    return trips


def get_lookups_for_airports(spark: SparkSession) -> DataFrame:
    taxi_zone_lookup = spark.read.csv("taxi+_zone_lookup.csv", header=True)
    filtered_taxi_zone_lookups = taxi_zone_lookup.filter(
        taxi_zone_lookup["Zone"].isin(["LaGuardia Airport", "JFK Airport"])
    )
    return filtered_taxi_zone_lookups


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
    spark = get_spark_session("nyc-taxi-trip-visualisations")
    ride_hailing_taxi_trips_df, green_taxi_trips_df, yellow_taxi_trips_df = get_trip_sources(spark)
    trips = consolidate_sources(ride_hailing_taxi_trips_df, green_taxi_trips_df, yellow_taxi_trips_df)
    airport_lookups = get_lookups_for_airports(spark)
    airport_trips = get_airport_trips(trips, airport_lookups)
    airport_trips_hod_and_dow = aggregated_airport_trips(airport_trips)
    plot_bar_chart(airport_trips_hod_and_dow)
    spark.stop()


if __name__ == "__main__":
    main()
