from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, when, col, date_format, round
import numpy as np
import matplotlib.pyplot as plt


def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.executor.memory", "12g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.default.parallelism", 8) \
        .getOrCreate()


def preprocess_dataframe(parquet_file_path: str, spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(parquet_file_path)

    df = df.withColumn("Ride Hailing Service", when(col("hvfhs_license_num") == "HV0003", "Uber")
                       .when(col("hvfhs_license_num") == "HV0005", "Lyft")
                       .otherwise(col("hvfhs_license_num")))

    df = df.withColumn("Trip Date", date_format(col("request_datetime"), "yyyy-MM-dd")) \
        .filter((col("Trip Date") >= "2023-06-01") & (col('Trip Date') <= "2023-06-30"))

    return df


def compute_trip_percentages(df):
    agg_df1 = df.groupBy("Ride Hailing Service", "Trip Date").agg(count("*").alias("Trip Count"))
    agg_df2 = df.groupBy("Trip Date").agg(count("*").alias("Total Trip Count"))

    joined_df = agg_df1.join(agg_df2, "Trip Date", "inner")
    joined_df = joined_df.withColumn("Trip percentage for the day",
                                     round((col("Trip Count") / col("Total Trip Count")) * 100, 2))

    return joined_df


def collect_and_convert_to_numpy(df, service):
    service_df = df.filter(col("Ride Hailing Service") == service).orderBy("Trip Date")
    service_trip_percentages = service_df.select("Trip percentage for the day").rdd.flatMap(lambda x: x).collect()

    service_array = np.array(service_trip_percentages)
    return service_array


def plot_stacked_area_chart(lyft_array, uber_array):
    days = np.arange(1, 31)
    plt.figure(figsize=(12, 6))

    total_array = np.minimum(lyft_array + uber_array, 100)

    plt.fill_between(days, lyft_array, label='Lyft', color='pink', alpha=0.7)
    plt.fill_between(days, lyft_array, total_array, label='Uber', color='blue', alpha=0.7)

    plt.title('Market Share of Ride Hailing Services over the month of June 2023 in NYC (Daily Granularity)')
    plt.xlabel('Day of the Month')
    plt.ylabel('Market Share (%)')
    plt.ylim(0, 100)
    plt.legend(loc='upper left')
    plt.tight_layout()
    plt.show()


def plot_stacked_bar_chart(lyft_array, uber_array):
    days = np.arange(1, 31)
    plt.figure(figsize=(12, 6))
    plt.bar(days, uber_array, label='Uber', color='pink', alpha=0.7)
    plt.bar(days, lyft_array, bottom=uber_array, label='Lyft', color='blue', alpha=0.7)
    plt.title('Market Share of Ride Hailing Services over the month of June 2023 in NYC (Daily Granularity)')
    plt.xlabel('Day of the Month')
    plt.ylabel('Market Share (%)')
    plt.legend(loc='upper left')
    plt.tight_layout()
    plt.show()


def plot_radar_chart(lyft_array, uber_array):
    days = np.arange(1, 31)
    categories = [f"Day {day}" for day in days]
    fig = plt.figure(figsize=(8, 8))
    ax = fig.add_subplot(111, polar=True)

    angles = np.linspace(0, 2 * np.pi, 30, endpoint=False).tolist()

    ax.fill(angles, uber_array, 'b', alpha=0.1)
    ax.set_xticks(angles)
    ax.set_xticklabels(days)

    ax.fill(angles, lyft_array, 'r', alpha=0.1)
    ax.set_yticks([0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
    ax.set_yticklabels([0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
    ax.set_xticklabels(categories)
    ax.set_title("Uber vs. Lyft Market Share Comparison (30 Days)")
    ax.legend(["Uber", "Lyft"], loc='upper right', bbox_to_anchor=(0.1, 0.1))

    plt.show()


def main():
    parquet_file_path = "../../../data/nyc-tlc-raw/fhvhv_tripdata_2023-06.parquet"
    spark = get_spark_session("nyc-taxi-trip-visualisations")

    df = preprocess_dataframe(parquet_file_path, spark)

    joined_df = compute_trip_percentages(df)

    uber_array = collect_and_convert_to_numpy(joined_df, "Uber")
    lyft_array = collect_and_convert_to_numpy(joined_df, "Lyft")

    # plot_stacked_bar_chart(lyft_array, uber_array)
    # plot_radar_chart(lyft_array, uber_array)
    plot_stacked_area_chart(lyft_array, uber_array)

    spark.stop()


if __name__ == "__main__":
    main()
