import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, DataFrame


def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.executor.memory", "12g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.default.parallelism", 8) \
        .getOrCreate()


def plot_bar_chart(title: str, airport_trips_by_hour_of_day: DataFrame, aggregation_column: str, X_label: str,
                   Y_label: str):
    pandas_df = airport_trips_by_hour_of_day.toPandas()
    laguardia_df = pandas_df[pandas_df['airport'] == 'LaGuardia Airport']
    jfk_df = pandas_df[pandas_df['airport'] == 'JFK Airport']

    plt.figure(figsize=(12, 6))
    plt.bar(laguardia_df[aggregation_column], laguardia_df['trip_count'], width=0.4, label='LaGuardia', align='center')
    plt.bar(jfk_df[aggregation_column] + 0.4, jfk_df['trip_count'], width=0.4, label='JFK Airport', align='center')
    plt.xlabel(X_label)
    plt.ylabel(Y_label)
    plt.title(title)
    plt.xticks(pandas_df[aggregation_column])
    plt.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    plt.show()


def plot_aggregated_airport_trips_charts(spark: SparkSession):
    aggregated_trips_by_hod = 'data/insights/airport-insights/trips/by_hour_of_day'
    aggregated_trips_by_hod = spark.read.csv(aggregated_trips_by_hod, header=True, inferSchema=True)
    aggregated_trips_by_dow = 'data/insights/airport-insights/trips/by_day_of_week'
    aggregated_trips_by_dow = spark.read.csv(aggregated_trips_by_dow, header=True, inferSchema=True)
    plot_bar_chart('Taxi trip Count Comparison between LaGuardia and JFK Airport for June 2023',
                   aggregated_trips_by_hod, 'hour_of_day', 'Hour Of Day', 'Trip Count')
    plot_bar_chart('Taxi trip Count Comparison between LaGuardia and JFK Airport for June 2023',
                   aggregated_trips_by_dow, 'day_of_week', 'Day Of Week', 'Trip Count')


def plot_aggregated_airport_pickups_charts(spark: SparkSession):
    aggregated_pickups_by_hod = 'data/insights/airport-insights/pickups/by_hour_of_day'
    aggregated_pickups_by_hod = spark.read.csv(aggregated_pickups_by_hod, header=True, inferSchema=True)
    aggregated_pickups_by_dow = 'data/insights/airport-insights/pickups/by_day_of_week'
    aggregated_pickups_by_dow = spark.read.csv(aggregated_pickups_by_dow, header=True, inferSchema=True)
    plot_bar_chart('Taxi trip Count Comparison between LaGuardia and JFK Airport for June 2023',
                   aggregated_pickups_by_hod, 'hour_of_day', 'Hour Of Day', 'Trip Count')
    plot_bar_chart('Taxi trip Count Comparison between LaGuardia and JFK Airport for June 2023',
                   aggregated_pickups_by_dow, 'day_of_week', 'Day Of Week', 'Trip Count')


def plot_aggregated_airport_dropoffs_charts(spark: SparkSession):
    aggregated_dropoffs_by_hod = 'data/insights/airport-insights/dropoffs/by_hour_of_day'
    aggregated_dropoffs_by_hod = spark.read.csv(aggregated_dropoffs_by_hod, header=True, inferSchema=True)
    aggregated_dropoffs_by_dow = 'data/insights/airport-insights/dropoffs/by_day_of_week'
    aggregated_dropoffs_by_dow = spark.read.csv(aggregated_dropoffs_by_dow, header=True, inferSchema=True)
    plot_bar_chart('Taxi trip Count Comparison between LaGuardia and JFK Airport for June 2023',
                   aggregated_dropoffs_by_hod, 'hour_of_day', 'Hour Of Day', 'Trip Count')
    plot_bar_chart('Taxi trip Count Comparison between LaGuardia and JFK Airport for June 2023',
                   aggregated_dropoffs_by_dow, 'day_of_week', 'Day Of Week', 'Trip Count')


def main():
    spark = get_spark_session("nyc-taxi-trip-visualisations")
    plot_aggregated_airport_pickups_charts(spark)
    plot_aggregated_airport_dropoffs_charts(spark)
    plot_aggregated_airport_trips_charts(spark)
    spark.stop()


if __name__ == "__main__":
    main()
