import logging
import sys
from typing import List, Tuple

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, GBTRegressionModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, radians, sin, cos, acos, lit, round, hour, dayofmonth, month, when, avg
from pyspark.sql.types import IntegerType, FloatType

logging.basicConfig(level=logging.INFO)


def parse_args() -> Tuple[str, str, str]:
    if len(sys.argv) != 4:
        logging.error("Usage: main.py <training_trips_path> <testing_trips_path> <output_path>")
        sys.exit(1)
    training_trips_path = sys.argv[1]
    testing_trips_path = sys.argv[2]
    output_path = sys.argv[3]
    logging.info("Training Trips Path: %s", training_trips_path)
    logging.info("Testing Trips Path: %s", testing_trips_path)
    logging.info("Output Path: %s", output_path)
    return training_trips_path, testing_trips_path, output_path


def apply_schema(df: DataFrame) -> DataFrame:
    if 'trip_duration' in df.columns:
        df = df.withColumn('trip_duration', col('trip_duration').cast(IntegerType()))
    return df.withColumn("vendor_id", col("vendor_id").cast(IntegerType())) \
        .withColumn("passenger_count", col("passenger_count").cast(IntegerType())) \
        .withColumn("store_and_fwd_flag", col("store_and_fwd_flag").cast(IntegerType())) \
        .withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("hour", col("hour").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("distance", col("distance").cast(FloatType()))


def add_haversine_distance(df, lat1, long1, lat2, long2):
    df = df.withColumn(
        'distance_in_kms',
        round(
            (
                    acos(
                        (sin(radians(col(lat1))) * sin(radians(col(lat2)))) +
                        ((cos(radians(col(lat1))) * cos(radians(col(lat2)))) * (cos(radians(long1) - radians(long2))))
                    ) * lit(6371.0)
            ),
            4
        )
    )
    average_distance = df.na.drop(subset=['distance_in_kms']).agg(avg("distance_in_kms")).first()[0]
    df = df.na.fill(average_distance, subset=['distance_in_kms']).withColumnRenamed('distance_in_kms', 'distance')
    return df


def add_hour_day_month(df: DataFrame) -> DataFrame:
    return df.withColumn("hour", hour(col("pickup_datetime"))) \
        .withColumn("day", dayofmonth(col("pickup_datetime"))) \
        .withColumn("month", month(col("pickup_datetime")))


def apply_feature_engg(df: DataFrame) -> DataFrame:
    df = df.withColumn("store_and_fwd_flag", when(col("store_and_fwd_flag") == "N", 0).otherwise(1))
    df = add_haversine_distance(df, 'pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude')
    df = add_hour_day_month(df)
    df = apply_schema(df)
    return df


def assemble_features(df: DataFrame, feature_cols: List[str]) -> DataFrame:
    vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    features_df = vector_assembler.transform(df)
    return features_df


def train_gbtr(training_df: DataFrame, feature_cols: List[str], label_col: str) -> GBTRegressionModel:
    features_df = assemble_features(training_df, feature_cols)
    gbtr = GBTRegressor(featuresCol='features', labelCol=label_col)
    return gbtr.fit(features_df)


def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


def get_predictions(model: GBTRegressionModel, testing_df: DataFrame, feature_cols: List[str]) -> DataFrame:
    features_df = assemble_features(testing_df, feature_cols)
    predictions = model.transform(features_df)
    return predictions.select("id", "prediction") \
        .withColumn("trip_duration", predictions["prediction"].cast("int")) \
        .select("id", "trip_duration")


def main():
    spark = get_spark_session("nyc-taxi-tri-duration")

    training_trips_path, testing_trips_path, output_path = parse_args()
    raw_training_df = spark.read.csv(training_trips_path, header=True, inferSchema=True)
    raw_testing_df = spark.read.csv(testing_trips_path, header=True, inferSchema=True)

    feature_cols = ['vendor_id', 'passenger_count', 'store_and_fwd_flag', 'trip_duration', 'day', 'hour', 'month',
                    'distance']
    label_col = 'trip_duration'

    training_df = apply_feature_engg(raw_training_df).select(feature_cols + [label_col] + ['id'])
    testing_df = apply_feature_engg(raw_testing_df).select(feature_cols + ['id'])

    model = train_gbtr(training_df=training_df, feature_cols=feature_cols, label_col=label_col)
    submissions_df = get_predictions(model=model, testing_df=testing_df, feature_cols=feature_cols)

    submissions_df.repartition(1).write.csv(f"{output_path}/predictions", header=True)
    model.save(f"{output_path}/models/gbtr")

    spark.stop()


if __name__ == '__main__':
    main()
