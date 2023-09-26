import sys
import logging
from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline, PipelineModel

logging.basicConfig(level=logging.INFO)


def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


def train_lr(train_df: DataFrame, feature_cols: List[str]) -> PipelineModel:
    vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="trip_duration")
    pipeline = Pipeline(stages=[vector_assembler, lr])
    return pipeline.fit(train_df)


def get_submission(model: PipelineModel, test_df: DataFrame):
    predictions = model.transform(test_df)
    return predictions.select("id", "prediction") \
        .withColumn("trip_duration", predictions["prediction"].cast("int")) \
        .select("id", "trip_duration")


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


if __name__ == '__main__':
    training_trips_path, testing_trips_path, output_path = parse_args()

    spark = get_spark_session("nyc-taxi-tri-duration")

    train_df = spark.read.csv(training_trips_path, header=True, inferSchema=True)
    test_df = spark.read.csv(testing_trips_path, header=True, inferSchema=True)
    model = train_lr(train_df,
                     feature_cols=["pickup_longitude", "pickup_latitude", "dropoff_longitude", "passenger_count"])
    submissions_df = get_submission(model, test_df)

    submissions_df.repartition(1).write.csv(output_path, header=True)

    spark.stop()
