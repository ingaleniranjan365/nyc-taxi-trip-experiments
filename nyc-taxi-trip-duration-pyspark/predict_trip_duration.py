from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline


def get_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.executor.memory", "12g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.default.parallelism", 8) \
        .getOrCreate()


spark = get_spark_session("nyc-taxi-tri-duration")
train_df = spark.read.csv("nyc-taxi-trip-duration/train.csv", header=True, inferSchema=True)
test_df = spark.read.csv("nyc-taxi-trip-duration/test.csv", header=True, inferSchema=True)

feature_cols = ["pickup_longitude", "pickup_latitude", "dropoff_longitude", "passenger_count"]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

lr = LinearRegression(featuresCol="features", labelCol="trip_duration")

pipeline = Pipeline(stages=[vector_assembler, lr])

model = pipeline.fit(train_df)

predictions = model.transform(test_df)

submissions_df = predictions.select("id", "prediction")\
                            .withColumn("trip_duration", predictions["prediction"].cast("int"))\
                            .select("id", "trip_duration")


submissions_df.repartition(1).write.csv("predictions", header=True)

spark.stop()
