from typing import Dict

from flask import Flask, request, jsonify
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def get_feature_schema() -> StructType:
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("vendor_id", IntegerType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("store_and_fwd_flag", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("distance", FloatType(), True)
    ])
    return schema


app = Flask(__name__)

required_feature_keys = ['vendor_id', 'passenger_count', 'store_and_fwd_flag', 'day', 'hour', 'month', 'distance']
required_keys = ['id'] + required_feature_keys

model_path = '../../data/nyc-taxi-trip-duration-kaggle/models/gbtr'
spark = SparkSession.builder.appName('real-time-taxi-trip-prediction').getOrCreate()
loaded_model = GBTRegressionModel.load(model_path)


@app.route('/predict-trip-duration', methods=['POST'])
def predict_trip_duration():
    def get_prediction(model: GBTRegressionModel, request_data: Dict) -> int:
        input_features = [request_data[key] for key in required_keys]
        input_df = spark.createDataFrame([tuple(input_features)], schema=get_feature_schema())
        vector_assembler = VectorAssembler(inputCols=required_feature_keys, outputCol="features")
        input_df = vector_assembler.transform(input_df)

        predictions = model.transform(input_df)
        predicted_trip_duration = predictions.select("prediction").collect()[0]["prediction"]
        return predicted_trip_duration

    try:
        data = request.json

        for key in required_keys:
            if key not in data:
                return jsonify({'error': f'Missing feature or id: {key}'}), 400

        predicted_trip_duration = get_prediction(loaded_model, data)

        response_data = {'predicted_trip_duration': predicted_trip_duration}
        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
