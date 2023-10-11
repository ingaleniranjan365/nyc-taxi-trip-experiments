resource "google_storage_bucket" "dataproc_bucket" {
  name     = "nyc-taxi-trip-experiments-dataproc-bucket"
  location = "US"
}

resource "google_storage_bucket_object" "insights_pyspark_file" {
  name   = "nyc-taxi-trip-experiments/insights/airport_insights.py"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../pyspark/airport-insights/airport_insights.py"
}

resource "google_storage_bucket_object" "ride_hailing_services_taxi_trips" {
  name   = "nyc-taxi-trip-experiments/nyc-tlc-raw-taxi-trips/fhvhv_tripdata_2023-06.parquet"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../data/nyc-tlc-raw/fhvhv_tripdata_2023-06.parquet"
}

resource "google_storage_bucket_object" "green_taxi_trips" {
  name   = "nyc-taxi-trip-experiments/nyc-tlc-raw-taxi-trips/green_tripdata_2023-06.parquet"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../data/nyc-tlc-raw/green_tripdata_2023-06.parquet"
}

resource "google_storage_bucket_object" "yellow_taxi_trips" {
  name   = "nyc-taxi-trip-experiments/nyc-tlc-raw-taxi-trips/yellow_tripdata_2023-06.parquet"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../data/nyc-tlc-raw/yellow_tripdata_2023-06.parquet"
}

resource "google_storage_bucket_object" "taxi_zones_lookup_table" {
  name   = "nyc-taxi-trip-experiments/nyc-tlc-raw-taxi-trips/taxi+_zone_lookup.csv"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../data/nyc-tlc-raw/taxi+_zone_lookup.csv"
}

resource "google_storage_bucket_object" "predict_trip_duration_pyspark_file" {
  name   = "nyc-taxi-trip-experiments/taxi-trip-duration/predict_trip_duration.py"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../pyspark/taxi-trip-duration/predict_trip_duration.py"
}

resource "google_storage_bucket_object" "predict_trip_duration_training_trips" {
  name   = "nyc-taxi-trip-experiments/kaggle-taxi-trip-data/train.csv"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../data/nyc-taxi-trip-duration-kaggle/train.csv"
}

resource "google_storage_bucket_object" "predict_trip_duration_testing_trips" {
  name   = "nyc-taxi-trip-experiments/test.csv"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../data/nyc-taxi-trip-duration-kaggle/kaggle-taxi-trip-data/test.csv"
}


