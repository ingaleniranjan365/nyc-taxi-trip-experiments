resource "google_bigquery_dataset" "nyc_tlc_raw_taxi_trips" {
  dataset_id = var.raw_nyc_tlc_taxi_trips_dataset
  project    = var.project
  location   = var.region
}

resource "google_bigquery_table" "ride_hailing_services_taxi_trips" {
  dataset_id = google_bigquery_dataset.nyc_tlc_raw_taxi_trips.dataset_id
  table_id   = "ride_hailing_services_taxi_trips"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = [
      "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.ride_hailing_services_taxi_trips.name}"
    ]
  }
}

resource "google_bigquery_table" "green_taxi_trips" {
  dataset_id = google_bigquery_dataset.nyc_tlc_raw_taxi_trips.dataset_id
  table_id   = "green_taxi_trips"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = [
      "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.green_taxi_trips.name}"
    ]
  }
}

resource "google_bigquery_table" "yellow_taxi_trips" {
  dataset_id = google_bigquery_dataset.nyc_tlc_raw_taxi_trips.dataset_id
  table_id   = "yellow_taxi_trips"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = [
      "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.yellow_taxi_trips.name}"
    ]
  }
}

resource "google_bigquery_table" "taxi_zones_lookup_table" {
  dataset_id = google_bigquery_dataset.nyc_tlc_raw_taxi_trips.dataset_id
  table_id   = "taxi_zones_lookup_table"

  external_data_configuration {
    autodetect    = true
    source_format = "CSV"

    source_uris = [
      "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.taxi_zones_lookup_table.name}"
    ]
  }
}
