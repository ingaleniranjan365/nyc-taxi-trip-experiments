provider "google" {
  credentials = file("./gcp_keys/tf_gcp_key.json")
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "dataproc_bucket" {
  name     = "nyc_taxi_trip_duration_dataproc_bucket"
  location = "US"
}

resource "google_storage_bucket_object" "insights_pyspark_file" {
  name   = "nyc_taxi_trip_experiments/insights/airport_insights.py"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../airport_insights.py"
}

resource "google_storage_bucket_object" "ride_hailing_services_taxi_trips" {
  name   = "nyc_taxi_trip_experiments/nyc-tlc-raw-taxi-trips/fhvhv_tripdata_2023-06.parquet"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../nyc-tlc-raw-taxi-trips/fhvhv_tripdata_2023-06.parquet"
}

resource "google_storage_bucket_object" "green_taxi_trips" {
  name   = "nyc_taxi_trip_experiments/nyc-tlc-raw-taxi-trips/green_tripdata_2023-06.parquet"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../nyc-tlc-raw-taxi-trips/green_tripdata_2023-06.parquet"
}

resource "google_storage_bucket_object" "yellow_taxi_trips" {
  name   = "nyc_taxi_trip_experiments/nyc-tlc-raw-taxi-trips/yellow_tripdata_2023-06.parquet"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../nyc-tlc-raw-taxi-trips/yellow_tripdata_2023-06.parquet"
}

resource "google_storage_bucket_object" "taxi_zones_lookup_table" {
  name   = "nyc_taxi_trip_experiments/nyc-tlc-raw-taxi-trips/taxi+_zone_lookup.csv"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../nyc-tlc-raw-taxi-trips/taxi+_zone_lookup.csv"
}

resource "google_service_account" "dataproc_sa" {
  account_id   = "dataproc-sa"
  display_name = "Dataproc Service Account"
}

variable "roles_to_assign" {
  type = list(string)
  default = [
    "roles/iam.serviceAccountActor",
    "roles/dataproc.worker",
  ]
}

resource "google_project_iam_member" "user_roles" {
  project = var.project
  role    = "roles/iam.serviceAccountActor"

  member = "serviceAccount:terraform@dogwood-cinema-393905.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "service_account_roles" {
  project = var.project
  role    = element(var.roles_to_assign, 1)

  member = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_dataproc_cluster" "nyc_taxi_trip_experiments" {
  name     = "nyc-taxi-trip-experiments"
  region   = "us-central1"
  graceful_decommission_timeout = "120s"
  labels = {
    foo = "bar"
  }

  cluster_config {
    staging_bucket = google_storage_bucket.dataproc_bucket.name

    lifecycle_config {
      idle_delete_ttl = "600s"
    }

    master_config {
      num_instances = 1
      machine_type  = "n2-standard-2"
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
      }
    }

    software_config {
      image_version = "2.0-debian10"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    gce_cluster_config {
      tags = ["foo", "bar"]
      service_account = google_service_account.dataproc_sa.email
      service_account_scopes = [
        "cloud-platform"
      ]
    }
  }
}

resource "google_dataproc_job" "airport_insights" {
  region       = google_dataproc_cluster.nyc_taxi_trip_experiments.region
  force_delete = true
  placement {
    cluster_name = google_dataproc_cluster.nyc_taxi_trip_experiments.name
  }

  pyspark_config {
    main_python_file_uri = "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.insights_pyspark_file.name}"
    properties = {
      "spark.logConf" = "true"
    }

    args = [
      "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.ride_hailing_services_taxi_trips.name}",
      "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.green_taxi_trips.name}",
      "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.yellow_taxi_trips.name}",
      "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.taxi_zones_lookup_table.name}",
      "gs://${google_storage_bucket.dataproc_bucket.name}/nyc_taxi_trip_experiments/insights/airport_insights"
    ]

  }
}

output "pyspark_status" {
  value = google_dataproc_job.airport_insights.status[0].state
}

#
#resource "google_storage_bucket_object" "predict_trip_duration_pyspark_file" {
#  name   = "nyc_taxi_trip_experiments/predict_trip_duration.py"
#  bucket = google_storage_bucket.dataproc_bucket.name
#  source = "../predict_trip_duration.py"
#}
#
#resource "google_storage_bucket_object" "predict_trip_duration_training_trips" {
#  name   = "nyc_taxi_trip_experiments/train.csv"
#  bucket = google_storage_bucket.dataproc_bucket.name
#  source = "../nyc-taxi-trip-duration/train.csv"
#}
#
#resource "google_storage_bucket_object" "predict_trip_duration_testing_trips" {
#  name   = "nyc_taxi_trip_experiments/test.csv"
#  bucket = google_storage_bucket.dataproc_bucket.name
#  source = "../nyc-taxi-trip-duration/test.csv"
#}
#
#resource "google_dataproc_job" "pyspark_nyc_taxi_trip_duration_prediction" {
#  region       = google_dataproc_cluster.nyc_trip_duration_prediction.region
#  force_delete = true
#  placement {
#    cluster_name = google_dataproc_cluster.nyc_trip_duration_prediction.name
#  }
#
#  pyspark_config {
#    main_python_file_uri = "gs://${google_storage_bucket.dataproc_bucket.name}/${google_storage_bucket_object.predict_trip_duration_pyspark_file.name}"
#    properties = {
#      "spark.logConf" = "true"
#    }
#
#    args = [
#      "gs://nyc_taxi_trip_duration_dataproc_bucket/nyc_taxi_trip_experiments/train.csv",
#      "gs://nyc_taxi_trip_duration_dataproc_bucket/nyc_taxi_trip_experiments/test.csv",
#      "gs://nyc_taxi_trip_duration_dataproc_bucket/nyc_taxi_trip_experiments/predictions",
#    ]
#
#  }
#}

