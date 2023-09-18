provider "google" {
  credentials = file("./gcp_keys/dogwood-cinema-393905-5604e4d6e9e1.json")
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "dataproc_bucket" {
  name     = "nyc_taxi_trip_duration_dataproc_bucket"
  location = "US"
}

resource "google_storage_bucket_object" "predict_trip_duration_pyspark_file" {
  name   = "nyc_taxi_trip_experiments/predict_trip_duration.py"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../predict_trip_duration.py"
}

resource "google_storage_bucket_object" "predict_trip_duration_training_trips" {
  name   = "nyc_taxi_trip_experiments/train.csv"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../nyc-taxi-trip-duration/train.csv"
}

resource "google_storage_bucket_object" "predict_trip_duration_testing_trips" {
  name   = "nyc_taxi_trip_experiments/test.csv"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "../nyc-taxi-trip-duration/test.csv"
}

#resource "google_service_account" "dataproc_sa" {
#  account_id   = "dataproc_sa"
#  display_name = "Dataproc Service Account"
#}
#
#resource "google_dataproc_cluster" "nyc_trip_duration_prediction" {
#  name     = "nyc_trip_duration_prediction"
#  region   = "us-central1"
#  graceful_decommission_timeout = "120s"
#  labels = {
#    foo = "bar"
#  }
#
#  cluster_config {
#    staging_bucket = google_storage_bucket.dataproc_bucket.name
#
#    lifecycle_config {
#      idle_delete_ttl = "5m"
#    }
#
#    master_config {
#      num_instances = 1
#      machine_type  = "e2-medium"
#      disk_config {
#        boot_disk_type    = "pd-ssd"
#        boot_disk_size_gb = 4
#      }
#    }
#
#    # Override or set some custom properties
#    software_config {
#      image_version = "2.0.35-debian10"
#      override_properties = {
#        "dataproc:dataproc.allow.zero.workers" = "true"
#      }
#    }
#
#    gce_cluster_config {
#      tags = ["foo", "bar"]
#      # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
#      service_account = google_service_account.dataproc_sa.email
#      service_account_scopes = [
#        "cloud-platform"
#      ]
#    }
#
#    # You can define multiple initialization_action blocks
##    initialization_action {
##      script      = "gs://dataproc-initialization-actions/stackdriver/stackdriver.sh"
##      timeout_sec = 500
##    }
#  }
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
#  }
#}
#
#output "pyspark_status" {
#  value = google_dataproc_job.pyspark_nyc_taxi_trip_duration_prediction.status[0].state
#}