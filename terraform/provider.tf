provider "google" {
  credentials = file("./gcp_keys/tf_gcp_key.json")
  project     = var.project
  region      = var.region
}
