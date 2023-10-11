resource "google_project_iam_member" "user_roles" {
  project = var.project
  role    = "roles/iam.serviceAccountActor"

  member = "serviceAccount:terraform@dogwood-cinema-393905.iam.gserviceaccount.com"
}
