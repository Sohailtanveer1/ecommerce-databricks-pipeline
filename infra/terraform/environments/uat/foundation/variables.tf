variable "project" {
  type    = string
  default = "ecomlake"
}
variable "location" {
  type    = string
  default = "eastus"
}
variable "my_object_id" {
  type = string
}
variable "postgres_jdbc_url" {
  type    = string
  default = "jdbc:postgresql://localhost:5432/ecommerce"
}
variable "postgres_username" {
  type      = string
  default   = "ecom_reader"
  sensitive = true
}
variable "postgres_password" {
  type      = string
  sensitive = true
}
variable "fx_api_key" {
  type      = string
  default   = ""
  sensitive = true
}
variable "alert_email" {
  type    = string
  default = "data-team@example.com"
}
