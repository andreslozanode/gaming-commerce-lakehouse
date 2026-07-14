variable "project_id"      { type = string }
variable "region"          { type = string }
variable "environment"     { type = string }
variable "project_name"    { type = string }
variable "layers"          { type = list(string) }
variable "enable_gpu_pool" { type = bool }
variable "labels"          { type = map(string) }
variable "cost_controls" {
  type = object({
    spot_instances   = bool
    autoscale_min    = number
    autoscale_max    = number
    budget_usd_month = number
  })
}
