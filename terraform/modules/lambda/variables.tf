variable "function_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "src_dir" {
  type        = string
}

variable "source_bucket_arn" {
  type        = string
}

variable "environment_variables" {
  type    = map(string)
  default = {}
}
