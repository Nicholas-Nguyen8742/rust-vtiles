variable "env" {
  description = "Environment name: dev, staging, or prod (TRD §16)."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.env)
    error_message = "env must be one of dev, staging, prod."
  }
}

variable "aws_region" {
  description = "Primary AWS region for the pipeline."
  type        = string
  default     = "us-east-1"
}

variable "processor_image" {
  description = <<-EOT
    Container image URI for the ECS Fargate tile processor (the `vtile` CLI,
    or GDAL+Tippecanoe if using the TRD reference stack).
  EOT
  type        = string
  default     = ""
}

variable "tile_origin_allowed_origins" {
  description = "CORS allowed origins for tile delivery (TRD §13)."
  type        = list(string)
  default = [
    "https://app.creplatform.com",
    "https://analytics.creplatform.com",
  ]
}

variable "upload_max_bytes" {
  description = "Maximum upload size in bytes (TRD §14: 2 GB)."
  type        = number
  default     = 2 * 1024 * 1024 * 1024
}

variable "prior_tile_version_retention_days" {
  description = "Retention for superseded tile versions, for rollback (TRD §6: 90 days)."
  type        = number
  default     = 90
}

variable "staging_retention_days" {
  description = "Retention for raw uploads and normalized intermediates (TRD §6: 30 days)."
  type        = number
  default     = 30
}

variable "fargate_cpu" {
  description = "Fargate task CPU units (US-02: 4 vCPU = 4096)."
  type        = number
  default     = 4096
}

variable "fargate_memory" {
  description = "Fargate task memory in MiB (US-02: 8 GB)."
  type        = number
  default     = 8192
}

variable "fargate_ephemeral_storage_gb" {
  description = "Ephemeral storage for large Shapefile/GeoJSON working sets."
  type        = number
  default     = 200
}

variable "vpc_subnet_ids" {
  description = "Subnets for Fargate tile-processor tasks (awsvpc network mode)."
  type        = list(string)
  default     = []
}

variable "vpc_security_group_ids" {
  description = "Security groups for Fargate tile-processor tasks."
  type        = list(string)
  default     = []
}
