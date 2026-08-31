locals {
  project     = "cre-geo"
  name_prefix = "${local.project}-${var.env}"

  staging_bucket_name = "${local.project}-staging-${var.env}"
  tiles_bucket_name   = "${local.project}-tiles-${var.env}"

  ingest_queue_name = "vector-tile-ingest-queue"
  dlq_name          = "vector-tile-ingest-dlq"

  jobs_table_name   = "${local.name_prefix}-vector-tile-jobs"
  layers_table_name = "${local.name_prefix}-vector-tile-layers"
}
