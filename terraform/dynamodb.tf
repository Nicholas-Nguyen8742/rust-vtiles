# Job and layer metadata tables (TRD §7 data models; on-demand capacity for
# the serverless ingestion pattern).

resource "aws_dynamodb_table" "jobs" {
  name         = local.jobs_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "jobId"

  attribute {
    name = "jobId"
    type = "S"
  }

  point_in_time_recovery {
    enabled = var.env == "prod"
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.pipeline.arn
  }

  tags = { Name = local.jobs_table_name }
}

resource "aws_dynamodb_table" "layers" {
  name         = local.layers_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "layerId"

  attribute {
    name = "layerId"
    type = "S"
  }

  attribute {
    name = "tenantId"
    type = "S"
  }

  # TRD §8.3 list-by-tenant queries.
  global_secondary_index {
    name            = "tenant-index"
    hash_key        = "tenantId"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = var.env == "prod"
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.pipeline.arn
  }

  tags = { Name = local.layers_table_name }
}
