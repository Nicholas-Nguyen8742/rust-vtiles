# KMS key for SSE-KMS object encryption (TRD §13 data protection).

resource "aws_kms_key" "pipeline" {
  description             = "SSE-KMS key for ${local.name_prefix} vector tile pipeline objects"
  deletion_window_in_days = var.env == "prod" ? 30 : 7
  enable_key_rotation     = true

  tags = {
    Name = "${local.name_prefix}-pipeline"
  }
}

resource "aws_kms_alias" "pipeline" {
  name          = "alias/${local.name_prefix}-pipeline"
  target_key_id = aws_kms_key.pipeline.key_id
}
