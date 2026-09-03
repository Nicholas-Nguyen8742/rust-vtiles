# KMS key for SSE-KMS object encryption (TRD §13 data protection).
#
# Sequence 5 TI-03: objects are tenant-prefixed and the tenant is recorded
# in job/layer metadata; access is restricted through IAM + this key policy.
# Higher-sensitivity tenants can graduate to per-tenant keys or aliases
# (`alias/{prefix}-pipeline-{tenantId}`) with key policies limited to that
# tenant's roles — add those keys here when required.

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
