# S3 buckets: staging (raw uploads + normalized intermediates) and tiles
# (published MVT objects), mirroring TRD §6.

resource "aws_s3_bucket" "staging" {
  bucket = local.staging_bucket_name
}

resource "aws_s3_bucket" "tiles" {
  bucket = local.tiles_bucket_name
}

resource "aws_s3_bucket_versioning" "staging" {
  bucket = aws_s3_bucket.staging.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Versioning on the tile bucket retains prior tile versions for rollback
# (TRD §6/§14): the live version is tracked by the manifest, superseded
# versions expire after the retention window.
resource "aws_s3_bucket_versioning" "tiles" {
  bucket = aws_s3_bucket.tiles.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "staging" {
  bucket                  = aws_s3_bucket.staging.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "tiles" {
  bucket                  = aws_s3_bucket.tiles.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "staging" {
  bucket = aws_s3_bucket.staging.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.pipeline.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tiles" {
  bucket = aws_s3_bucket.tiles.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.pipeline.arn
    }
    bucket_key_enabled = true
  }
}

# TRD §6 retention: raw uploads and normalized intermediates expire in 30 days.
resource "aws_s3_bucket_lifecycle_configuration" "staging" {
  bucket = aws_s3_bucket.staging.id

  rule {
    id     = "expire-staging"
    status = "Enabled"
    expiration {
      days = var.staging_retention_days
    }
    noncurrent_version_expiration {
      noncurrent_days = var.staging_retention_days
    }
  }
}

# TRD §6 retention: prior published tile versions kept 90 days for rollback.
resource "aws_s3_bucket_lifecycle_configuration" "tiles" {
  bucket = aws_s3_bucket.tiles.id

  rule {
    id     = "expire-prior-tile-versions"
    status = "Enabled"
    filter {
      prefix = "tiles/"
    }
    noncurrent_version_expiration {
      noncurrent_days = var.prior_tile_version_retention_days
    }
  }
}
