# Ingestion queue + DLQ (TRD §2, §14: retry 3x then DLQ).

resource "aws_sqs_queue" "ingest_dlq" {
  name                      = "${local.name_prefix}-${local.dlq_name}"
  message_retention_seconds = 1209600 # 14 days for DLQ inspection/replay
  kms_master_key_id         = aws_kms_key.pipeline.arn

  tags = { Name = "${local.name_prefix}-${local.dlq_name}" }
}

resource "aws_sqs_queue" "ingest" {
  name                       = "${local.name_prefix}-${local.ingest_queue_name}"
  visibility_timeout_seconds = 1200 # Fargate tile jobs run up to 15 min (US-02)
  message_retention_seconds  = 345600
  receive_wait_time_seconds  = 20
  kms_master_key_id          = aws_kms_key.pipeline.arn

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.ingest_dlq.arn
    maxReceiveCount     = 3
  })

  tags = { Name = "${local.name_prefix}-${local.ingest_queue_name}" }
}

# Allow S3 event notifications to enqueue messages (events.tf).
resource "aws_sqs_queue_policy" "ingest_from_s3" {
  queue_url = aws_sqs_queue.ingest.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowS3EventMessages"
      Effect    = "Allow"
      Principal = { Service = "s3.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.ingest.arn
      Condition = {
        ArnEquals = {
          "aws:SourceArn" = aws_s3_bucket.staging.arn
        }
        StringEquals = {
          "aws:SourceAccount" = data.aws_caller_identity.current.account_id
        }
      }
    }]
  })
}

data "aws_caller_identity" "current" {}
