# S3 event notification → SQS (TRD §2): an upload landing under
# staging/{tenantId}/{jobId}/input/ enqueues a processing message.

resource "aws_s3_bucket_notification" "staging" {
  bucket = aws_s3_bucket.staging.id

  queue {
    queue_arn     = aws_sqs_queue.ingest.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "staging/"
  }

  depends_on = [aws_sqs_queue_policy.ingest_from_s3]
}
