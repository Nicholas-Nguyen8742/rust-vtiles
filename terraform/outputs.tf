output "staging_bucket" {
  description = "Raw upload / staging bucket."
  value       = aws_s3_bucket.staging.bucket
}

output "tiles_bucket" {
  description = "Published tile bucket."
  value       = aws_s3_bucket.tiles.bucket
}

output "ingest_queue_url" {
  description = "Ingestion SQS queue URL."
  value       = aws_sqs_queue.ingest.url
}

output "ingest_dlq_url" {
  description = "Dead-letter queue URL."
  value       = aws_sqs_queue.ingest_dlq.url
}

output "jobs_table" {
  description = "DynamoDB job table."
  value       = aws_dynamodb_table.jobs.name
}

output "layers_table" {
  description = "DynamoDB layer catalog table."
  value       = aws_dynamodb_table.layers.name
}

output "ecs_cluster" {
  description = "Tile processor ECS cluster."
  value       = aws_ecs_cluster.pipeline.name
}

output "state_machine_arn" {
  description = "Ingestion Step Functions state machine."
  value       = aws_sfn_state_machine.ingest.arn
}

output "tile_distribution_domain" {
  description = "CloudFront domain for tile delivery."
  value       = aws_cloudfront_distribution.tiles.domain_name
}

output "tile_url_template" {
  description = "Client URL template for published tiles."
  value       = "https://${aws_cloudfront_distribution.tiles.domain_name}/tiles/{tenantId}/{layerId}/{z}/{x}/{y}.pbf"
}
