# Step Functions orchestration (TRD §2, §10; retry/DLQ behavior per US-06).
#
# The SQS → Step Functions bridge (EventBridge Pipes or a small poller
# Lambda) is environment-specific and intentionally not included in this
# scaffold; the state machine expects input shaped like:
#
#   { "jobId": "job_01J9XYZ", "tenantId": "tenant-acme", "layerId": "..." }

resource "aws_cloudwatch_log_group" "step_functions" {
  # CloudFront/States vended logs require this prefix for auto-policy.
  name              = "/aws/vendedlogs/states/${local.name_prefix}-vector-tile-ingest"
  retention_in_days = var.env == "prod" ? 365 : 30
}

resource "aws_sfn_state_machine" "ingest" {
  name     = "${local.name_prefix}-vector-tile-ingest"
  role_arn = aws_iam_role.step_functions.arn

  definition = templatefile("${path.module}/step_functions/state_machine.json.tftpl", {
    cluster_arn         = aws_ecs_cluster.pipeline.arn
    task_definition_arn = aws_ecs_task_definition.processor.arn
    container_name      = "tile-processor"
    subnet_ids          = var.vpc_subnet_ids
    security_group_ids  = var.vpc_security_group_ids
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  depends_on = [aws_iam_role_policy.step_functions]
}
