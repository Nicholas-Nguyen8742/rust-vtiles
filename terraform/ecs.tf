# ECS Fargate tile processor (TRD §11 Decision 2). Jobs are started
# per-queue-message by Step Functions (RunTask), not by a long-running
# service — this matches bursty ingestion with 20 concurrent jobs (TRD §14).

resource "aws_ecs_cluster" "pipeline" {
  name = "${local.name_prefix}-vector-tile"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_cluster_capacity_providers" "pipeline" {
  cluster_name       = aws_ecs_cluster.pipeline.name
  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
  }
}

resource "aws_ecs_task_definition" "processor" {
  family                   = "${local.name_prefix}-tile-processor"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.fargate_cpu
  memory                   = var.fargate_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  ephemeral_storage {
    size_in_gib = var.fargate_ephemeral_storage_gb
  }

  container_definitions = jsonencode([
    {
      name      = "tile-processor"
      image     = var.processor_image
      essential = true
      environment = [
        { name = "ENV", value = var.env },
        { name = "STAGING_BUCKET", value = aws_s3_bucket.staging.bucket },
        { name = "TILES_BUCKET", value = aws_s3_bucket.tiles.bucket },
        { name = "JOBS_TABLE", value = aws_dynamodb_table.jobs.name },
        { name = "LAYERS_TABLE", value = aws_dynamodb_table.layers.name },
        { name = "UPLOAD_MAX_BYTES", value = tostring(var.upload_max_bytes) },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.processor.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "tile-processor"
        }
      }
    }
  ])
}

resource "aws_cloudwatch_log_group" "processor" {
  name              = "/ecs/${local.name_prefix}-tile-processor"
  retention_in_days = var.env == "prod" ? 365 : 30 # TRD §6: job logs 1 year
}
