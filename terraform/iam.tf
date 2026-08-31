# IAM roles: ECS task execution/task roles and the Step Functions role.

data "aws_iam_policy_document" "ecs_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "states_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

# Pulls images and writes container logs.
resource "aws_iam_role" "ecs_execution" {
  name               = "${local.name_prefix}-tile-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

resource "aws_iam_role_policy" "ecs_execution" {
  name = "${local.name_prefix}-tile-execution"
  role = aws_iam_role.ecs_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.processor.arn}:*"
      },
    ]
  })
}

# Least-privilege pipeline access (TRD §13): staging read + tenant-scoped
# tile/manifest write, DynamoDB job/layer access, KMS use.
resource "aws_iam_role" "ecs_task" {
  name               = "${local.name_prefix}-tile-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role.json
}

resource "aws_iam_role_policy" "ecs_task" {
  name = "${local.name_prefix}-tile-task"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "StagingRead"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${aws_s3_bucket.staging.arn}/staging/*",
        ]
      },
      {
        Sid    = "StagingList"
        Effect = "Allow"
        Action = ["s3:ListBucket"]
        Resource = aws_s3_bucket.staging.arn
      },
      {
        Sid    = "TileWrite"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
        ]
        Resource = [
          "${aws_s3_bucket.tiles.arn}/tiles/*",
          "${aws_s3_bucket.tiles.arn}/manifests/*",
        ]
      },
      {
        Sid    = "Metadata"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:Query",
        ]
        Resource = [
          aws_dynamodb_table.jobs.arn,
          aws_dynamodb_table.layers.arn,
          "${aws_dynamodb_table.layers.arn}/index/*",
        ]
      },
      {
        Sid    = "KmsUse"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
        ]
        Resource = aws_kms_key.pipeline.arn
      },
    ]
  })
}

# Step Functions: run ECS tasks (runTask.sync integration) and track their
# state via EventBridge.
resource "aws_iam_role" "step_functions" {
  name               = "${local.name_prefix}-vector-tile-sfn"
  assume_role_policy = data.aws_iam_policy_document.states_assume_role.json
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${local.name_prefix}-vector-tile-sfn"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RunTasks"
        Effect = "Allow"
        Action = [
          "ecs:RunTask",
          "ecs:StopTask",
          "ecs:DescribeTasks",
        ]
        Resource = ["*"]
        Condition = {
          ArnEquals = {
            "ecs:cluster" = aws_ecs_cluster.pipeline.arn
          }
        }
      },
      {
        Sid      = "PassTaskRoles"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [
          aws_iam_role.ecs_execution.arn,
          aws_iam_role.ecs_task.arn,
        ]
      },
      {
        Sid    = "TaskStateEvents"
        Effect = "Allow"
        Action = [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule",
        ]
        Resource = [
          "arn:aws:events:${var.aws_region}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsForECSTaskRule",
        ]
      },
      {
        Sid    = "StateLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups",
        ]
        Resource = ["*"]
      },
    ]
  })
}
