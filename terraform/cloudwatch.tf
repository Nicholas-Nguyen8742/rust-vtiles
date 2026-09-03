# Observability (Sequence 4 US-OBS-03): CloudWatch dashboards + alarms.
#
# Custom metric namespace: `VectorTilePipeline` — emitted by the tile
# processor and ingestion Lambda with bounded dimensions (environment,
# tenantId, sourceFormat, layerCategory, errorCode). Metric inventory and
# the alert severity matrix live in docs/OBSERVABILITY.md.

locals {
  cw_namespace = "VectorTilePipeline"

  # Alert severity matrix (Sequence 4 US-OBS-03).
  alert_rules = {
    Tile5xxRateHigh                = "P1"
    OriginFailureRateHigh          = "P1"
    DlqMessageReceived             = "P2"
    JobFailureRateHigh             = "P2"
    NoCompletedJobsWithBacklog     = "P2"
    JobDurationHigh                = "P3"
    TileSizeP95High                = "P3"
    ReplayOperationFailed          = "P2"
    TenantAuthorizationFailure     = "P2"
  }
}

resource "aws_sns_topic" "ops_alerts" {
  name              = "${local.name_prefix}-vector-tile-alerts"
  kms_master_key_id = aws_kms_key.pipeline.arn

  tags = { Name = "${local.name_prefix}-vector-tile-alerts" }
}

# ── Alarms ───────────────────────────────────────────────────────────────────

# P1: tile 5xx rate > 1% for 5 minutes.
resource "aws_cloudwatch_metric_alarm" "tile_5xx_rate" {
  alarm_name          = "${local.name_prefix}-Tile5xxRateHigh"
  alarm_description   = "P1: tile delivery 5xx rate above 1% (docs/OBSERVABILITY.md)"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 0.01
  treat_missing_data  = "notBreaching"

  metric_query {
    id          = "rate"
    expression  = "errors / requests"
    label       = "5xx rate"
    return_data = true
  }
  metric_query {
    id = "errors"
    metric {
      namespace = local.cw_namespace
      metric_name = "tile_5xx_total"
      period      = 300
      stat        = "Sum"
    }
  }
  metric_query {
    id = "requests"
    metric {
      namespace = local.cw_namespace
      metric_name = "tile_requests_total"
      period      = 300
      stat        = "Sum"
    }
  }

  alarm_actions = [aws_sns_topic.ops_alerts.arn]
  ok_actions    = [aws_sns_topic.ops_alerts.arn]

  tags = { Name = "${local.name_prefix}-Tile5xxRateHigh", Severity = "P1" }
}

# P1: CloudFront origin failure rate > 1% for 5 minutes.
resource "aws_cloudwatch_metric_alarm" "origin_failure_rate" {
  alarm_name          = "${local.name_prefix}-OriginFailureRateHigh"
  alarm_description   = "P1: CloudFront 5xx error rate above 1%"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 0.01
  treat_missing_data  = "notBreaching"

  metric_query {
    id          = "rate"
    expression  = "errors / requests"
    label       = "origin 5xx rate"
    return_data = true
  }
  metric_query {
    id = "errors"
    metric {
      namespace   = "AWS/CloudFront"
      metric_name = "5xxErrorRate"
      period      = 300
      stat        = "Average"
      dimensions = {
        DistributionId = aws_cloudfront_distribution.tiles.id
        Region         = "Global"
      }
    }
  }
  metric_query {
    id = "requests"
    metric {
      namespace   = "AWS/CloudFront"
      metric_name = "Requests"
      period      = 300
      stat        = "Sum"
      dimensions = {
        DistributionId = aws_cloudfront_distribution.tiles.id
        Region         = "Global"
      }
    }
  }

  alarm_actions = [aws_sns_topic.ops_alerts.arn]

  tags = { Name = "${local.name_prefix}-OriginFailureRateHigh", Severity = "P1" }
}

# P2: any message in the dead-letter queue.
resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  alarm_name          = "${local.name_prefix}-DlqMessageReceived"
  alarm_description   = "P2: dead-letter queue depth > 0 (runbook: docs/RECOVERY.md)"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = { QueueName = aws_sqs_queue.ingest_dlq.name }

  alarm_actions = [aws_sns_topic.ops_alerts.arn]

  tags = { Name = "${local.name_prefix}-DlqMessageReceived", Severity = "P2" }
}

# P2: job failure rate > 5% over 15 minutes.
resource "aws_cloudwatch_metric_alarm" "job_failure_rate" {
  alarm_name          = "${local.name_prefix}-JobFailureRateHigh"
  alarm_description   = "P2: job failure rate above 5% over 15 minutes"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 0.05
  treat_missing_data  = "notBreaching"

  metric_query {
    id          = "rate"
    expression  = "failed / (failed + completed)"
    label       = "failure rate"
    return_data = true
  }
  metric_query {
    id = "failed"
    metric {
      namespace   = local.cw_namespace
      metric_name = "ingest_jobs_failed_total"
      period      = 900
      stat        = "Sum"
    }
  }
  metric_query {
    id = "completed"
    metric {
      namespace   = local.cw_namespace
      metric_name = "ingest_jobs_completed_total"
      period      = 900
      stat        = "Sum"
    }
  }

  alarm_actions = [aws_sns_topic.ops_alerts.arn]

  tags = { Name = "${local.name_prefix}-JobFailureRateHigh", Severity = "P2" }
}

# P2: queue has backlog but nothing completes for 30 minutes.
resource "aws_cloudwatch_metric_alarm" "no_completed_jobs_with_backlog" {
  alarm_name          = "${local.name_prefix}-NoCompletedJobsWithBacklog"
  alarm_description   = "P2: no completed jobs for 30 minutes while queue depth > 0"
  comparison_operator = "LessThanOrEqualToThreshold"
  evaluation_periods  = 6
  metric_name         = "ingest_jobs_completed_total"
  namespace           = local.cw_namespace
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "breaching"

  alarm_actions = [aws_sns_topic.ops_alerts.arn]

  tags = { Name = "${local.name_prefix}-NoCompletedJobsWithBacklog", Severity = "P2" }
}

# P3: job duration p95 above the 2x-baseline threshold (10 minutes here).
resource "aws_cloudwatch_metric_alarm" "job_duration_high" {
  alarm_name          = "${local.name_prefix}-JobDurationHigh"
  alarm_description   = "P3: job duration p95 above 2x baseline for 30 minutes"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 6
  metric_name         = "ingest_job_duration_seconds"
  namespace           = local.cw_namespace
  period              = 300
  extended_statistic  = "p95"
  threshold           = 600
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.ops_alerts.arn]

  tags = { Name = "${local.name_prefix}-JobDurationHigh", Severity = "P3" }
}

# P3: tile size p95 above 500 KB (parcel layers).
resource "aws_cloudwatch_metric_alarm" "tile_size_p95_high" {
  alarm_name          = "${local.name_prefix}-TileSizeP95High"
  alarm_description   = "P3: tile size p95 above 500 KB for parcel layers (TRD §5 targets)"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "geospatial_tile_size_bytes"
  namespace           = local.cw_namespace
  period              = 900
  extended_statistic  = "p95"
  threshold           = 512000
  treat_missing_data  = "notBreaching"

  dimensions = { layerCategory = "PARCEL" }

  alarm_actions = [aws_sns_topic.ops_alerts.arn]

  tags = { Name = "${local.name_prefix}-TileSizeP95High", Severity = "P3" }
}

# P2: replay operations failing.
resource "aws_cloudwatch_metric_alarm" "replay_failure" {
  alarm_name          = "${local.name_prefix}-ReplayOperationFailed"
  alarm_description   = "P2: replay operation failures (runbook: docs/RECOVERY.md)"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "replay_failure_count"
  namespace           = local.cw_namespace
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.ops_alerts.arn]

  tags = { Name = "${local.name_prefix}-ReplayOperationFailed", Severity = "P2" }
}

# P2: cross-tenant authorization denials (security review).
resource "aws_cloudwatch_metric_alarm" "tenant_authorization_failure" {
  alarm_name          = "${local.name_prefix}-TenantAuthorizationFailure"
  alarm_description   = "P2: cross-tenant authorization denials detected (TRD §13)"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "tenant_authorization_denied_total"
  namespace           = local.cw_namespace
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.ops_alerts.arn]

  tags = { Name = "${local.name_prefix}-TenantAuthorizationFailure", Severity = "P2" }
}

# ── Dashboard ────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "${local.name_prefix}-vector-tile-pipeline"

  dashboard_body = jsonencode({
    widgets = [
      # Pipeline Health
      {
        type = "metric"
        x = 0, y = 0, width = 12, height = 6
        properties = {
          title   = "Pipeline Health — jobs"
          region  = var.aws_region
          metrics = [
            [local.cw_namespace, "ingest_jobs_submitted_total"],
            [local.cw_namespace, "ingest_jobs_started_total"],
            [local.cw_namespace, "ingest_jobs_completed_total"],
            [local.cw_namespace, "ingest_jobs_failed_total"],
          ]
          stat   = "Sum"
          period = 300
        }
      },
      {
        type = "metric"
        x = 12, y = 0, width = 12, height = 6
        properties = {
          title   = "Backlog + DLQ depth"
          region  = var.aws_region
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.ingest.name],
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.ingest_dlq.name],
            [local.cw_namespace, "ingest_retry_total"],
          ]
          stat   = "Maximum"
          period = 60
        }
      },
      # Failure Analysis
      {
        type = "metric"
        x = 0, y = 6, width = 12, height = 6
        properties = {
          title   = "Failure Analysis — failures by error code"
          region  = var.aws_region
          metrics = [
            [local.cw_namespace, "ingest_jobs_failed_total"],
            [local.cw_namespace, "ingest_validation_failures_total"],
            [local.cw_namespace, "ingest_dlq_messages_total"],
          ]
          stat   = "Sum"
          period = 300
        }
      },
      {
        type = "metric"
        x = 12, y = 6, width = 12, height = 6
        properties = {
          title   = "Tenant isolation signals"
          region  = var.aws_region
          metrics = [
            [local.cw_namespace, "tenant_authorization_denied_total"],
            [local.cw_namespace, "cross_tenant_access_attempt_total"],
            [local.cw_namespace, "replay_operation_total"],
          ]
          stat   = "Sum"
          period = 300
        }
      },
      # Tile Delivery
      {
        type = "metric"
        x = 0, y = 12, width = 12, height = 6
        properties = {
          title   = "Tile Delivery — latency p95"
          region  = var.aws_region
          metrics = [
            [local.cw_namespace, "tile_request_duration_seconds"],
          ]
          extendedStatistic = "p95"
          period            = 300
        }
      },
      {
        type = "metric"
        x = 12, y = 12, width = 12, height = 6
        properties = {
          title   = "Tile Delivery — status classes"
          region  = var.aws_region
          metrics = [
            [local.cw_namespace, "tile_requests_total"],
            [local.cw_namespace, "tile_4xx_total"],
            [local.cw_namespace, "tile_5xx_total"],
            [local.cw_namespace, "tile_empty_responses_total"],
            [local.cw_namespace, "tile_cache_hits_total"],
          ]
          stat   = "Sum"
          period = 300
        }
      },
      # CRE Layer Quality
      {
        type = "metric"
        x = 0, y = 18, width = 12, height = 6
        properties = {
          title   = "CRE Layer Quality — publish duration + volume"
          region  = var.aws_region
          metrics = [
            [local.cw_namespace, "layer_publish_duration_seconds"],
            [local.cw_namespace, "geospatial_features_processed_total"],
            [local.cw_namespace, "geospatial_tiles_published_total"],
          ]
          stat   = "Sum"
          period = 900
        }
      },
      {
        type = "metric"
        x = 12, y = 18, width = 12, height = 6
        properties = {
          title   = "CRE Layer Quality — tile size p95 (TRD §5 targets)"
          region  = var.aws_region
          metrics = [
            [local.cw_namespace, "geospatial_tile_size_bytes", "layerCategory", "PARCEL"],
            [local.cw_namespace, "geospatial_tile_size_bytes"],
          ]
          extendedStatistic = "p95"
          period            = 900
        }
      },
    ]
  })
}
