# CloudFront tile delivery (TRD §2, US-03). MVP: single-region origin; V2
# adds an origin group with regional failover (US-08).
#
# Missing tiles return 404 from S3; the Lambda@Edge OriginResponse handler
# that converts them to 204 (US-03) is deployed separately with the edge
# function code and is referenced here via `lambda_function_association`
# once published.

resource "aws_cloudfront_origin_access_control" "tiles" {
  name                              = "${local.name_prefix}-tiles"
  description                       = "OAC for ${local.tiles_bucket_name}"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# CORS exactly as TRD §13.
resource "aws_cloudfront_response_headers_policy" "tiles_cors" {
  name    = "${local.name_prefix}-tiles-cors"
  comment = "Platform tile CORS (TRD §13)"

  cors_config {
    access_control_allow_origins = var.tile_origin_allowed_origins
    access_control_allow_methods = ["GET", "HEAD"]
    access_control_allow_headers = ["Authorization", "Content-Type"]
    access_control_expose_headers = ["ETag", "Content-Encoding"]
    access_control_max_age_sec    = 3600
    origin_override               = true
  }
}

resource "aws_cloudfront_cache_policy" "tiles" {
  name    = "${local.name_prefix}-tiles"
  comment = "Tile caching: gzip pass-through, no query strings"

  default_ttl = 3600  # detail zooms (US-03); low zooms get long CDN TTL anyway
  min_ttl     = 0
  max_ttl     = 86400

  parameters_in_cache_key_and_forwarded_to_origin {
    enable_accept_encoding_gzip   = true
    enable_accept_encoding_brotli = false

    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "none"
    }
  }
}

resource "aws_cloudfront_distribution" "tiles" {
  enabled         = true
  comment         = "Vector tile delivery (${var.env})"
  price_class     = "PriceClass_100"
  http_version    = "http2and3"
  is_ipv6_enabled = true

  origin {
    domain_name              = aws_s3_bucket.tiles.bucket_regional_domain_name
    origin_id                = "tiles-s3"
    origin_access_control_id = aws_cloudfront_origin_access_control.tiles.id
  }

  default_cache_behavior {
    target_origin_id       = "tiles-s3"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    cache_policy_id          = aws_cloudfront_cache_policy.tiles.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.tiles_cors.id
  }

  # Missing tiles surface as S3 404s until the Lambda@Edge OriginResponse
  # handler (US-03) converts them to 204; map clients should treat 404 as
  # empty in the interim.
  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }

  tags = { Name = "${local.name_prefix}-tiles" }
}

# OAC read access on the tile bucket.
resource "aws_s3_bucket_policy" "tiles" {
  bucket = aws_s3_bucket.tiles.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowCloudFrontServicePrincipal"
      Effect    = "Allow"
      Principal = { Service = "cloudfront.amazonaws.com" }
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.tiles.arn}/tiles/*"
      Condition = {
        StringEquals = {
          "AWS:SourceArn" = aws_cloudfront_distribution.tiles.arn
        }
      }
    }]
  })
}
