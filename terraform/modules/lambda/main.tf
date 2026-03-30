data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = var.src_dir
  output_path = "${path.module}/builds/function.zip"
}

resource "aws_iam_role" "this" {
  name = "${var.function_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "s3_read" {
  name = "${var.function_name}-s3-read"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:HeadObject"
      ]
      Resource = "${var.source_bucket_arn}/*"
    }]
  })
}

resource "aws_iam_role_policy" "s3_write" {
  name = "${var.function_name}-s3-write"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:PutObject"
      ]
      Resource = "${var.destination_bucket_arn}/*"
    }]
  })
}

resource "aws_iam_role_policy" "github_actions_layer_access" {
  name = "${var.function_name}-s3-write"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "lambda:GetLayerVersion"
      ]
      Resource = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:16"
    }]
  })
}

resource "aws_lambda_function" "this" {
  function_name = var.function_name
  role          = aws_iam_role.this.arn

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  handler = "processor.lambda_handler"
  runtime = "python3.12"

  timeout     = 300
  memory_size = 512

  layers = ["arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:16"]

  environment {
    variables = var.environment_variables
  }
}