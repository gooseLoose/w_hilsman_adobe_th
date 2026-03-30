module "hits_bucket_post" {
  source = "./modules/s3"
  bucket_name = "hits-file-post-${var.environment}"
  environment = var.environment
}

module "hits_bucket_agg" {
  source = "./modules/s3"
  bucket_name = "hits-file-agg-${var.environment}"
  environment = var.environment
}

module "space_bloom_hit_processer" {
  source = "./modules/lambda"
  function_name = "space_bloom_hit_processer-${var.environment}"
  environment = var.environment
  src_dir = "${path.root}/../processer"

  source_bucket_arn = module.hits_bucket_post.bucket_arn
  destination_bucket_arn = module.hits_bucket_agg.bucket_arn

  
  environment_variables = {
    ENVIRONMENT = var.environment
  }
}

resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowS3Invoke"
  action = "lambda:InvokeFunction"
  function_name = module.space_bloom_hit_processer.lambda_name
  principal = "s3.amazonaws.com"
  source_arn = module.hits_bucket_post.bucket_arn
}

resource "aws_s3_bucket_notification" "raw_trigger" {
  bucket = module.hits_bucket_post.bucket_id

  lambda_function {
    lambda_function_arn = module.space_bloom_hit_processer.lambda_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".tsv"
  }
  
  depends_on = [aws_lambda_permission.allow_s3]
}