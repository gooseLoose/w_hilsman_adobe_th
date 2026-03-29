module "hits_bucket_post" {
  source      = "./modules/s3"
  bucket_name = "hits-file-post-${var.environment}"
  environment = var.environment
}

module "hits_bucket_agg" {
  source      = "./modules/s3"
  bucket_name = "hits-file-agg-${var.environment}"
  environment = var.environment
}