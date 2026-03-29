terraform {
  backend "s3" {
    bucket         = "wh-tf-adobe-backend"
    key            = "terraform.tfstate"
    region         = "us-east-2"
    encrypt        = true
  }
}