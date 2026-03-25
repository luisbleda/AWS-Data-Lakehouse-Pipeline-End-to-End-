provider "aws" {
  region = var.aws_region
}

# Bucket S3 para el Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = var.s3_bucket_name

  versioning {
    enabled = true
  }

  tags = {
    Name        = "DataLake"
    Environment = var.environment
  }
}

# Glue Database
resource "aws_glue_catalog_database" "data_lake_db" {
  name = var.glue_database_name
}

# Opcional: permisos públicos bloqueados
resource "aws_s3_bucket_public_access_block" "block_public" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}