# AWS Region
variable "aws_region" {
  type        = string
  description = "AWS region"
  default     = "eu-west-1"
}

# Nombre del bucket
variable "s3_bucket_name" {
  type        = string
  description = "Nombre del bucket S3 para el Data Lake"
}

# Nombre de la Glue Database
variable "glue_database_name" {
  type        = string
  description = "Nombre de la base de datos de Glue"
  default     = "data_lake_db"
}

# Entorno
variable "environment" {
  type        = string
  description = "Environment (dev, prod, etc.)"
  default     = "dev"
}