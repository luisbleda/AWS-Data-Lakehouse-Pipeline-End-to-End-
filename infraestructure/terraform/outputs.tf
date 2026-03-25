output "s3_bucket_name" {
  value       = aws_s3_bucket.data_lake.bucket
  description = "Nombre del bucket S3 creado"
}

output "glue_database_name" {
  value       = aws_glue_catalog_database.data_lake_db.name
  description = "Nombre de la base de datos Glue"
}

output "s3_bucket_arn" {
  value       = aws_s3_bucket.data_lake.arn
  description = "ARN del bucket S3"
}