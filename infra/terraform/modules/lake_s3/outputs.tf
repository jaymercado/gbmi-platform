output "lake_bucket" {
  description = "Name of the lake bucket."
  value       = aws_s3_bucket.this["lake"].bucket
}

output "lake_bucket_arn" {
  description = "ARN of the lake bucket."
  value       = aws_s3_bucket.this["lake"].arn
}

output "ops_bucket" {
  description = "Name of the ops bucket."
  value       = aws_s3_bucket.this["ops"].bucket
}

output "ops_bucket_arn" {
  description = "ARN of the ops bucket."
  value       = aws_s3_bucket.this["ops"].arn
}
