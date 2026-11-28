output "instance_id" {
  description = "Id of the host instance."
  value       = aws_instance.host.id
}

output "instance_arn" {
  description = "ARN of the host instance."
  value       = aws_instance.host.arn
}

output "deploy_document_name" {
  description = "Command document which the deployment workflow sends to the host."
  value       = aws_ssm_document.deploy.name
}

output "deploy_document_arn" {
  description = "ARN of the command document."
  value       = aws_ssm_document.deploy.arn
}
