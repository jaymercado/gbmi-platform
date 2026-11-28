output "vpc_id" {
  description = "Id of the VPC."
  value       = aws_vpc.this.id
}

output "public_subnet_id" {
  description = "Id of the public subnet of the host."
  value       = aws_subnet.public.id
}

output "host_security_group_id" {
  description = "Id of the security group of the host: no inbound rule, outbound HTTPS."
  value       = aws_security_group.host.id
}
