# terraform/outputs.tf
# ============================================================
# Useful outputs after deployment.
# Access via: terraform output <name>
# ============================================================

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "eks_cluster_endpoint" {
  description = "EKS cluster API endpoint"
  value       = aws_eks_cluster.main.endpoint
}

output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = aws_eks_cluster.main.name
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint (hostname:port)"
  value       = aws_db_instance.postgres.endpoint
}

output "rds_database_name" {
  description = "RDS database name"
  value       = aws_db_instance.postgres.db_name
}

output "redis_endpoint" {
  description = "ElastiCache Redis endpoint"
  value       = aws_elasticache_cluster.redis.cache_nodes[0].address
}

output "msk_bootstrap_brokers" {
  description = "MSK Kafka bootstrap broker connection string"
  value       = aws_msk_cluster.main.bootstrap_brokers
}

output "ecr_repository_urls" {
  description = "ECR repository URLs for each service"
  value       = { for k, v in aws_ecr_repository.services : k => v.repository_url }
}

output "app_role_arn" {
  description = "IAM role ARN for application pods (annotate K8s ServiceAccount with this)"
  value       = aws_iam_role.app_role.arn
}
