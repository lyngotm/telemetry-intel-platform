# Terraform — AWS Production Infrastructure

Defines the cloud infrastructure for the Telemetry Intelligence Platform.

## Resources

| Resource | AWS Service | Purpose | Estimated Monthly Cost |
|----------|-------------|---------|----------------------|
| VPC | VPC + Subnets + NAT | Network isolation, public/private separation | ~$35 (NAT Gateway) |
| EKS | Elastic Kubernetes Service | Container orchestration, auto-scaling | ~$73 (control plane) + node costs |
| EKS Nodes | EC2 (t3.medium × 2) | Run application pods | ~$60 |
| RDS | RDS PostgreSQL (db.t3.medium) | Primary data store (devices, telemetry, anomalies) | ~$50 |
| ElastiCache | ElastiCache Redis (cache.t3.micro) | Cache, rolling windows, rate limiting, metadata | ~$12 |
| MSK | Managed Streaming for Kafka (t3.small × 2) | Event backbone (telemetry.raw, enriched, dlq, anomalies) | ~$130 |
| ECR | Elastic Container Registry | Docker image storage (4 repos) | ~$1 |
| IAM | IAM Roles + Policies | EKS node permissions, Bedrock access (Week 5) | Free |

**Estimated total: ~$360/month** (dev configuration, single-AZ where possible)

## Usage

```bash
cd terraform

# Initialize (downloads AWS provider)
terraform init

# Validate syntax
terraform validate

# Preview what would be created (no cost)
terraform plan -var-file="terraform.tfvars"

# Deploy (costs money!)
terraform apply -var-file="terraform.tfvars"

# Tear down everything
terraform destroy -var-file="terraform.tfvars"
