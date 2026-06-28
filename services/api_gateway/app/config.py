"""
Configuration for the API Gateway service.
Loads settings from environment variables (with .env file support).
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "telemetry"
    postgres_user: str = "telemetry_user"
    postgres_password: str = "telemetry_pass"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9093"
    kafka_topic_raw: str = "telemetry.raw"
    kafka_topic_enriched: str = "telemetry.enriched"
    kafka_topic_dlq: str = "telemetry.dlq"
    kafka_publish_timeout_seconds: float = 5.0

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # JWT Authentication
    jwt_private_key_path: str = "keys/private.pem"
    jwt_public_key_path: str = "keys/public.pem"
    jwt_algorithm: str = "RS256"
    jwt_expiry_minutes: int = 60

    # Rate Limiting (requests per minute)
    rate_limit_ingestion: int = 1000
    rate_limit_query: int = 100

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # ChromaDB (Vector Store)
    chroma_host: str = "localhost"
    chroma_port: int = 8100

    # AWS Bedrock
    aws_region: str = "us-west-2"
    bedrock_model_id: str = ""
    bedrock_embedding_model_id: str = ""
    embedding_provider: str = "bedrock"  # "bedrock" or "local"

    log_level: str = "INFO"

    @property
    def database_url(self) -> str:
        """Constructs the asyncpg connection string."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


# Singleton instance — import this wherever you need config
settings = Settings()
