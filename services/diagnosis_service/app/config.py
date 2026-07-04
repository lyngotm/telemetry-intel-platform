"""
Configuration for the Diagnosis Service.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "telemetry"
    postgres_user: str = "telemetry_user"
    postgres_password: str = "telemetry_pass"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9093"
    kafka_topic_anomalies: str = "anomalies.detected"
    kafka_consumer_group: str = "diagnosis-service-group"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # ChromaDB
    chroma_host: str = "localhost"
    chroma_port: int = 8100

    # AWS Bedrock
    aws_region: str = ""
    bedrock_model_id: str = ""
    bedrock_embedding_model_id: str = ""
    embedding_provider: str = "bedrock"  # "bedrock" or "local"

    # RAG Configuration
    context_window_minutes: int = 30  # How far back to look for recent telemetry
    retrieval_top_k: int = 5  # Number of ChromaDB results to retrieve

    # Metrics
    metrics_port: int = 9092

    # Logging
    log_level: str = "INFO"

    @property
    def database_url(self) -> str:
        """Constructs the asyncpg connection string."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


# Singleton instance
settings = Settings()
