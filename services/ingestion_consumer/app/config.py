"""
Configuration for the Ingestion Consumer service.
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
    kafka_consumer_group: str = "ingestion-consumer-group"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Prometheus
    metrics_port: int = 9090

    log_level: str = "INFO"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
