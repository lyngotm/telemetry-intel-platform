"""
Configuration for the Anomaly Detection Service.
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
    kafka_topic_enriched: str = "telemetry.enriched"
    kafka_topic_anomalies: str = "anomalies.detected"
    kafka_consumer_group: str = "anomaly-detection-group"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Prometheus
    metrics_port: int = 9091

    log_level: str = "INFO"

    # Anomaly detection parameters
    window_size_seconds: int = 300  # 5-minute rolling window
    min_window_samples: int = 10  # Minimum readings before computing z-score
    z_score_threshold_low: float = 2.0
    z_score_threshold_medium: float = 2.5
    z_score_threshold_high: float = 3.0
    z_score_threshold_critical: float = 4.0

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
