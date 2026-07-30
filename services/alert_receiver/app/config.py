"""
Configuration for the Alert Receiver Service.
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

    # Service ports
    api_port: int = 8000
    metrics_port: int = 9097

    # Logging
    log_level: str = "INFO"

    # Prometheus (for hot-reload of custom rules)
    prometheus_url: str = "http://localhost:9094"

    # AI Triage (optional — only invoked on critical alerts)
    # When enabled, critical alerts trigger LLM-based triage
    triage_enabled: bool = True
    claude_model_id: str = ""
    claude_embedding_model_id: str = ""
    aws_region: str = "us-east-2"
    chroma_host: str = "localhost"
    chroma_port: int = 8000

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
