from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TRANSFORM_",
        env_file=".env",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_topic_raw: str = "events.raw"
    kafka_topic_dlq: str = "events.dlq"
    kafka_group_id: str = "transform-workers"

    database_url: str = "postgresql+asyncpg://eventpipe:eventpipe@localhost:5432/eventpipe"

    minio_endpoint_url: str = "http://localhost:9000"
    minio_access_key: str = "minio"
    minio_secret_key: str = "minio12345"
    minio_bucket_raw: str = "raw-events"
    minio_region: str = "us-east-1"

    max_retries: int = 3
    retry_backoff_base_seconds: float = 0.5

    geo_ip_timeout_seconds: float = 2.0
    metrics_host: str = "0.0.0.0"
    metrics_port: int = 9101


@lru_cache
def get_settings() -> Settings:
    return Settings()
