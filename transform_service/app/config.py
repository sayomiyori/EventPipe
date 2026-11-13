from functools import lru_cache

from pydantic import Field
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

    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "minio"
    s3_secret_key: str = "minio12345"
    s3_bucket_raw: str = "raw-events"
    s3_region: str = "us-east-1"

    max_retries: int = Field(3, ge=1, le=10)
    retry_backoff_base_seconds: float = Field(0.5, ge=0, le=60)

    geo_ip_timeout_seconds: float = 2.0
    geo_ip_enabled: bool = False
    platform_mode: bool = False
    health_host: str = "0.0.0.0"
    health_port: int = 9101


@lru_cache
def get_settings() -> Settings:
    return Settings()
