from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QUERY_", env_file=".env", extra="ignore")

    api_host: str = "0.0.0.0"
    api_port: int = 8020

    database_url: str = "postgresql+asyncpg://eventpipe:eventpipe@localhost:5434/eventpipe"

    s3_endpoint_url: str = "http://localhost:9000"
    s3_public_url: str = "http://localhost:9000"
    s3_access_key: str = "minio"
    s3_secret_key: str = "minio12345"
    s3_bucket_raw: str = "raw-events"
    s3_region: str = "us-east-1"

    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_topic_dlq: str = "events.dlq"


@lru_cache
def get_settings() -> Settings:
    return Settings()
