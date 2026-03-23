from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EVENTPIPE_",
        env_file=".env",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_raw: str = "events.raw"
    grpc_host: str = "0.0.0.0"
    grpc_port: int = 50051
    api_host: str = "0.0.0.0"
    api_port: int = 8000


@lru_cache
def get_settings() -> Settings:
    return Settings()
