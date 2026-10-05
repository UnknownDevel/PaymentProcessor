from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "info"
    reload: bool = True
    api_key: SecretStr = SecretStr("development-api-key")
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres"
    rabbitmq_url: str = "amqp://rabbit:rabbit@localhost:5672/"
    outbox_batch_size: int = Field(default=32, ge=1)
    outbox_poll_interval: float = Field(default=1.0, gt=0)
    broker_publish_timeout: float = Field(default=10.0, gt=0)
    webhook_timeout: float = Field(default=10.0, gt=0)
    retry_base_delay: float = Field(default=1.0, gt=0)

    @field_validator("database_url")
    @classmethod
    def use_async_driver(cls, value: str) -> str:
        if value.startswith("postgres://"):
            return value.replace("postgres://", "postgresql+asyncpg://", 1)
        if value.startswith("postgresql://"):
            return value.replace("postgresql://", "postgresql+asyncpg://", 1)
        return value

    @field_validator("api_key")
    @classmethod
    def require_api_key(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("API_KEY must not be empty")
        return value


settings = Settings()
