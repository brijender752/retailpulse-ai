from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    app_name: str = "RetailPulse AI API"

    app_version: str = "1.0.0"

    postgres_host: str = "postgres"

    postgres_port: int = 5432

    postgres_db: str = "retailpulse"

    postgres_user: str = "postgres"

    postgres_password: str = "postgres"
    
    ollama_url: str = (
        "http://host.docker.internal:11434"
    )

    ollama_model: str = "qwen3:4b"

    ollama_timeout_seconds: float = 300.0

    ollama_num_predict: int = 384


settings = Settings()
