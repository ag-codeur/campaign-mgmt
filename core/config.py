from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    groq_api_key: str = ""
    langsmith_api_key: str = ""
    langsmith_project: str = "campaign-mgmt"
    database_url: str = "sqlite:///./campaign.db"
    chroma_persist_dir: str = "./chroma_db"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    environment: str = "local"  # local | gcp

    class Config:
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    return Settings()