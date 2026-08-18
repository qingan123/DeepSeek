from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "DeepSeek Web OpenAI Proxy"
    app_host: str = "0.0.0.0"
    app_port: int = 60089
    app_secret: str = "change-me"
    admin_password: str = "change-me"
    database_url: str = "sqlite:///./data/app.db"
    log_dir: str = "./logs"
    baidu_base_url: str = "https://chat.baidu.com"
    deepseek_base_url: str = "https://chat.deepseek.com"
    default_upstream_timeout: int = 120

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    @property
    def log_path(self) -> Path:
        return Path(self.log_dir)


@lru_cache
def get_settings() -> Settings:
    return Settings()
