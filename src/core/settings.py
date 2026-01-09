from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # App
    APP_NAME: str = "data-platform"
    ENV: str = Field(default="dev")  # dev | prod | test
    DEBUG: bool = False

    # Paths
    BASE_DIR: Path = Path(__file__).resolve().parents[2]
    LOG_DIR: Path = BASE_DIR / "logs"
    TMP_DIR: Path = BASE_DIR / "tmp"

    # Database
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "test_biomont"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "root"

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+psycopg2://"
            f"{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
