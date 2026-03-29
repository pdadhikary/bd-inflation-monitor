from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    db_user: str
    db_password: str
    db_host: str
    db_port: int
    db_name: str
    api_url: str
    bbs_url: str = "https://bbs.gov.bd/pages/static-pages/6922de7a933eb65569e1ae8f"
    stage_dir: str = "data/staged"
    processed_dir: str = "data/processed"
    log_dir: str = "logs"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.db_user}:"
            f"{self.db_password}@{self.db_host}:"
            f"{self.db_port}/{self.db_name}"
        )

    @property
    def database_info(self) -> str:
        return (
            f"dbname={self.db_name} "
            f"user={self.db_user} "
            f"password={self.db_password} "
            f"host={self.db_host} "
            f"port={self.db_port}"
        )

    class Config:
        env_file = ".env"


settings = Settings()

__all__ = ["settings"]
