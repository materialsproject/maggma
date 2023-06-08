from pydantic import BaseSettings, Field


class CLISettings(BaseSettings):
    WORKER_TIMEOUT: int = Field(
        None,
        description="Timeout in seconds for a distributed worker",
    )

    MANAGER_TIMEOUT: int = Field(
        3600,
        description="Timeout in seconds for the worker manager",
    )

    class Config:
        env_prefix = "MAGGMA_"
        extra = "ignore"
