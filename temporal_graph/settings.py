from __future__ import annotations

from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    neo4j_uri: str = Field(default="bolt://localhost:7687", alias="NEO4J_URI")
    neo4j_user: str = Field(default="neo4j", alias="NEO4J_USER")
    neo4j_password: str = Field(default="changeme", alias="NEO4J_PASSWORD")
    neo4j_database: str | None = Field(default=None, alias="NEO4J_DATABASE")

    llm_config_path: Path = Field(
        default_factory=lambda: _repo_root() / "llm_config.yml",
        alias="LLM_CONFIG_PATH",
    )
    ontologies_dir: Path = Field(
        default_factory=lambda: _repo_root() / "ontologies",
        alias="ONTOLOGIES_DIR",
    )
    predicates_path: Path = Field(
        default_factory=lambda: _repo_root() / "predicates" / "default.yml",
        alias="PREDICATES_PATH",
    )
    predicate_groups_path: Path = Field(
        default_factory=lambda: _repo_root() / "predicates" / "groups.yml",
        alias="PREDICATE_GROUPS_PATH",
    )

    llm_service_base_url: str = Field(
        default="http://127.0.0.1:8000",
        alias="LLM_SERVICE_BASE_URL",
    )
    llm_processing_timeout_seconds: float = Field(default=120.0, alias="LLM_PROCESSING_TIMEOUT_SECONDS")
    llm_processing_connect_timeout_seconds: float = Field(
        default=10.0, alias="LLM_PROCESSING_CONNECT_TIMEOUT_SECONDS"
    )
    llm_processing_max_retries: int = Field(default=3, alias="LLM_PROCESSING_MAX_RETRIES")
    llm_processing_retry_backoff_seconds: float = Field(
        default=0.5, alias="LLM_PROCESSING_RETRY_BACKOFF_SECONDS"
    )

    ingest_max_concurrent_jobs: int = Field(default=2, alias="INGEST_MAX_CONCURRENT_JOBS")
    job_webhook_signing_secret: str = Field(default="", alias="JOB_WEBHOOK_SIGNING_SECRET")
    job_backend: str = Field(default="memory", alias="JOB_BACKEND")
    redis_url: str = Field(default="", alias="REDIS_URL")
    redis_job_queue_key: str = Field(default="ingest:queue", alias="REDIS_JOB_QUEUE_KEY")
    redis_job_key_prefix: str = Field(default="ingest:job", alias="REDIS_JOB_KEY_PREFIX")
    ingest_start_redis_worker: bool = Field(default=True, alias="INGEST_START_REDIS_WORKER")

    default_invalidation_publish_date_threshold_hours: float = Field(
        default=12.0,
        alias="DEFAULT_INVALIDATION_PUBLISH_DATE_THRESHOLD_HOURS",
        description="Fallback hours when ontology default_publish_date_threshold_hours is 0 or missing.",
    )

    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")

    @field_validator("llm_config_path", "ontologies_dir", "predicates_path", "predicate_groups_path", mode="after")
    @classmethod
    def _resolve_repo_paths(cls, v: Path) -> Path:
        return v if v.is_absolute() else (_repo_root() / v).resolve()


def get_settings() -> Settings:
    return Settings()
