from typing import Literal, Optional, List
from pydantic import BaseModel, AnyUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Endpoint(BaseModel):
    host: str
    api_base_url: AnyUrl
    protocol: Literal["http", "https"] | None = None

    @field_validator("protocol", mode="before")
    @classmethod
    def derive_protocol(cls, v, values):
        if v is None and (u := values.get("api_base_url")):
            return u.scheme
        return v


class APISettings(BaseModel):
    timeout_s: int = 30
    target: Literal["remote", "local"] = "remote"
    remote: Endpoint
    local: Endpoint


class AppSettings(BaseModel):
    name: str = "oep-uploader"
    timezone: str = "Europe/Berlin"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"


class PathsSettings(BaseModel):
    root: str = "."
    data_dir: str = "data"
    datapackage_file: Optional[str] = (
        None  # e.g. data/datapackages/example/oed_example.json
    )


class UploadSettings(BaseModel):
    batch_size: int = 500
    dry_run: bool = False
    default_schema: str = "model_draft"
    max_retries: int = 5
    retry_base_delay: float = 1.5
    null_tokens: List[str] = ["", "null", "none", "na", "nan", "n/a"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
    )

    env: Literal["dev", "prod", "test"] = "dev"
    app: AppSettings
    api: APISettings
    paths: PathsSettings = PathsSettings()
    upload: UploadSettings = UploadSettings()

    oep_user: Optional[str] = None
    oep_api_token: Optional[str] = None
    oep_api_token_local: Optional[str] = None

    @property
    def endpoint(self) -> Endpoint:
        return self.api.local if self.api.target == "local" else self.api.remote

    @property
    def effective_api_token(self) -> Optional[str]:
        if self.api.target == "local":
            return self.oep_api_token_local or self.oep_api_token
        return self.oep_api_token

    @property
    def oedialect_protocol(self) -> str:
        return self.endpoint.protocol or "https"
