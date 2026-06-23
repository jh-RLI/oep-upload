import os
from pathlib import Path
from typing import Literal, Optional, List
from pydantic import (
    BaseModel,
    AnyUrl,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


class Endpoint(BaseModel):
    host: str
    api_base_url: AnyUrl
    protocol: Literal["http", "https"] | None = None

    @model_validator(mode="after")
    def derive_protocol(self):
        # Fill protocol from the URL scheme when it isn't set explicitly.
        # Runs even when `protocol` is left at its default (unlike a field
        # validator), so a config that omits `protocol` still works.
        if self.protocol is None and self.api_base_url is not None:
            scheme = getattr(self.api_base_url, "scheme", None) or str(
                self.api_base_url
            ).split("://", 1)[0]
            if scheme in ("http", "https"):
                self.protocol = scheme
        return self


class APISettings(BaseModel):
    timeout_s: int = 30
    target: Literal["remote", "local"] = "remote"
    remote: Endpoint
    local: Endpoint


class AppSettings(BaseModel):
    name: str = "oep-uploader"
    timezone: str = "Europe/Berlin"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    # Optional path to also write logs to. A file path appends; a directory
    # (or a trailing slash) creates one timestamped file per run.
    log_file: Optional[str] = None


class PathsSettings(BaseModel):
    """Filesystem locations for the datapackage being processed.

    All three values accept ``~`` and ``$ENV_VAR`` references and may be given
    either as absolute paths or relative to the directory you run the tool
    from. ``data_dir`` and ``datapackage_file`` are resolved *relative to*
    ``root`` (an absolute value simply overrides ``root``), so in the common
    case you only need to set ``root``.

    Use the ``resolved_*`` properties instead of joining these by hand — they
    centralize expansion, joining and trailing-slash handling in one place.
    """

    root: str = "data"
    data_dir: Optional[str] = None
    datapackage_file: Optional[str] = None

    @field_validator("root", "data_dir", "datapackage_file", mode="before")
    @classmethod
    def _clean_path(cls, v, info: ValidationInfo):
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            # root must always have a value; the others are optional.
            return "data" if info.field_name == "root" else None
        return os.path.expanduser(os.path.expandvars(s))

    @property
    def resolved_root(self) -> Path:
        return Path(self.root).expanduser().resolve()

    @property
    def resolved_data_dir(self) -> Path:
        # An absolute data_dir overrides root; a relative one is joined to it.
        if self.data_dir:
            return (self.resolved_root / self.data_dir).resolve()
        return self.resolved_root

    @property
    def resolved_datapackage_file(self) -> Optional[Path]:
        # Absolute datapackage_file overrides data_dir; relative is joined to it.
        if self.datapackage_file:
            return (self.resolved_data_dir / self.datapackage_file).resolve()
        return None


class FileSettings(BaseModel):
    encoding: str = "utf-8"
    delimiter: str = ";"


class UploadSettings(BaseModel):
    batch_size: int = 5000
    dry_run: bool = False
    default_schema: str = "data"
    max_retries: int = 5
    retry_base_delay: float = 1.5
    null_tokens: List[str] = ["", "null", "none", "na", "nan", "n/a"]
    # "append": add rows to whatever is already in the table (default).
    # "replace": clear the table's existing rows first, for a fresh upload.
    strategy: Literal["append", "replace"] = "append"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=None,
        env_nested_delimiter="__",
        extra="ignore",
    )

    env: Literal["dev", "prod", "test"] = "prod"
    app: AppSettings
    api: APISettings
    paths: PathsSettings = PathsSettings()
    files: FileSettings = FileSettings()
    upload: UploadSettings = UploadSettings()

    oep_user: Optional[str] = None
    oep_api_token: Optional[str] = None
    oep_api_token_local: Optional[str] = None

    @property
    def endpoint(self) -> Endpoint:
        return self.api.local if self.api.target == "local" else self.api.remote

    @property
    def effective_api_token(self) -> Optional[str]:
        # For a local OEP prefer the dedicated local token, but fall back to
        # the main token so users don't have to set both for simple setups.
        if self.api.target == "local":
            return self.oep_api_token_local or self.oep_api_token
        return self.oep_api_token

    @property
    def oedialect_protocol(self) -> str:
        return self.endpoint.protocol or "https"
