import json
import os
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]


class Provider(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    name: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,40}$")
    base_url: str
    model: str
    api_key_env: str
    capabilities: set[Literal["text", "tools", "json", "stream"]]
    concurrency: int = Field(default=4, ge=1, le=256)
    queue_capacity: int = Field(default=16, ge=0, le=10000)
    rpm: int = Field(default=120, ge=1)
    extra_body: dict = Field(default_factory=dict)
    api_key: str = Field(default="", exclude=True, repr=False)

    @model_validator(mode="after")
    def url_valid(self):
        p = urlsplit(self.base_url)
        if not p.hostname or p.username or p.password or p.query or p.fragment or not (
            p.scheme == "https" or p.scheme == "http" and p.hostname in {"127.0.0.1", "localhost", "::1"}
        ):
            raise ValueError("provider requires HTTPS or loopback HTTP URL without credentials")
        return self


class Routing(BaseModel):
    model_config = ConfigDict(extra="forbid")
    providers: list[Provider] = Field(min_length=1)
    routes: dict[str, list[str]]

    @model_validator(mode="after")
    def check_routes(self):
        names = {p.name for p in self.providers}
        if len(names) != len(self.providers):
            raise ValueError("duplicate provider name")
        if not self.routes or any(not v or len(v) != len(set(v)) or set(v) - names for v in self.routes.values()):
            raise ValueError("invalid route providers")
        return self


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GATEWAY_", env_file=".env", extra="ignore", hide_input_in_errors=True)
    mode: Literal["mock", "real"] = "mock"
    config: Path = ROOT / "configs" / "mock.json"
    api_key: SecretStr = SecretStr("demo-client-key")
    rate_backend: Literal["memory", "redis"] = "memory"
    redis_url: SecretStr = SecretStr("redis://127.0.0.1:6379/0")
    redis_prefix: str = "llmgw"
    deadline_seconds: float = Field(default=15, gt=0, le=300)
    queue_timeout_seconds: float = Field(default=3, gt=0, le=60)
    max_attempts: int = Field(default=3, ge=1, le=8)
    retry_delay_seconds: float = Field(default=0.05, ge=0, le=5)
    breaker_min_samples: int = Field(default=5, ge=1)
    breaker_failure_ratio: float = Field(default=0.5, gt=0, le=1)
    breaker_window_seconds: float = Field(default=30, gt=0)
    breaker_cooldown_seconds: float = Field(default=5, gt=0)

    def load_routing(self) -> Routing:
        routing = Routing.model_validate(json.loads(self.config.read_text(encoding="utf-8")))
        local_env = dotenv_values(".env")
        if not self.api_key.get_secret_value() or (self.mode == "real" and self.api_key.get_secret_value() == "demo-client-key"):
            raise ValueError("configure a private gateway client key in real mode")
        for p in routing.providers:
            key = os.environ.get(p.api_key_env, local_env.get(p.api_key_env, "")) or ""
            if self.mode == "mock":
                if urlsplit(p.base_url).hostname not in {"127.0.0.1", "localhost", "::1"}:
                    raise ValueError("mock mode requires loopback providers")
                key = key or "mock-upstream-key"
            elif not key:
                raise ValueError("missing configured upstream credential")
            p.api_key = key
        return routing
