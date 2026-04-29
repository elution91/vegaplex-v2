from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="VEGAPLEX_",
        extra="ignore",
    )

    def __hash__(self) -> int:  # needed for FastAPI Depends cache
        return id(self)

    # Data source
    data_source: Literal["yfinance", "ibkr"] = "yfinance"

    # IBKR
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 7497
    ibkr_client_id: int = 1

    # Polygon
    polygon_api_key: str = ""

    # CORS origins — comma-separated in env (env: VEGAPLEX_CORS_ORIGINS)
    # Stored as a string so pydantic-settings doesn't JSON-parse it.
    # Use the `cors_origins` property to get a list.
    cors_origins: str = Field(
        default="http://localhost:5173,http://127.0.0.1:5173",
    )

    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    # Shared-password gate for beta access. Empty string disables auth (local dev).
    # Set VEGAPLEX_AUTH_PASSWORD in production to require it.
    auth_password: str = ""

    # Analytics path (resolved relative to this file at runtime)
    analytics_dir: Path = Path(__file__).parent.parent / "analytics"

    # Scheduler
    scheduler_timezone: str = "UTC"
    polygon_seed_cron: str = "0 6 * * *"  # 06:00 UTC daily

    @classmethod
    def from_config_json(cls, path: Path | None = None) -> "Settings":
        """Load from config.json (v1 format) with env override."""
        overrides: dict = {}
        if path is None:
            path = Path(__file__).parent.parent / "analytics" / "config.json"
        if path.exists():
            raw = json.loads(path.read_text())
            overrides = {
                k: v
                for k, v in raw.items()
                if k in cls.model_fields
            }
        return cls(**overrides)


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings.from_config_json()
    return _settings

