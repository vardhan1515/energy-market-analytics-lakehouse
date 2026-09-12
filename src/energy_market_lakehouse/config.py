"""Environment and table-name configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol


class SecretGetter(Protocol):
    def get(self, *, scope: str, key: str) -> str: ...


class DatabricksUtilities(Protocol):
    secrets: SecretGetter


@dataclass(frozen=True)
class Settings:
    raw_data_dir: Path = Path("data/raw")
    miso_api_base_url: str = "https://apim.misoenergy.org"
    miso_public_api_base_url: str = "https://public-api.misoenergy.org"
    open_meteo_api_base_url: str = "https://archive-api.open-meteo.com"
    miso_api_token: str | None = None

    @classmethod
    def from_environment(cls) -> Settings:
        return cls(
            raw_data_dir=Path(os.getenv("ENERGY_RAW_DATA_DIR", "data/raw")),
            miso_api_base_url=os.getenv("MISO_API_BASE_URL", cls.miso_api_base_url).rstrip("/"),
            miso_public_api_base_url=os.getenv(
                "MISO_PUBLIC_API_BASE_URL", cls.miso_public_api_base_url
            ).rstrip("/"),
            open_meteo_api_base_url=os.getenv(
                "OPEN_METEO_API_BASE_URL", cls.open_meteo_api_base_url
            ).rstrip("/"),
            miso_api_token=os.getenv("MISO_API_TOKEN") or None,
        )

    @classmethod
    def from_databricks_secret(
        cls,
        dbutils: DatabricksUtilities,
        *,
        scope: str = "energy-market",
        key: str = "miso-api-token",
    ) -> Settings:
        """Load the MISO token from a Databricks-backed secret scope."""
        token = dbutils.secrets.get(scope=scope, key=key)
        if not token:
            raise ValueError(f"Databricks secret {scope}/{key} is empty")
        return replace(cls.from_environment(), miso_api_token=token)


@dataclass(frozen=True)
class LakehouseNames:
    catalog: str = "workspace"

    def table(self, layer: str, name: str) -> str:
        if layer not in {"bronze", "silver", "gold", "ops"}:
            raise ValueError(f"Unsupported layer: {layer}")
        return f"{self.catalog}.{layer}.{name}"
