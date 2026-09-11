"""Environment and table-name configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


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


@dataclass(frozen=True)
class LakehouseNames:
    catalog: str = "energy_market"

    def table(self, layer: str, name: str) -> str:
        if layer not in {"bronze", "silver", "gold", "ops"}:
            raise ValueError(f"Unsupported layer: {layer}")
        return f"{self.catalog}.{layer}.{name}"
