# Databricks notebook source
"""Expose the src-layout package without installing over Databricks' runtime."""

import sys
from pathlib import Path


def find_project_root() -> Path:
    for candidate in (Path.cwd(), *Path.cwd().parents):
        if (candidate / "src" / "energy_market_lakehouse").is_dir():
            return candidate
    raise FileNotFoundError("Could not locate the energy lakehouse Git-folder root")


source_root = str(find_project_root() / "src")
if source_root not in sys.path:
    sys.path.insert(0, source_root)
