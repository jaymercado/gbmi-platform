"""GBMI platform: ingestion, processing and quality code shared by every pipeline."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("gbmi")
except PackageNotFoundError:  # a source tree that has not been installed
    __version__ = "0.0.0+unknown"

__all__ = ["__version__"]
