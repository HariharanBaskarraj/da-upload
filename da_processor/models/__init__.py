"""
Data models for the DA Processor application.

This package contains all data model classes used throughout the application
for representing domain entities and facilitating data transfer between services.
"""
from .asset_models import IngestAsset, TitleInfo, AssetInfo

__all__ = ['IngestAsset', 'TitleInfo', 'AssetInfo']
