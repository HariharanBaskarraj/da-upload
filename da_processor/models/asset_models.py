"""
Data models for asset ingestion and management.

This module defines dataclass models that represent assets, titles, and related
metadata throughout the ingestion and validation pipeline. These models provide
a structured way to handle data transfer to/from DynamoDB and between services.
"""
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any


@dataclass
class IngestAsset:
    """
    Model representing an asset in the ingestion pipeline.

    This model tracks assets as they move through the ingestion process,
    including their S3 location, checksum, and processing status.

    Attributes:
        IngestId: Unique identifier for the ingest record
        S3ObjectId: S3 object identifier
        AssetPath: Full S3 path to the asset
        Checksum: MD5 checksum of the asset file
        UploadedBy: User or system that uploaded the asset
        CreatedDate: ISO 8601 timestamp of creation
        ModifiedDate: ISO 8601 timestamp of last modification
        ModifiedBy: User or system that last modified the record
        FolderStructure: Folder hierarchy information
        ComponentId: Associated component identifier
        ProcessStatus: Current status in the processing pipeline
    """
    IngestId: str
    S3ObjectId: str
    AssetPath: str
    Checksum: str
    UploadedBy: str
    CreatedDate: str
    ModifiedDate: str
    ModifiedBy: str
    FolderStructure: str
    ComponentId: str
    ProcessStatus: str

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the model to a dictionary for DynamoDB operations.

        Returns:
            Dictionary representation of the model
        """
        return asdict(self)


@dataclass
class TitleInfo:
    """
    Model representing title information from CSV metadata.

    This model captures all title-level information including version details
    and EIDR identifiers for content tracking and rights management.

    Attributes:
        Title_ID: Unique title identifier
        Uploader: User or system that uploaded the title
        Title_Name: Human-readable title name (optional)
        Title_EIDR_ID: Entertainment Identifier Registry ID for title (optional)
        Version_Name: Version name/description (optional)
        Version_ID: Version identifier (optional)
        Version_EIDR_ID: EIDR ID for specific version (optional)
        Release_Year: Year of release (optional)
    """
    Title_ID: str
    Uploader: str
    Title_Name: Optional[str] = None
    Title_EIDR_ID: Optional[str] = None
    Version_Name: Optional[str] = None
    Version_ID: Optional[str] = None
    Version_EIDR_ID: Optional[str] = None
    Release_Year: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for DynamoDB, filtering out None values.

        Returns:
            Dictionary with only non-None values
        """
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class AssetInfo:
    """
    Model representing detailed asset information and metadata.

    This model stores comprehensive information about individual assets,
    including their folder location, checksums, version tracking, and
    studio-provided metadata for revision tracking.

    Attributes:
        AssetId: Unique asset identifier
        Title_ID: Associated title identifier
        Version_ID: Associated version identifier (optional)
        Creation_Date: ISO 8601 timestamp of asset creation (optional)
        Filename: Name of the asset file (optional)
        Checksum: MD5 checksum for integrity verification (optional)
        Folder_Path: S3 folder path where asset resides (optional)
        Studio_Revision_Notes: Notes from studio about revisions (optional)
        Studio_Revision_Urgency: Urgency level for revision (optional)
        Studio_Asset_ID: Studio's internal asset identifier (optional)
        Studio_System_Name: Name of studio's source system (optional)
        Version: Version number for tracking asset iterations (optional)
    """
    AssetId: str
    Title_ID: str
    Version_ID: Optional[str] = None
    Creation_Date: Optional[str] = None
    Filename: Optional[str] = None
    Checksum: Optional[str] = None
    Folder_Path: Optional[str] = None
    Studio_Revision_Notes: Optional[str] = None
    Studio_Revision_Urgency: Optional[str] = None
    Studio_Asset_ID: Optional[str] = None
    Studio_System_Name: Optional[str] = None
    Version: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for DynamoDB, filtering out None values.

        Returns:
            Dictionary with only non-None values
        """
        return {k: v for k, v in asdict(self).items() if v is not None}
