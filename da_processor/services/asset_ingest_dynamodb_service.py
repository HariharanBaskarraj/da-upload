"""
DynamoDB service for asset ingestion operations.

This service handles all DynamoDB operations specific to the asset ingestion pipeline,
including tracking asset validation status, managing title and asset metadata, and
version control for asset updates.
"""
import logging
import uuid
import os
import boto3
from typing import Dict, Any, List, Optional
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from django.conf import settings
from da_processor.models.asset_models import TitleInfo, AssetInfo
from da_processor.utils.path_utils import normalize_s3_path

logger = logging.getLogger(__name__)


class AssetIngestDynamoDBService:
    """
    Service for managing DynamoDB operations during asset ingestion.

    This service provides methods for:
    - Scanning and querying ingest asset records by status
    - Managing title and asset information
    - Updating processing status for ingested assets
    - Version control for asset updates
    """

    def __init__(self, ingest_table_name: str, title_info_table_name: str, asset_info_table_name: str):
        """
        Initialize the Asset Ingest DynamoDB Service.

        Args:
            ingest_table_name: Name of the ingest asset tracking table
            title_info_table_name: Name of the title information table
            asset_info_table_name: Name of the asset information table
        """
        self.dynamodb = boto3.resource('dynamodb', region_name=settings.AWS_REGION)
        self.ingest_table = self.dynamodb.Table(ingest_table_name)
        self.title_info_table = self.dynamodb.Table(title_info_table_name)
        self.asset_info_table = self.dynamodb.Table(asset_info_table_name)

    def scan_ingest_items(self, status: str) -> List[Dict[str, Any]]:
        """
        Scan the ingest table for all items with a specific ProcessStatus.

        Args:
            status: The process status to filter by (e.g., 'VALID_STRUCTURE', 'PENDING')

        Returns:
            List of matching items from DynamoDB
        """
        logger.info(f"Scanning ingest table for status: {status}")
        try:
            response = self.ingest_table.query(
                IndexName='ProcessStatusIndex',
                KeyConditionExpression=Key('ProcessStatus').eq(status)
            )
            items = response.get("Items", [])
            logger.info(f"Found {len(items)} items with status {status}")
            return items
        except ClientError as e:
            logger.error(f"DynamoDB scan failed for status {status}: {e}")
            return []

    def get_assets_by_folder(self, folder_prefix: str) -> List[Dict[str, Any]]:
        """
        Get all assets belonging to a specific folder prefix.

        Args:
            folder_prefix: The folder path to filter assets by

        Returns:
            List of matching DynamoDB items
        """
        folder_prefix = normalize_s3_path(folder_prefix)
        try:
            response = self.ingest_table.scan(
                FilterExpression=Attr("AssetPath").eq(folder_prefix)
            )
            items = response.get("Items", [])
            logger.info(f"Found {len(items)} assets for folder: {folder_prefix}")
            return items
        except ClientError as e:
            logger.error(f"Failed to fetch assets for folder {folder_prefix}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in get_assets_by_folder: {e}")
            return []

    def update_process_status(self, folder_prefix: str, status: str) -> bool:
        """
        Update the ProcessStatus field for all items belonging to a folder.

        Args:
            folder_prefix: The folder to update
            status: The new status to set (e.g., 'SUCCESS', 'FAILED', 'INVALID_CSV')

        Returns:
            True if at least one item updated, else False
        """
        logger.info(f"Updating process status for folder: {folder_prefix} to {status}")
        normalized_path = normalize_s3_path(folder_prefix)
        assets = self.get_assets_by_folder(normalized_path)

        if not assets:
            logger.warning(f"No items found for folder {folder_prefix} to update status.")
            return False

        try:
            for asset in assets:
                self.ingest_table.update_item(
                    Key={'IngestId': asset['IngestId']},
                    UpdateExpression='SET ProcessStatus = :s',
                    ExpressionAttributeValues={':s': status}
                )
            logger.info(f"Updated {len(assets)} items to status {status} for folder {folder_prefix}")
            return True
        except Exception as e:
            logger.error(f"Failed to update ProcessStatus for {folder_prefix}: {e}")
            return False

    def put_title_info(self, item: Dict[str, Any]) -> bool:
        """
        Insert title info record with conditional check to prevent duplicates.

        Args:
            item: Title information dictionary

        Returns:
            True if successful, False otherwise
        """
        try:
            self.title_info_table.put_item(
                Item=item,
                ConditionExpression='attribute_not_exists(Title_ID) AND attribute_not_exists(Version_ID)'
            )
            logger.info(f"Inserted title info: Title_ID={item.get('Title_ID')}, Version_ID={item.get('Version_ID')}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                logger.warning(f"Title already exists: {item['Title_ID']}")
            else:
                logger.error(f"Error inserting title info: {e}")
            return False

    def put_asset_info(self, item: Dict[str, Any]) -> bool:
        """
        Insert asset info record.

        Args:
            item: Asset information dictionary

        Returns:
            True if successful, False otherwise
        """
        try:
            self.asset_info_table.put_item(Item=item)
            logger.debug(f"Inserted asset info: AssetId={item.get('AssetId')}")
            return True
        except ClientError as e:
            logger.error(f"Error inserting asset info: {e}")
            return False

    def get_next_version(self, folder_path: str, new_checksum: str, filename: str) -> Optional[int]:
        """
        Determine the next version number for an asset.

        Checks existing versions of the asset and increments if checksum differs.
        Returns None if exact duplicate (same folder, filename, and checksum).

        Args:
            folder_path: Folder path where the asset resides
            new_checksum: MD5 checksum of the new asset
            filename: Name of the asset file

        Returns:
            Next version number, or None if duplicate
        """
        normalized = normalize_s3_path(folder_path)
        logger.debug(f"Checking version for: {normalized}/{filename}")

        try:
            response = self.asset_info_table.query(
                IndexName="FolderPathIndex",
                KeyConditionExpression=Key("Folder_Path").eq(normalized)
            )

            items = response.get("Items", [])
            if not items:
                logger.info(f"No existing assets found for {normalized}, starting at version 1")
                return 1

            # Check for exact duplicate (same folder, filename, and checksum)
            for item in items:
                if (
                    normalize_s3_path(item.get("Folder_Path")) == normalized and
                    item.get("Filename") == filename and
                    item.get("Checksum") == new_checksum
                ):
                    logger.info(f"Exact duplicate found for {normalized}/{filename}, skipping")
                    return None

            # Get latest version and increment
            latest = max(items, key=lambda x: x.get("Version", 1))
            latest_checksum = latest.get("Checksum")

            if latest_checksum == new_checksum:
                logger.info(f"Checksum unchanged for {normalized}, skipping version increment")
                return None
            else:
                new_version = latest.get("Version", 1) + 1
                logger.info(f"Incrementing version to {new_version} for {normalized}/{filename}")
                return new_version

        except ClientError as e:
            logger.error(f"Error fetching version for {folder_path}: {e}")
            return 1
