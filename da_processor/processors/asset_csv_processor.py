"""
CSV processing for asset ingestion pipeline.

This processor handles validation and processing of CSV files during asset ingestion,
extracting title metadata and asset information for storage in DynamoDB.
"""
import csv
import io
import uuid
import logging
from typing import List, Dict, Tuple
from datetime import datetime, timezone
from django.conf import settings
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from da_processor.models.asset_models import TitleInfo, AssetInfo
from da_processor.services.asset_ingest_dynamodb_service import AssetIngestDynamoDBService
from da_processor.utils.path_utils import normalize_s3_path

logger = logging.getLogger(__name__)


class AssetCSVProcessor:
    """
    Processor for CSV files in the asset ingestion pipeline.

    This processor validates CSV structure, extracts title and asset metadata,
    and stores information in DynamoDB with version tracking.
    """

    def __init__(self, db_service: AssetIngestDynamoDBService):
        """
        Initialize the Asset CSV Processor.

        Args:
            db_service: DynamoDB service instance for asset ingestion operations
        """
        self.db_service = db_service
        self.current_title_id = None
        self.current_version_id = None

    def validate_csv(self, csv_content: bytes) -> bool:
        """
        Validate CSV structure and required fields.

        Checks for presence of required title section fields and asset section columns.

        Args:
            csv_content: CSV file content as bytes

        Returns:
            True if valid, False otherwise
        """
        title_required = ["Title Name", "Title ID", "Version Name", "Version ID", "Release Year"]
        asset_required = ["Folder Path", "Filename", "Checksum"]

        logger.debug("Validating CSV structure")
        text = csv_content.decode("utf-8-sig")
        rows = list(csv.reader(io.StringIO(text)))

        # Split sections - find where asset section begins
        title_section = []
        asset_section = []

        for idx, row in enumerate(rows):
            if any("Filename" in cell for cell in row):
                asset_section = rows[idx:]  # header + data rows
                title_section = rows[:idx]  # everything before
                break

        # Validate title section (key-value pairs)
        title_kv = {}
        for r in title_section:
            if len(r) >= 2 and r[0].strip():
                title_kv[r[0].strip()] = r[1].strip()

        missing_titles = [t for t in title_required if t not in title_kv or not title_kv[t]]
        if missing_titles:
            logger.error(f"Missing required title fields: {missing_titles}")
            return False

        # Validate asset section
        asset_reader = csv.DictReader(io.StringIO("\n".join([",".join(r) for r in asset_section])))
        asset_fields = asset_reader.fieldnames

        missing_assets = [a for a in asset_required if a not in asset_fields]
        if missing_assets:
            logger.error(f"Missing required asset columns: {missing_assets}")
            return False

        # Validate each asset row has required fields
        for i, row in enumerate(asset_reader, start=2):
            for field in asset_required:
                if not row.get(field):
                    logger.error(f"Empty value for {field} in asset row {i}")
                    return False

        logger.info("CSV validation successful")
        return True

    def process_csv(self, csv_content: bytes, asset_path: str) -> bool:
        """
        Process CSV content and insert into DynamoDB tables.

        Args:
            csv_content: CSV file content as bytes
            asset_path: S3 path of the CSV file

        Returns:
            True if successful, False otherwise
        """
        try:
            text_content = csv_content.decode('utf-8')
            reader = list(csv.reader(io.StringIO(text_content)))

            logger.info(f"Processing CSV with {len(reader)} rows from {asset_path}")

            # Process title info (rows 2-11)
            self._process_title_info(reader, asset_path)

            # Process asset info (rows 13+)
            self._process_asset_info(reader)

            return True

        except Exception as e:
            logger.error(f"Error processing CSV: {e}", exc_info=True)
            return False

    def _process_title_info(self, reader: List[List[str]], asset_path: str) -> bool:
        """
        Process rows 2-11 for title information.

        Args:
            reader: CSV reader rows
            asset_path: S3 path of the CSV file

        Returns:
            True if successful, False otherwise
        """
        title_data = {}

        # Read rows 2–11 (Python index 1–10)
        for i, row in enumerate(reader[1:11], start=2):
            if not row or len(row) < 2:
                continue

            field_name = row[0].strip() if len(row) > 0 else None
            field_value = row[1].strip() if len(row) > 1 else None

            if field_name and field_value:
                title_data[field_name] = field_value

        if not title_data:
            logger.error(f"No title data found in rows 2-11 for {asset_path}")
            return False

        # Create TitleInfo record
        title_info = TitleInfo(
            Title_ID=title_data.get("Title ID"),
            Uploader=title_data.get("Uploader", "SYSTEM"),
            Title_Name=title_data.get("Title Name"),
            Title_EIDR_ID=title_data.get("Title EIDR ID"),
            Version_Name=title_data.get("Version Name"),
            Version_ID=title_data.get("Version ID"),
            Version_EIDR_ID=title_data.get("Version EIDR ID"),
            Release_Year=title_data.get("Release Year")
        )

        success = self.db_service.put_title_info(title_info.to_dict())
        self.current_title_id = title_data.get("Title ID")
        self.current_version_id = title_data.get("Version ID")

        if success:
            logger.info(f"Inserted/updated title info for Title_ID={self.current_title_id}")
        else:
            logger.warning(f"Title info already exists for Title_ID={self.current_title_id}")

        return True

    def _process_asset_info(self, reader: List[List[str]]) -> None:
        """
        Process rows 13+ for asset information.

        Args:
            reader: CSV reader rows
        """
        logger.debug("Processing asset info rows (13+)")

        # Skip first 12 rows, process from row 13 onwards
        for i, row in enumerate(reader[12:], start=13):
            if not row or len(row) < 3:
                continue

            folder_path = normalize_s3_path(row[3]) if len(row) > 3 else None
            checksum = row[2].strip('"') if len(row) > 2 and row[2] else None
            filename = row[1] if len(row) > 1 else None

            if not folder_path or not checksum:
                logger.warning(f"Skipping row {i}: missing Folder Path or Checksum")
                continue

            # Determine version (returns None for duplicates)
            version = self.db_service.get_next_version(folder_path, checksum, filename)

            if version is None:
                logger.debug(f"Skipping duplicate asset: {folder_path}/{filename}")
                continue

            logger.debug(f"Processing asset row {i}: {filename} (version {version})")

            asset_info = AssetInfo(
                AssetId=str(uuid.uuid4()),
                Title_ID=self.current_title_id,
                Version_ID=self.current_version_id,
                Creation_Date=row[0] if len(row) > 0 else datetime.now(timezone.utc).isoformat(),
                Filename=filename,
                Checksum=checksum,
                Folder_Path=folder_path,
                Studio_Revision_Notes=row[4] if len(row) > 4 else None,
                Studio_Revision_Urgency=row[5] if len(row) > 5 else None,
                Studio_Asset_ID=row[6] if len(row) > 6 else None,
                Studio_System_Name=row[7] if len(row) > 7 else None,
                Version=version
            )

            success = self.db_service.put_asset_info(asset_info.to_dict())

            if success:
                logger.debug(f"Inserted asset info for {filename} (version {version})")
            else:
                logger.warning(f"Failed to insert asset info for {filename}")
