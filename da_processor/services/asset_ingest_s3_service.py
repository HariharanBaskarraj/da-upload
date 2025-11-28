"""
S3 service for asset ingestion operations.

This service handles all S3 operations specific to the asset ingestion pipeline,
including file retrieval, copying, moving, deletion, and verification operations
across Upload, Error, and Asset Repository buckets.
"""
import logging
import boto3
from typing import List, Dict, Optional
from botocore.exceptions import ClientError
from django.conf import settings
from da_processor.utils.path_utils import normalize_s3_path

logger = logging.getLogger(__name__)


class AssetIngestS3Service:
    """
    Service for managing S3 operations during asset ingestion.

    This service provides methods for:
    - Retrieving file content from S3
    - Listing objects in folders
    - Copying files between buckets and folders
    - Moving files to success/error locations
    - Verifying and deleting folders after successful transfer
    """

    def __init__(self, ingest_bucket: str, asset_repo_bucket: str):
        """
        Initialize the Asset Ingest S3 Service.

        Args:
            ingest_bucket: Name of the S3 bucket for ingestion (contains Upload/ and Error/)
            asset_repo_bucket: Name of the S3 bucket for final asset repository
        """
        self.s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
        self.ingest_bucket = ingest_bucket
        self.asset_repo_bucket = asset_repo_bucket

    def get_object_content(self, bucket: str, key: str) -> bytes:
        """
        Get object content from S3 as bytes.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Object content as bytes

        Raises:
            ClientError: If S3 operation fails
        """
        try:
            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = obj['Body'].read()
            logger.debug(f"Retrieved object: {bucket}/{key} ({len(content)} bytes)")
            return content
        except ClientError as e:
            logger.error(f"Error reading S3 object {bucket}/{key}: {e}")
            raise

    def list_objects(self, bucket: str, prefix: str = None) -> List[Dict]:
        """
        List all objects under a given prefix, excluding folder markers.

        Args:
            bucket: S3 bucket name
            prefix: Prefix to filter objects (optional)

        Returns:
            List of objects (excludes keys ending with '/')
        """
        logger.debug(f"Listing objects in bucket: {bucket}, prefix: {prefix}")

        if prefix:
            prefix = normalize_s3_path(prefix)
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        else:
            response = self.s3_client.list_objects_v2(Bucket=bucket)

        contents = response.get('Contents', [])
        # Filter out folder markers (keys ending with '/')
        objects = [obj for obj in contents if not obj['Key'].endswith('/')]
        logger.debug(f"Found {len(objects)} objects")
        return objects

    def copy_object(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> bool:
        """
        Copy object from source to destination.

        Args:
            source_bucket: Source bucket name
            source_key: Source object key
            dest_bucket: Destination bucket name
            dest_key: Destination object key

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.debug(f"Copying: {source_bucket}/{source_key} -> {dest_bucket}/{dest_key}")
            self.s3_client.copy_object(
                Bucket=dest_bucket,
                Key=dest_key,
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                MetadataDirective='COPY'
            )
            return True
        except ClientError as e:
            logger.error(f"Error copying object: {e}")
            return False

    def delete_object(self, bucket: str, key: str) -> None:
        """
        Delete a single object from S3.

        Args:
            bucket: S3 bucket name
            key: Object key to delete

        Raises:
            Exception: If deletion fails
        """
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=key)
            logger.debug(f"Deleted object: {bucket}/{key}")
        except Exception as e:
            logger.error(f"Error deleting object {key}: {str(e)}")
            raise

    def delete_folder(self, bucket: str, folder_prefix: str) -> bool:
        """
        Delete all objects under a specific S3 prefix (folder).

        Uses batch deletion for efficiency (up to 1000 objects per request).

        Args:
            bucket: S3 bucket name
            folder_prefix: Folder prefix to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=folder_prefix)
            to_delete = {'Objects': []}

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        to_delete['Objects'].append({'Key': obj['Key']})
                        # Batch delete every 1000 objects (S3 limit)
                        if len(to_delete['Objects']) == 1000:
                            self.s3_client.delete_objects(Bucket=bucket, Delete=to_delete)
                            to_delete = {'Objects': []}

            # Delete remaining objects
            if to_delete['Objects']:
                self.s3_client.delete_objects(Bucket=bucket, Delete=to_delete)

            logger.info(f"Deleted folder: {bucket}/{folder_prefix}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting folder {folder_prefix}: {e}")
            return False

    def verify_and_delete_folder(self, source_bucket: str, destination_bucket: str, folder_prefix: str) -> None:
        """
        Verify all files from Upload/<folder_prefix>/ exist in destination, then delete from source.

        Args:
            source_bucket: Source bucket name
            destination_bucket: Destination bucket name
            folder_prefix: Folder prefix to verify and delete
        """
        source_prefix = f"Upload/{folder_prefix}/"
        logger.info(f"Verifying files from {source_bucket}/{source_prefix}")

        # List all files in source folder
        response = self.s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
        source_files = [obj['Key'] for obj in response.get('Contents', []) if not obj['Key'].endswith('/')]

        if not source_files:
            logger.info(f"No files found in {source_prefix}")
            return

        all_exist = True

        # Verify each file exists in destination
        for key in source_files:
            dest_key = key.replace("Upload/", "", 1)
            try:
                self.s3_client.head_object(Bucket=destination_bucket, Key=dest_key)
                logger.debug(f"Verified exists in destination: {destination_bucket}/{dest_key}")
            except ClientError:
                logger.warning(f"Missing in destination: {destination_bucket}/{dest_key}")
                all_exist = False

        # Delete only if all files verified
        if all_exist:
            logger.info(f"All files verified, deleting source folder: {source_prefix}")
            for key in source_files:
                self.s3_client.delete_object(Bucket=source_bucket, Key=key)
                logger.debug(f"Deleted: {key}")
        else:
            logger.warning(f"Not all files found in destination, source not deleted")

    def verify_and_delete_error_folder(self, bucket_name: str, folder_prefix: str) -> None:
        """
        Verify all files from Upload/<folder_prefix>/ exist in Error/, then delete from Upload/.

        Args:
            bucket_name: S3 bucket name (same bucket for Upload/ and Error/)
            folder_prefix: Folder prefix to verify
        """
        logger.info(f"Verifying error folder migration for: {folder_prefix}")

        stage_prefix = f"Upload/{folder_prefix}/"
        error_prefix = f"Error/{folder_prefix}/"

        # List all files in Upload/
        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=stage_prefix)
        if 'Contents' not in response:
            logger.info(f"No files found in {stage_prefix}")
            return

        stage_objects = [obj['Key'] for obj in response['Contents']]

        # Verify all files exist in Error/
        all_copied = True
        for key in stage_objects:
            error_key = key.replace("Upload/", "Error/", 1)
            try:
                self.s3_client.head_object(Bucket=bucket_name, Key=error_key)
            except ClientError:
                logger.warning(f"Missing in Error: {key}")
                all_copied = False

        # Delete originals only if all copied successfully
        if all_copied:
            for key in stage_objects:
                self.s3_client.delete_object(Bucket=bucket_name, Key=key)
                logger.debug(f"Deleted from Upload/: {key}")
            logger.info(f"All files verified in Error/, deleted from Upload/")
        else:
            logger.warning(f"Not all files copied to Error/, Upload/ files not deleted")

    def copy_to_error(self, asset_path: str, folder_prefix: str, reason: str) -> None:
        """
        Copy asset to Error/ folder with logging of failure reason.

        Args:
            asset_path: Path to the asset file
            folder_prefix: Folder prefix for organizing errors
            reason: Reason for the failure
        """
        normalized_path = normalize_s3_path(asset_path)
        dest_key = f"Error/{normalized_path.split('/')[-1]}"

        try:
            self.s3_client.copy_object(
                CopySource={'Bucket': self.ingest_bucket, 'Key': normalized_path},
                Bucket=self.ingest_bucket,
                Key=dest_key
            )
            logger.info(f"Copied to Error/: {normalized_path} (Reason: {reason})")
        except ClientError as e:
            logger.error(f"Failed to copy to Error/: {e}")

    def get_text_file(self, bucket: str, key: str) -> str:
        """
        Retrieve full text file content (CSV, JSON, TXT).

        Args:
            bucket: S3 bucket name
            key: Object key

        Returns:
            File content as string (UTF-8 decoded)
        """
        obj = self.s3_client.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")
