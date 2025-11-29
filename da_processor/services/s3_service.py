"""
S3 service for CSV file operations and file management.

This service handles S3 operations for the DA processing pipeline, including
retrieving CSV files, moving processed files, and error handling.
"""
import boto3
import logging
import re
from typing import Optional
from django.conf import settings
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Service:
    """
    Service for managing S3 operations in the DA processing pipeline.

    This service provides methods for:
    - Retrieving CSV content from S3
    - Moving files to Processed/ folder after successful processing
    - Moving files to Error/ folder when processing fails
    """
    def __init__(self):
        self.s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
        self.bucket_name = settings.AWS_DA_BUCKET

    def get_csv_content(self, key: str) -> Optional[str]:
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            logger.info(f"Successfully retrieved CSV from S3: {key}")
            return content
        except ClientError as e:
            logger.error(f"Error retrieving CSV from S3: {e}")
            raise

    def move_file_to_processed(self, key: str) -> bool:
        try:
            filename = key.split('/')[-1]
            new_key = f"Processed/{filename}"

            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                CopySource={'Bucket': self.bucket_name, 'Key': key},
                Key=new_key
            )
            logger.info(f"Copied file from {key} to {new_key}")

            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"Deleted original file after moving: {key}")

            return True

        except ClientError as e:
            logger.error(f"Error moving file to Processed/: {e}")
            return False

    def move_file_to_error(self, key: str) -> bool:
        try:
            filename = key.split('/')[-1]
            new_key = f"Error/{filename}"

            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                CopySource={'Bucket': self.bucket_name, 'Key': key},
                Key=new_key
            )
            logger.info(f"Copied failed file from {key} to {new_key}")

            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(
                f"Deleted original file after moving to Error/: {key}")

            return True

        except ClientError as e:
            logger.error(f"Error moving file to Error/: {e}")
            return False


    def move_mov_files(self, manifest: dict):
        watermark_cache = settings.AWS_WATERMARKED_BUCKET
        licencee_cache = settings.AWS_LICENSEE_BUCKET
        assets = manifest.get("assets", [])

        mov_assets = [
            asset for asset in assets
            if asset.get("file_name", "").lower().endswith(".mov") and asset.get("file_status") in ("New", "Revised")
        ]

        logger.info(f"mov_assets: {mov_assets}")

        if not mov_assets:
            logger.info("No .mov files detected.")
            return []

        moved_details = []

        for asset in mov_assets:
            original_name = asset["file_name"]                     # FirstLook.mov
            base_name = original_name.replace(".mov", "")          # FirstLook
            folder_path = "/".join(asset["file_path"].split("/")[:-1])

            prefix = f"{folder_path}/{base_name}_WM"

            logger.info(f"Scanning for WM versions: {prefix}")

            response = self.s3_client.list_objects_v2(
                Bucket=watermark_cache,
                Prefix=prefix
            )

            if "Contents" not in response:
                logger.warning(f"No WM files found for {original_name}")
                continue

            versioned = []
            for obj in response["Contents"]:
                key = obj["Key"]
                match = re.search(r"_WM(\d+)\.mov$", key)
                if match:
                    versioned.append((int(match.group(1)), key))

            if not versioned:
                logger.warning(f"No WM version file found for: {original_name}")
                continue

            versioned.sort(key=lambda x: x[0])
            lowest_version, lowest_key = versioned[0]

            file_name = lowest_key.split("/")[-1]
            licensee_id = manifest["main_body"]["licensee_id"]
            da_id = manifest["main_body"]["distribution_authorization_id"]

            file_name = lowest_key.split("/")[-1]

            # Extract folder path from watermark key
            folder_path = "/".join(lowest_key.split("/")[:-1])   # e.g., 1234.5678/Trailers

            # Correct licensee path: PrimeVideo/{same_folder_path}/filename.mov
            dest_key = f"{licensee_id}/{folder_path}/{file_name}"

            logger.info(f"Moving: {lowest_key} → {dest_key}")

            """ dest_key = f"{licensee_id}/{da_id}/{file_name}"

            logger.info(f"Moving: {lowest_key} → {dest_key}") """

            # Copy to licensee
            try:
                self.s3_client.copy_object(
                    Bucket=licencee_cache,
                    Key=dest_key,
                    CopySource={"Bucket": watermark_cache, "Key": lowest_key}
                )
            except Exception as e:
                logger.error(f"Copy failed: {e}")
                continue

            # Delete from watermark
            try:
                self.s3_client.delete_object(
                    Bucket=watermark_cache,
                    Key=lowest_key
                )
            except Exception as e:
                logger.error(f"Delete failed: {e}")
                continue

            moved_details.append({
                "base_file": original_name,   # FirstLook.mov
                "lowest_key": lowest_key,     # Full S3 path of WM1
                "version": lowest_version
            })

        logger.info(f"Total MOV files moved: {len(moved_details)}")
        return moved_details


   
    @staticmethod
    def extract_wm_version(file_name: str) -> int:
        match = re.search(r"_WM(\d+)\.mov$", file_name, re.IGNORECASE)
        return int(match.group(1)) if match else None

