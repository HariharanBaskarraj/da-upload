import boto3
import logging
from typing import Optional
from django.conf import settings
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Service:
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
        """
        Moves only .mov assets from watermark bucket → licensee bucket.
        Returns count of moved files.
        """
        watermark_cache = settings.AWS_WATERMARKED_BUCKET
        licencee_cache = settings.AWS_LICENSEE_BUCKET
        assets = manifest.get("assets", [])
        mov_assets = [
            asset for asset in assets 
            if asset.get("file_name", "").lower().endswith(".mov")
        ]

        if not mov_assets:
            logger.info("No .mov files detected — skipping move.")
            return 0

        licensee_id = manifest["main_body"]["licensee_id"]
        da_id = manifest["main_body"]["distribution_authorization_id"]

        moved_count = 0

        for asset in mov_assets:
            source_key = asset["file_path"]  # in watermark bucket
            file_name = asset["file_name"]

            # Standardized destination key
            dest_key = f"{licensee_id}/{da_id}/{file_name}"

            logger.info(f"Moving MOV file: {source_key} → {dest_key}")

            # 1️⃣ Copy
            try:
                self.s3.copy_object(
                    Bucket=licencee_cache,
                    Key=dest_key,
                    CopySource={"Bucket": watermark_cache, "Key": source_key}
                )
                logger.info("Copy successful.")
            except Exception as e:
                logger.error(f"Copy failed for {source_key}: {e}")
                continue  # skip delete if copy fails

            # 2️⃣ Delete
            try:
                self.s3.delete_object(
                    Bucket=watermark_cache,
                    Key=source_key
                )
                logger.info("Delete from Watermark Cache successful.")
                moved_count += 1
            except Exception as e:
                logger.error(f"Delete failed for {source_key}: {e}")
                continue

        logger.info(f"Total MOV files moved: {moved_count}")
        return moved_count