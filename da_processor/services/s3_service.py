import boto3
import logging
from typing import Optional
from django.conf import settings
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Service:
    def __init__(self):
        self.s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
        self.bucket_name = settings.AWS_S3_BUCKET

    def get_csv_content(self, key: str) -> Optional[str]:
        """Download CSV content from S3"""
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
        """
        Move the processed file to the 'Processed/' folder within the same bucket.
        """
        try:
        # Construct new key
            filename = key.split('/')[-1]
            new_key = f"Processed/{filename}"

        # Copy the file
            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                CopySource={'Bucket': self.bucket_name, 'Key': key},
                Key=new_key
            )
            logger.info(f"Copied file from {key} to {new_key}")

            # Delete the original file
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"Deleted original file after moving: {key}")

            return True

        except ClientError as e:
            logger.error(f"Error moving file to Processed/: {e}")
            return False

