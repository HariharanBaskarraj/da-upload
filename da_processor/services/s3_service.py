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

    def delete_file(self, key: str) -> bool:
        """Delete file from S3 after processing"""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"Successfully deleted file from S3: {key}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting file from S3: {e}")
            return False
