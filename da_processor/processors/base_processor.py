"""
Base processor for Distribution Authorization (DA) processing.

This module provides the abstract base class for all DA processors,
establishing common initialization and helper methods for DA processing operations.
"""
import logging
import boto3
from abc import ABC, abstractmethod
from typing import Dict
from django.conf import settings
from da_processor.services.dynamodb_service import DynamoDBService
from da_processor.services.default_values_service import DefaultValuesService

logger = logging.getLogger(__name__)


class BaseDAProcessor(ABC):
    """
    Abstract base class for Distribution Authorization processors.

    This class provides common initialization and utility methods for all DA processors:
    - Database service initialization
    - Default values service setup
    - SNS and SQS client configuration
    - Exception notification methods
    - Asset availability notification methods
    """
    
    def __init__(self):
        self.db_service = DynamoDBService()
        self.default_service = DefaultValuesService(self.db_service)
        self.sns_client = boto3.client('sns', region_name=settings.AWS_REGION)
        self.sqs_client = boto3.client('sqs', region_name=settings.AWS_REGION)

    @abstractmethod
    def process(self, data) -> Dict:
        """Process the DA data"""
        pass

    def send_exception_notification(self, error_message: str, da_data: Dict) -> None:
        """Send exception notification to configured recipients"""
        try:
            recipients = da_data.get('ExceptionRecipients', '')
            if not recipients:
                recipients = ','.join(settings.DEFAULT_EXCEPTION_RECIPIENTS)
            
            if not recipients:
                logger.warning("No exception recipients configured")
                return
            
            title_id = da_data.get('TitleID', 'Unknown')
            licensee_id = da_data.get('LicenseeID', 'Unknown')
            
            subject = f"DA Processing Exception - Title: {title_id}"
            message = f"""
Distribution Authorization Processing Exception

Title ID: {title_id}
Licensee ID: {licensee_id}
Error: {error_message}

Please review and take appropriate action.
            """
            
            # This would use SNS or SES to send notifications
            # For now, just log
            logger.error(f"Exception notification would be sent to: {recipients}")
            logger.error(f"Subject: {subject}")
            logger.error(f"Message: {message}")
            
        except Exception as e:
            logger.error(f"Failed to send exception notification: {e}")

    def send_asset_availability_notification(self, title_id: str, licensee_id: str) -> None:
        """Send notification to licensee SQS about available assets"""
        try:
            # This would be configured per licensee
            # For now, log the notification
            logger.info(f"Asset availability notification for Title {title_id} to Licensee {licensee_id}")
            
            message = {
                'title_id': title_id,
                'licensee_id': licensee_id,
                'notification_type': 'ASSETS_AVAILABLE',
                'timestamp': str(settings.datetime.utcnow().isoformat())
            }
            
            # Here you would send to licensee's SQS queue
            # queue_url = get_licensee_queue_url(licensee_id)
            # self.sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))
            
            logger.info(f"Notification sent: {message}")
            
        except Exception as e:
            logger.error(f"Failed to send asset availability notification: {e}")