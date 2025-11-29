"""
SQS service for sending messages to licensee queues.

This service handles SQS operations for sending manifest notifications
to licensee-specific queues for asset availability notifications.
"""
import json
import logging
import boto3
from typing import Dict
from django.conf import settings

logger = logging.getLogger(__name__)


class SQSService:
    """
    Service for managing SQS message operations to licensee queues.

    This service provides methods for:
    - Sending manifest notifications to licensee-specific SQS queues
    - Managing queue URL mappings for different licensees
    """
    
    def __init__(self):
        self.sqs_client = boto3.client('sqs', region_name=settings.AWS_REGION)
    
    def send_manifest_to_licensee(self, licensee_id: str, manifest: Dict) -> bool:
        """
        Send manifest to licensee-specific SQS queue.

        Args:
            licensee_id: Licensee identifier
            manifest: Complete manifest dictionary

        Returns:
            True if sent successfully, False otherwise
        """
        queue_url = self._get_queue_url_for_licensee(licensee_id)
        
        if not queue_url:
            logger.error(f"No queue URL configured for licensee: {licensee_id}")
            return False
        
        try:
            message_body = json.dumps(manifest)
            
            response = self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                MessageAttributes={
                    'licensee_id': {
                        'DataType': 'String',
                        'StringValue': licensee_id
                    },
                    'manifest_type': {
                        'DataType': 'String',
                        'StringValue': 'asset_availability'
                    }
                }
            )
            
            logger.info(f"Manifest sent to queue {queue_url}, MessageId: {response['MessageId']}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending manifest to SQS: {e}")
            return False
    
    def _get_queue_url_for_licensee(self, licensee_id: str) -> str:
        """
        Get SQS queue URL for a specific licensee.

        Args:
            licensee_id: Licensee identifier

        Returns:
            Queue URL string, or empty string if not configured
        """
        queue_mapping = {
            'PrimeVideo': settings.AWS_SQS_PRIMEVIDEO_QUEUE_URL
        }
        
        return queue_mapping.get(licensee_id, '')
    
    def send_to_dlq(self, message: Dict, error_reason: str) -> bool:
        """
        Send failed message to Dead Letter Queue.

        Args:
            message: Original message dictionary
            error_reason: Reason for failure

        Returns:
            True if sent to DLQ successfully, False otherwise
        """
        try:
            message_body = json.dumps({
                'original_message': message,
                'error_reason': error_reason
            })
            
            response = self.sqs_client.send_message(
                QueueUrl=settings.AWS_SQS_DLQ_URL,
                MessageBody=message_body
            )
            
            logger.warning(f"Message sent to DLQ, MessageId: {response['MessageId']}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending to DLQ: {e}")
            return False