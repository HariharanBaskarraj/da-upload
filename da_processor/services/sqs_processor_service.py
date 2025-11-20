import json
import logging
import boto3
import time
from typing import Optional, Dict, Callable
from django.conf import settings

logger = logging.getLogger(__name__)


class SQSProcessorService:
    
    def __init__(self, queue_url: str, processor_func: Callable):
        self.sqs_client = boto3.client('sqs', region_name=settings.AWS_REGION)
        self.queue_url = queue_url
        self.processor_func = processor_func
        self.running = True
        
    def start_polling(self):
        logger.info(f"Starting SQS polling for queue: {self.queue_url}")
        
        while self.running:
            try:
                response = self.sqs_client.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20,
                    VisibilityTimeout=300,
                    MessageAttributeNames=['All']
                )
                
                messages = response.get('Messages', [])
                
                if not messages:
                    logger.debug("No messages received, continuing to poll...")
                    continue
                
                for message in messages:
                    receipt_handle = message['ReceiptHandle']
                    
                    try:
                        body = json.loads(message['Body'])
                        logger.info(f"Processing message: {body}")
                        
                        self.processor_func(body)
                        
                        self.sqs_client.delete_message(
                            QueueUrl=self.queue_url,
                            ReceiptHandle=receipt_handle
                        )
                        logger.info("Message processed and deleted successfully")
                        
                    except Exception as e:
                        logger.error(f"Error processing message: {e}", exc_info=True)
                        
            except Exception as e:
                logger.error(f"Error receiving messages from SQS: {e}", exc_info=True)
                time.sleep(5)
    
    def stop_polling(self):
        logger.info("Stopping SQS polling...")
        self.running = False