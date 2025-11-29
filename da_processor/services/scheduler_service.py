"""
Scheduler Service with recurring manifest generation based on licensee manifest_frequency.

This service creates RECURRING schedules that trigger at the configured frequency
to check for version changes and send manifests when changes are detected.
"""
import json
import logging
import boto3
from datetime import datetime
from dateutil import parser
from django.conf import settings
from da_processor.utils.date_utils import parse_date

logger = logging.getLogger(__name__)


class SchedulerService:
    """
    Service for managing AWS EventBridge Scheduler schedules for DA workflows.

    CRITICAL: Manifest schedules are RECURRING (not one-time) and trigger at
    the licensee's configured manifest_frequency to check for version changes.
    """
    
    def __init__(self):
        self.scheduler_client = boto3.client('scheduler', region_name=settings.AWS_REGION)
        self.dynamodb = boto3.resource('dynamodb', region_name=settings.AWS_REGION)
        self.licensee_table = self.dynamodb.Table(settings.DYNAMODB_LICENSEE_TABLE)
    
    def create_manifest_schedule(self, da_id: str, earliest_delivery_date: str, licensee_id: str) -> str:
        """
        Create RECURRING EventBridge schedule for manifest generation.
        
        The schedule triggers at the licensee's configured manifest_frequency
        (e.g., every 30 minutes, every 1 hour) to check for version changes.

        Args:
            da_id: Distribution Authorization ID
            earliest_delivery_date: ISO format date when schedule should start
            licensee_id: Licensee identifier (to get manifest_frequency)

        Returns:
            Schedule ARN

        Raises:
            ValueError: If date is invalid
            Exception: If schedule creation fails
        """
        logger.info(f"Creating RECURRING manifest schedule for DA {da_id}")
        
        # Validate earliest delivery date
        schedule_dt = parse_date(earliest_delivery_date)
        if not schedule_dt:
            raise ValueError(f"Invalid earliest delivery date: {earliest_delivery_date}")
        
        # Get manifest frequency from licensee configuration
        manifest_frequency_seconds = self._get_manifest_frequency(licensee_id)
        
        # Convert seconds to minutes for rate expression
        manifest_frequency_minutes = max(1, manifest_frequency_seconds // 60)
        
        # Create RECURRING schedule expression
        # This will trigger every X minutes starting from earliest_delivery_date
        schedule_expression = f"rate({manifest_frequency_minutes} minutes)"
        
        # Calculate start date (when the recurring schedule should begin)
        start_date = schedule_dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        schedule_name = f"manifest-{da_id}"
        
        logger.info(
            f"Creating recurring schedule: {schedule_name}, "
            f"frequency: every {manifest_frequency_minutes} minutes, "
            f"starts: {start_date}"
        )
        
        try:
            response = self.scheduler_client.create_schedule(
                Name=schedule_name,
                ScheduleExpression=schedule_expression,
                ScheduleExpressionTimezone='UTC',
                StartDate=schedule_dt,  # Schedule starts at earliest_delivery_date
                FlexibleTimeWindow={'Mode': 'OFF'},
                Target={
                    'Arn': settings.LAMBDA_MANIFEST_GENERATOR_ARN,
                    'RoleArn': settings.EVENTBRIDGE_SCHEDULER_ROLE_ARN,
                    'Input': json.dumps({
                        'da_id': da_id,
                        'licensee_id': licensee_id,
                        'trigger_type': 'scheduled_manifest'
                    })
                },
                State='ENABLED'
            )
            
            logger.info(
                f"Manifest schedule created: {schedule_name}, "
                f"triggers every {manifest_frequency_minutes} minutes"
            )
            return response['ScheduleArn']
            
        except self.scheduler_client.exceptions.ConflictException:
            logger.warning(f"Manifest schedule {schedule_name} already exists, updating...")
            return self._update_manifest_schedule(
                schedule_name, schedule_expression, da_id, licensee_id, schedule_dt
            )
        except Exception as e:
            logger.error(f"Error creating manifest schedule: {e}")
            raise
    
    def _get_manifest_frequency(self, licensee_id: str) -> int:
        """
        Get manifest frequency in seconds from licensee configuration.
        
        Args:
            licensee_id: Licensee identifier
            
        Returns:
            Frequency in seconds (default: 1800 = 30 minutes)
        """
        try:
            response = self.licensee_table.get_item(Key={'Licensee_ID': licensee_id})
            
            if 'Item' not in response:
                logger.warning(
                    f"Licensee {licensee_id} not found, using default frequency"
                )
                return settings.MANIFEST_CHECK_INTERVAL  # Default from settings
            
            licensee_data = response['Item']
            manifest_frequency = int(licensee_data.get('Manifest_Frequency', settings.MANIFEST_CHECK_INTERVAL))
            
            logger.info(
                f"Licensee {licensee_id} manifest frequency: {manifest_frequency} seconds"
            )
            
            return manifest_frequency
            
        except Exception as e:
            logger.error(f"Error getting manifest frequency: {e}")
            return settings.MANIFEST_CHECK_INTERVAL  # Fallback to default
    
    def create_exception_notification_schedule(self, da_id: str, exception_notification_date: str) -> str:
        """
        Create ONE-TIME EventBridge schedule for exception notification.
        
        This is a one-time schedule (not recurring) that triggers at the
        exception_notification_date to check for missing assets.

        Args:
            da_id: Distribution Authorization ID
            exception_notification_date: ISO format date for schedule

        Returns:
            Schedule ARN

        Raises:
            ValueError: If date is invalid
            Exception: If schedule creation fails
        """
        logger.info(f"Creating exception notification schedule for DA {da_id}")
        
        schedule_dt = parse_date(exception_notification_date)
        if not schedule_dt:
            raise ValueError(f"Invalid exception notification date: {exception_notification_date}")
        
        # ONE-TIME schedule using "at" expression
        schedule_expression = f"at({schedule_dt.strftime('%Y-%m-%dT%H:%M:%S')})"
        schedule_name = f"exception-{da_id}"
        
        try:
            response = self.scheduler_client.create_schedule(
                Name=schedule_name,
                ScheduleExpression=schedule_expression,
                ScheduleExpressionTimezone='UTC',
                FlexibleTimeWindow={'Mode': 'OFF'},
                Target={
                    'Arn': settings.LAMBDA_EXCEPTION_NOTIFIER_ARN,
                    'RoleArn': settings.EVENTBRIDGE_SCHEDULER_ROLE_ARN,
                    'Input': json.dumps({
                        'da_id': da_id,
                        'trigger_type': 'exception_notification'
                    })
                },
                State='ENABLED'
            )
            
            logger.info(f"Exception schedule created: {schedule_name} at {schedule_expression}")
            return response['ScheduleArn']
            
        except self.scheduler_client.exceptions.ConflictException:
            logger.warning(f"Exception schedule {schedule_name} already exists, updating...")
            return self._update_exception_schedule(schedule_name, schedule_expression, da_id)
        except Exception as e:
            logger.error(f"Error creating exception schedule: {e}")
            raise
    
    def _update_manifest_schedule(
        self, schedule_name: str, schedule_expression: str, 
        da_id: str, licensee_id: str, start_date: datetime
    ) -> str:
        """Update existing manifest schedule."""
        try:
            response = self.scheduler_client.update_schedule(
                Name=schedule_name,
                ScheduleExpression=schedule_expression,
                ScheduleExpressionTimezone='UTC',
                StartDate=start_date,
                FlexibleTimeWindow={'Mode': 'OFF'},
                Target={
                    'Arn': settings.LAMBDA_MANIFEST_GENERATOR_ARN,
                    'RoleArn': settings.EVENTBRIDGE_SCHEDULER_ROLE_ARN,
                    'Input': json.dumps({
                        'da_id': da_id,
                        'licensee_id': licensee_id,
                        'trigger_type': 'scheduled_manifest'
                    })
                },
                State='ENABLED'
            )
            
            logger.info(f"Manifest schedule updated: {schedule_name}")
            return response['ScheduleArn']
        except Exception as e:
            logger.error(f"Error updating manifest schedule: {e}")
            raise
    
    def _update_exception_schedule(self, schedule_name: str, schedule_expression: str, da_id: str) -> str:
        """Update existing exception schedule."""
        try:
            response = self.scheduler_client.update_schedule(
                Name=schedule_name,
                ScheduleExpression=schedule_expression,
                ScheduleExpressionTimezone='UTC',
                FlexibleTimeWindow={'Mode': 'OFF'},
                Target={
                    'Arn': settings.LAMBDA_EXCEPTION_NOTIFIER_ARN,
                    'RoleArn': settings.EVENTBRIDGE_SCHEDULER_ROLE_ARN,
                    'Input': json.dumps({
                        'da_id': da_id,
                        'trigger_type': 'exception_notification'
                    })
                },
                State='ENABLED'
            )
            
            logger.info(f"Exception schedule updated: {schedule_name}")
            return response['ScheduleArn']
        except Exception as e:
            logger.error(f"Error updating exception schedule: {e}")
            raise
    
    def delete_schedule(self, da_id: str) -> bool:
        """
        Delete manifest generation schedule.

        Args:
            da_id: Distribution Authorization ID

        Returns:
            True if deleted successfully, False if not found or failed
        """
        schedule_name = f"manifest-{da_id}"
        
        try:
            self.scheduler_client.delete_schedule(Name=schedule_name)
            logger.info(f"Manifest schedule deleted: {schedule_name}")
            return True
        except self.scheduler_client.exceptions.ResourceNotFoundException:
            logger.warning(f"Manifest schedule not found: {schedule_name}")
            return False
        except Exception as e:
            logger.error(f"Error deleting manifest schedule: {e}")
            return False
    
    def delete_exception_schedule(self, da_id: str) -> bool:
        """
        Delete exception notification schedule.

        Args:
            da_id: Distribution Authorization ID

        Returns:
            True if deleted successfully, False if not found or failed
        """
        schedule_name = f"exception-{da_id}"
        
        try:
            self.scheduler_client.delete_schedule(Name=schedule_name)
            logger.info(f"Exception schedule deleted: {schedule_name}")
            return True
        except self.scheduler_client.exceptions.ResourceNotFoundException:
            logger.warning(f"Exception schedule not found: {schedule_name}")
            return False
        except Exception as e:
            logger.error(f"Error deleting exception schedule: {e}")
            return False