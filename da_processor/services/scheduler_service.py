import json
import logging
import boto3
from datetime import datetime
from dateutil import parser
from django.conf import settings
from da_processor.utils.date_utils import parse_date

logger = logging.getLogger(__name__)


class SchedulerService:
    
    def __init__(self):
        self.scheduler_client = boto3.client('scheduler', region_name=settings.AWS_REGION)
    
    def create_manifest_schedule(self, da_id: str, earliest_delivery_date: str, licensee_id: str) -> str:
        logger.info(f"Creating manifest schedule for DA {da_id} with delivery date {earliest_delivery_date}")
        
        schedule_dt = parse_date(earliest_delivery_date)
        if not schedule_dt:
            raise ValueError(f"Invalid earliest delivery date: {earliest_delivery_date}")
        
        start_date = schedule_dt.strftime('%Y-%m-%dT%H:%M:%S')
        schedule_expression = f"rate(1 minute)"
        schedule_name = f"manifest-{da_id}"
        
        try:
            response = self.scheduler_client.create_schedule(
                Name=schedule_name,
                ScheduleExpression=schedule_expression,
                ScheduleExpressionTimezone='UTC',
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
            
            logger.info(f"Manifest schedule created: {schedule_name} at {schedule_expression}")
            return response['ScheduleArn']
            
        except self.scheduler_client.exceptions.ConflictException:
            logger.warning(f"Manifest schedule {schedule_name} already exists, updating...")
            return self._update_manifest_schedule(schedule_name, schedule_expression, da_id, licensee_id)
        except Exception as e:
            logger.error(f"Error creating manifest schedule: {e}")
            raise
    
    def create_exception_notification_schedule(self, da_id: str, exception_notification_date: str) -> str:
        logger.info(f"Creating exception notification schedule for DA {da_id} with date {exception_notification_date}")
        
        schedule_dt = parse_date(exception_notification_date)
        if not schedule_dt:
            raise ValueError(f"Invalid exception notification date: {exception_notification_date}")
        
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
    
    def _update_manifest_schedule(self, schedule_name: str, schedule_expression: str, 
                        da_id: str, licensee_id: str) -> str:
        try:
            response = self.scheduler_client.update_schedule(
                Name=schedule_name,
                ScheduleExpression=schedule_expression,
                ScheduleExpressionTimezone='UTC',
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