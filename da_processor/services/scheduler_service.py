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
        logger.info(f"Creating schedule for DA {da_id} with delivery date {earliest_delivery_date}")
        
        schedule_dt = parse_date(earliest_delivery_date)
        if not schedule_dt:
            raise ValueError(f"Invalid earliest delivery date: {earliest_delivery_date}")
        
        schedule_expression = f"at({schedule_dt.strftime('%Y-%m-%dT%H:%M:%S')})"
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
            
            logger.info(f"Schedule created: {schedule_name} at {schedule_expression}")
            return response['ScheduleArn']
            
        except self.scheduler_client.exceptions.ConflictException:
            logger.warning(f"Schedule {schedule_name} already exists, updating...")
            return self._update_schedule(schedule_name, schedule_expression, da_id, licensee_id)
        except Exception as e:
            logger.error(f"Error creating schedule: {e}")
            raise
    
    def _update_schedule(self, schedule_name: str, schedule_expression: str, 
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
            
            logger.info(f"Schedule updated: {schedule_name}")
            return response['ScheduleArn']
        except Exception as e:
            logger.error(f"Error updating schedule: {e}")
            raise
    
    def delete_schedule(self, da_id: str) -> bool:
        schedule_name = f"manifest-{da_id}"
        
        try:
            self.scheduler_client.delete_schedule(Name=schedule_name)
            logger.info(f"Schedule deleted: {schedule_name}")
            return True
        except self.scheduler_client.exceptions.ResourceNotFoundException:
            logger.warning(f"Schedule not found: {schedule_name}")
            return False
        except Exception as e:
            logger.error(f"Error deleting schedule: {e}")
            return False