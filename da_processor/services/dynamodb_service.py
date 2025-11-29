"""
DynamoDB service for Distribution Authorization (DA) operations with Is_Active flag support.

This service handles all DynamoDB operations with enhanced status tracking and activation control.
"""
import boto3
import uuid
import logging
from typing import Dict, List, Optional
from django.conf import settings
from botocore.exceptions import ClientError
from da_processor.utils.date_utils import to_zulu, get_current_zulu

logger = logging.getLogger(__name__)


class DynamoDBService:
    """
    Service for managing DynamoDB operations related to Distribution Authorizations.
    Enhanced with Is_Active flag and comprehensive status tracking.
    """

    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name=settings.AWS_REGION)
        self.da_table = self.dynamodb.Table(settings.DYNAMODB_DA_TABLE)
        self.title_table = self.dynamodb.Table(settings.DYNAMODB_TITLE_TABLE)
        self.component_table = self.dynamodb.Table(settings.DYNAMODB_COMPONENT_TABLE)
        self.studio_config_table = self.dynamodb.Table(settings.DYNAMODB_STUDIO_CONFIG_TABLE)
        self.watermark_table = settings.WATERMARK_JOB_TABLE
        self.table = self.dynamodb.Table(self.watermark_table)

    def create_if_not_exists_title_info(self, title_data: Dict) -> Dict:
        try:
            title_id = title_data.get('Title_ID', '')
            version_id = title_data.get('Version_ID', '')

            if not title_id or not version_id:
                raise ValueError("Title_ID and Version_ID are required")

            response = self.title_table.get_item(Key={
                'Title_ID': title_id,
                'Version_ID': version_id
            })

            is_new = 'Item' not in response

            if is_new:
                title_item = {
                    'Title_ID': title_id,
                    'Version_ID': version_id,
                    'Title_Name': title_data.get('Title_Name', ''),
                    'Title_EIDR_ID': title_data.get('Title_EIDR_ID', ''),
                    'Version_Name': title_data.get('Version_Name', ''),
                    'Version_EIDR_ID': title_data.get('Version_EIDR_ID', ''),
                    'Release_Year': title_data.get('Release_Year', ''),
                    'Uploader': title_data.get('Uploader', 'SYSTEM'),
                    'Created_At': get_current_zulu()
                }
                self.title_table.put_item(Item=title_item)
                logger.info(f"Created new title info record: Title_ID={title_id}, Version_ID={version_id}")
            else:
                logger.info(f"Title info already exists: Title_ID={title_id}, Version_ID={version_id}")

            return {"is_new": is_new}

        except ClientError as e:
            logger.error(f"Error in create_if_not_exists_title_info: {e}")
            raise

    def create_da_record(self, da_data: Dict) -> Dict:
        """
        Create DA record with Is_Active=False initially.
        Is_Active will be set to True only when earliest_delivery_date is reached.
        """
        try:
            record_id = str(uuid.uuid4())

            item = {
                'ID': record_id,
                'Title_ID': da_data.get('Title_ID', ''),
                'Version_ID': da_data.get('Version_ID', ''),
                'Licensee_ID': da_data.get('Licensee_ID', ''),
                'DA_Description': da_data.get('DA_Description', ''),
                'Due_Date': to_zulu(da_data.get('Due_Date')) or '',
                'Earliest_Delivery_Date': to_zulu(da_data.get('Earliest_Delivery_Date')) or '',
                'License_Period_Start': to_zulu(da_data.get('License_Period_Start')) or '',
                'License_Period_End': to_zulu(da_data.get('License_Period_End')) or '',
                'Territories': da_data.get('Territories', ''),
                'Exception_Notification_Date': to_zulu(da_data.get('Exception_Notification_Date')) or '',
                'Exception_Recipients': da_data.get('Exception_Recipients', ''),
                'Internal_Studio_ID': da_data.get('Internal_Studio_ID', ''),
                'Studio_System_ID': da_data.get('Studio_System_ID', ''),
                'Is_Active': False,
                'Delivery_Status': 'PENDING',
                'Created_At': get_current_zulu()
            }

            response = self.da_table.put_item(Item=item)
            logger.info(f"DA record created: ID={record_id}, Is_Active=False, Delivery_Status=PENDING")
            return {"ID": record_id, "response": response}

        except ClientError as e:
            logger.error(f"Error creating DA record: {e}")
            raise

    def set_da_active(self, da_id: str) -> bool:
        """
        Set Is_Active=True for a DA when earliest_delivery_date is reached.
        """
        try:
            self.da_table.update_item(
                Key={'ID': da_id},
                UpdateExpression='SET Is_Active = :active',
                ExpressionAttributeValues={':active': True}
            )
            logger.info(f"Set Is_Active=True for DA: {da_id}")
            return True
        except ClientError as e:
            logger.error(f"Error setting DA active: {e}")
            return False

    def set_da_inactive(self, da_id: str) -> bool:
        """
        Set Is_Active=False for a DA when license period ends.
        """
        try:
            self.da_table.update_item(
                Key={'ID': da_id},
                UpdateExpression='SET Is_Active = :active',
                ExpressionAttributeValues={':active': False}
            )
            logger.info(f"Set Is_Active=False for DA: {da_id}")
            return True
        except ClientError as e:
            logger.error(f"Error setting DA inactive: {e}")
            return False

    def get_da_record(self, record_id: str) -> Optional[Dict]:
        try:
            response = self.da_table.get_item(Key={'ID': record_id})
            return response.get('Item')
        except ClientError as e:
            logger.error(f"Error getting DA record {record_id}: {e}")
            return None

    def create_component(self, record_id: str, title_id: str, version_id: str, component_data: Dict) -> Dict:
        """
        Create component record with initial Delivery_Status=PENDING.
        """
        try:
            item = {
                'ID': record_id,
                'Title_ID': title_id,
                'Version_ID': version_id,
                'Component_ID': component_data.get('Component_ID', ''),
                'Required_Flag': component_data.get('Required_Flag', 'FALSE'),
                'Watermark_Required': component_data.get('Watermark_Required', 'FALSE'),
                'Delivery_Status': 'PENDING',
                'Created_At': get_current_zulu()
            }

            response = self.component_table.put_item(Item=item)
            logger.info(f"Component {item['Component_ID']} created for DA={record_id}, Status=PENDING")
            return response

        except ClientError as e:
            logger.error(f"Error creating component for ID={record_id}: {e}")
            raise

    def get_components_by_id(self, record_id: str) -> List[Dict]:
        try:
            response = self.component_table.query(
                KeyConditionExpression='ID = :id',
                ExpressionAttributeValues={':id': record_id}
            )
            return response.get('Items', [])
        except ClientError as e:
            logger.error(f"Error getting components for ID {record_id}: {e}")
            return []

    def get_studio_config(self, studio_id: str = None) -> Optional[Dict]:
        try:
            response = self.studio_config_table.get_item(Key={'Studio_ID': '1234'})
            config = response.get('Item')
            if config:
                logger.info(f"Retrieved studio config for Studio_ID=1234")
            else:
                logger.warning(f"No studio config found for Studio_ID=1234")
            return config
        except ClientError as e:
            logger.error(f"Error fetching studio config: {e}")
            return None

    def create_job(self, job_data):
        self.table.put_item(Item=job_data, ConditionExpression="attribute_not_exists(job_id)")
        return job_data

    def update_job(self, job_id, updates: dict):
        update_expression = "SET " + ", ".join(f"#{k}= :{k}" for k in updates.keys())
        expression_values = {f":{k}": v for k, v in updates.items()}
        expression_names = {f"#{k}": k for k in updates.keys()}

        self.table.update_item(
            Key={"job_id": job_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ExpressionAttributeNames=expression_names
        )