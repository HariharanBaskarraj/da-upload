import boto3
import uuid
import logging
from typing import Dict, List, Optional
from django.conf import settings
from botocore.exceptions import ClientError
from da_processor.utils.date_utils import to_zulu, get_current_zulu

logger = logging.getLogger(__name__)


class DynamoDBService:

    def __init__(self):
        self.dynamodb = boto3.resource(
            'dynamodb', region_name=settings.AWS_REGION)
        self.da_table = self.dynamodb.Table(settings.DYNAMODB_DA_TABLE)
        self.title_table = self.dynamodb.Table(settings.DYNAMODB_TITLE_TABLE)
        self.component_table = self.dynamodb.Table(
            settings.DYNAMODB_COMPONENT_TABLE)
        self.studio_config_table = self.dynamodb.Table(
            settings.DYNAMODB_STUDIO_CONFIG_TABLE)

    def create_or_update_title_info(self, title_data: Dict) -> Dict:
        try:
            title_id = title_data.get('TitleID', '')
            version_id = title_data.get('VersionID', '')
            
            if not title_id or not version_id:
                raise ValueError("TitleID and VersionID are required")
            
            response = self.title_table.get_item(Key={
                'TitleID': title_id,
                'VersionID': version_id
            })
            
            is_new = 'Item' not in response
            
            if is_new:
                title_item = {
                    'TitleID': title_id,
                    'VersionID': version_id,
                    'TitleName': title_data.get('TitleName', ''),
                    'TitleEIDRID': title_data.get('TitleEIDRID', ''),
                    'VersionName': title_data.get('VersionName', ''),
                    'VersionEIDRID': title_data.get('VersionEIDRID', ''),
                    'ReleaseYear': title_data.get('ReleaseYear', ''),
                    'Uploader': title_data.get('Uploader', 'SYSTEM'),
                    'CreatedAt': get_current_zulu()
                }
                self.title_table.put_item(Item=title_item)
                logger.info(f"Created new title info record: TitleID={title_id}, VersionID={version_id}")
            else:
                logger.info(f"Title info already exists, no update performed: TitleID={title_id}, VersionID={version_id}")
            
            return {"is_new": is_new}

        except ClientError as e:
            logger.error(f"Error in create_or_update_title_info: {e}")
            raise

    def create_da_record(self, da_data: Dict) -> Dict:
        try:
            record_id = str(uuid.uuid4())

            item = {
                'ID': record_id,
                'TitleID': da_data.get('TitleID', ''),
                'VersionID': da_data.get('VersionID', ''),
                'LicenseeID': da_data.get('LicenseeID', ''),
                'DADescription': da_data.get('DADescription', ''),
                'DueDate': to_zulu(da_data.get('DueDate')) or '',
                'EarliestDeliveryDate': to_zulu(da_data.get('EarliestDeliveryDate')) or '',
                'LicensePeriodStart': to_zulu(da_data.get('LicensePeriodStart')) or '',
                'LicensePeriodEnd': to_zulu(da_data.get('LicensePeriodEnd')) or '',
                'Territories': da_data.get('Territories', ''),
                'ExceptionNotificationDate': to_zulu(da_data.get('ExceptionNotificationDate')) or '',
                'ExceptionRecipients': da_data.get('ExceptionRecipients', ''),
                'InternalStudioID': da_data.get('InternalStudioID', ''),
                'StudioSystemID': da_data.get('StudioSystemID', ''),
                'CreatedAt': get_current_zulu()
            }

            response = self.da_table.put_item(Item=item)
            logger.info(f"DA record created: ID={record_id}, TitleID={item['TitleID']}, VersionID={item['VersionID']}")
            return {"ID": record_id, "response": response}

        except ClientError as e:
            logger.error(f"Error creating DA record: {e}")
            raise

    def get_da_record(self, record_id: str) -> Optional[Dict]:
        try:
            response = self.da_table.get_item(Key={'ID': record_id})
            return response.get('Item')
        except ClientError as e:
            logger.error(f"Error getting DA record {record_id}: {e}")
            return None

    def create_component(self, record_id: str, title_id: str, component_data: Dict) -> Dict:
        try:
            item = {
                'ID': record_id,
                'TitleID': title_id,
                'ComponentID': component_data.get('ComponentID', ''),
                'RequiredFlag': component_data.get('RequiredFlag', 'FALSE'),
                'WatermarkRequired': component_data.get('WatermarkRequired', 'FALSE'),
                'CreatedAt': get_current_zulu()
            }

            response = self.component_table.put_item(Item=item)
            logger.info(
                f"Component {item['ComponentID']} added for ID={record_id}, TitleID={title_id}")
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

    def get_studio_config(self, studio_id: str) -> Optional[Dict]:
        try:
            response = self.studio_config_table.get_item(
                Key={'StudioID': studio_id})
            return response.get('Item')
        except ClientError as e:
            logger.error(
                f"Error fetching studio config for {studio_id}: {e}")
            return None