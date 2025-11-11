import boto3
import uuid
import logging
from datetime import datetime
from typing import Dict, List, Optional
from django.conf import settings
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class DynamoDBService:
    """
    Service layer for interacting with DynamoDB tables for DA processing.
    """

    def __init__(self):
        self.dynamodb = boto3.resource(
            'dynamodb', region_name=settings.AWS_REGION)
        self.title_table = self.dynamodb.Table(settings.DYNAMODB_TITLE_TABLE)
        self.component_table = self.dynamodb.Table(
            settings.DYNAMODB_COMPONENT_TABLE)
        self.licensee_defaults_table = self.dynamodb.Table(
            settings.DYNAMODB_CONFIG_TABLE)

    # ---------------------------------------------------------------------
    # 🟢 Title (DA Upload) Records
    # ---------------------------------------------------------------------

    def create_or_update_title(self, title_data: Dict) -> Dict:
        """
        Create a new DA upload record in DynamoDB with a unique ID.
        """
        try:
            record_id = str(uuid.uuid4())  # ✅ Unique ID for each DA upload

            item = {
                'ID': record_id,  # Primary key
                'TitleID': title_data.get('TitleID', ''),
                'TitleName': title_data.get('TitleName', ''),
                'TitleEIDRID': title_data.get('TitleEIDRID', ''),
                'VersionID': title_data.get('VersionID', ''),
                'VersionName': title_data.get('VersionName', ''),
                'VersionEIDRID': title_data.get('VersionEIDRID', ''),
                'ReleaseYear': title_data.get('ReleaseYear', ''),
                'LicenseeID': title_data.get('LicenseeID', ''),
                'DADescription': title_data.get('DADescription', ''),
                'DueDate': title_data.get('DueDate', ''),
                'EarliestDeliveryDate': title_data.get('EarliestDeliveryDate', ''),
                'LicensePeriodStart': title_data.get('LicensePeriodStart', ''),
                'LicensePeriodEnd': title_data.get('LicensePeriodEnd', ''),
                'Territories': title_data.get('Territories', ''),
                'ExceptionNotificationDate': title_data.get('ExceptionNotificationDate', ''),
                'ExceptionRecipients': title_data.get('ExceptionRecipients', ''),
                'InternalStudioID': title_data.get('InternalStudioID', ''),
                'StudioSystemID': title_data.get('StudioSystemID', ''),
                'CreatedAt': datetime.utcnow().isoformat() + 'Z'
            }

            response = self.title_table.put_item(Item=item)
            logger.info(f"✅ DA upload record created: ID={record_id}")
            return {"ID": record_id, "response": response}

        except ClientError as e:
            logger.error(f"❌ Error creating DA upload record: {e}")
            raise

    def get_title(self, record_id: str) -> Optional[Dict]:
        """Get a DA upload record by its unique ID."""
        try:
            response = self.title_table.get_item(Key={'ID': record_id})
            return response.get('Item')
        except ClientError as e:
            logger.error(f"Error getting DA record {record_id}: {e}")
            return None

    # ---------------------------------------------------------------------
    # 🧩 Component Records
    # ---------------------------------------------------------------------

    def create_component(self, record_id: str, title_id: str, component_data: Dict) -> Dict:
        """
        Create or update a component record tied to a specific DA upload.
        """
        try:
            item = {
                'ID': record_id,  # ✅ Foreign key to title record
                'TitleID': title_id,  # ✅ Reference to parent title
                'ComponentID': component_data.get('ComponentID', ''),
                'RequiredFlag': component_data.get('RequiredFlag', 'FALSE'),
                'WatermarkRequired': component_data.get('WatermarkRequired', 'FALSE'),
                'CreatedAt': datetime.utcnow().isoformat() + 'Z'
            }

            response = self.component_table.put_item(Item=item)
            logger.info(
                f"Component {item['ComponentID']} added for ID={record_id}, TitleID={title_id}")
            return response

        except ClientError as e:
            logger.error(f"❌ Error creating component for ID={record_id}: {e}")
            raise

    def get_components_by_id(self, record_id: str) -> List[Dict]:
        """Get all component records linked to a specific DA upload."""
        try:
            response = self.component_table.query(
                KeyConditionExpression='ID = :id',
                ExpressionAttributeValues={':id': record_id}
            )
            return response.get('Items', [])
        except ClientError as e:
            logger.error(f"Error getting components for ID {record_id}: {e}")
            return []

    # ---------------------------------------------------------------------
    # ⚙️ Licensee Defaults
    # ---------------------------------------------------------------------

    def get_licensee_defaults(self, licensee_id: str) -> Optional[Dict]:
        """Fetch licensee-specific default settings from DynamoDB."""
        try:
            response = self.licensee_defaults_table.get_item(
                Key={'LicenseeID': licensee_id})
            return response.get('Item')
        except ClientError as e:
            logger.error(
                f"Error fetching licensee defaults for {licensee_id}: {e}")
            return None
