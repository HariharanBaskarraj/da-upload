import logging
import boto3
from typing import Dict, List, Optional
from django.conf import settings
from botocore.exceptions import ClientError
from da_processor.utils.date_utils import get_current_zulu

logger = logging.getLogger(__name__)


class FileDeliveryService:

    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name=settings.AWS_REGION)
        self.dynamodb_client = boto3.client('dynamodb', region_name=settings.AWS_REGION)
        self.file_tracker_table = self.dynamodb.Table(settings.DYNAMODB_FILE_DELIVERY_TABLE)
        self.component_table = self.dynamodb.Table(settings.DYNAMODB_COMPONENT_TABLE)
        self.da_table = self.dynamodb.Table(settings.DYNAMODB_DA_TABLE)
        self.asset_table = self.dynamodb.Table(settings.DYNAMODB_ASSET_TABLE)

    def track_file_delivery(self, da_id: str, asset: Dict, file_status: str) -> Dict:
        """
        Tracks or creates a file delivery record for DA_ID + Asset_Id (DynamoDB primary key).
        Validates asset id is present and not empty.
        """
        try:
            asset_id = asset.get('Asset_Id') or asset.get('AssetId') or ''
            filename = asset.get('Filename', '')
            checksum = asset.get('Checksum', '')

            logger.debug(f"[TRACK] Received tracking request: DA={da_id}, Asset_Id={asset_id}, Filename={filename}, Status={file_status}")

            if not asset_id:
                msg = f"Invalid tracking request: empty Asset_Id for DA {da_id}, filename {filename}"
                logger.error(msg)
                raise ValueError(msg)

            current_time = get_current_zulu()

            existing = self._get_file_tracker(da_id, asset_id)

            if existing:
                logger.debug(f"[TRACK] Existing tracker record found for DA={da_id}, Asset_Id={asset_id}: {existing}")
                old_checksum = existing.get('Checksum', '')

                if old_checksum == checksum:
                    file_status_calc = 'NO_CHANGE'
                else:
                    file_status_calc = 'REVISED'

                update_expr = 'SET #status = :status, Checksum = :checksum, Date_Last_Delivered = :last_delivered, Version = :version'
                expr_attr_names = {'#status': 'File_Status'}
                expr_attr_values = {
                    ':status': file_status_calc,
                    ':checksum': checksum,
                    ':last_delivered': current_time,
                    ':version': int(asset.get('Version', 1))
                }

                if file_status_calc == 'REVISED':
                    update_expr += ', Revision_Count = if_not_exists(Revision_Count, :zero) + :one'
                    expr_attr_values[':zero'] = 0
                    expr_attr_values[':one'] = 1

                self.file_tracker_table.update_item(
                    Key={'DA_ID': da_id, 'Asset_Id': asset_id},
                    UpdateExpression=update_expr,
                    ExpressionAttributeNames=expr_attr_names,
                    ExpressionAttributeValues=expr_attr_values
                )

                logger.info(f"[TRACK] Updated file tracker: DA={da_id}, Asset_Id={asset_id}, Status={file_status_calc}")
                file_status = file_status_calc
            else:
                item = {
                    'DA_ID': da_id,
                    'Asset_Id': asset_id,
                    'Filename': filename,
                    'Title_ID': asset.get('Title_ID', ''),
                    'Version_ID': asset.get('Version_ID', ''),
                    'Licensee_ID': self._get_licensee_id_for_da(da_id),
                    'Component_ID': self._infer_component_id(asset),
                    'Checksum': checksum,
                    'File_Status': 'NEW',
                    'Original_Delivery_Date': current_time,
                    'Date_Last_Delivered': current_time,
                    'Version': int(asset.get('Version', 1)),
                    'Revision_Count': 0,
                    'Folder_Path': asset.get('Folder_Path', ''),
                    'Studio_Asset_ID': asset.get('Studio_Asset_ID', ''),
                    'Studio_Revision_Notes': asset.get('Studio_Revision_Notes', ''),
                    'Studio_Revision_Urgency': asset.get('Studio_Revision_Urgency', '')
                }

                logger.debug(f"[TRACK] Creating new file tracker item: {item}")

                # DynamoDB put_item will fail if Asset_Id is empty — validated above
                self.file_tracker_table.put_item(Item=item)
                logger.info(f"[TRACK] Created file tracker: DA={da_id}, Asset_Id={asset_id}, Filename={filename}")
                file_status = 'NEW'

            return {
                'asset_id': asset_id,
                'file_status': file_status,
                'delivered_at': current_time
            }

        except ClientError as e:
            logger.error(f"Error tracking file delivery (ClientError): {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Error tracking file delivery: {e}", exc_info=True)
            raise

    def get_files_for_da(self, da_id: str) -> List[Dict]:
        try:
            response = self.file_tracker_table.query(
                KeyConditionExpression='DA_ID = :da_id',
                ExpressionAttributeValues={':da_id': da_id}
            )
            items = response.get('Items', [])
            logger.debug(f"[GET_FILES] Found {len(items)} file tracker items for DA={da_id}")
            return items
        except ClientError as e:
            logger.error(f"Error getting files for DA {da_id}: {e}", exc_info=True)
            return []

    def get_files_by_component(self, da_id: str, component_id: str) -> List[Dict]:
        files = self.get_files_for_da(da_id)
        return [f for f in files if f.get('Component_ID') == component_id]

    def update_component_delivery_status(self, da_id: str, component_id: str, title_id: str, version_id: str) -> None:
        try:
            component_key = {'ID': da_id, 'Component_ID': component_id}

            response = self.component_table.get_item(Key=component_key)
            if 'Item' not in response:
                logger.warning(f"[COMP_STATUS] Component not found: DA={da_id}, Component={component_id}")
                return

            component_files = self.get_files_by_component(da_id, component_id)
            expected_assets = self._get_expected_assets_for_component(title_id, version_id, component_id)

            if not expected_assets:
                logger.info(f"[COMP_STATUS] No expected assets found for component {component_id}")
                return

            delivered_asset_ids = {f.get('Asset_Id') or f.get('AssetId') for f in component_files}
            expected_asset_ids = {a.get('AssetId') or a.get('Asset_Id') for a in expected_assets}

            current_time = get_current_zulu()

            if expected_asset_ids.issubset(delivered_asset_ids):
                delivery_status = 'COMPLETE'
            elif delivered_asset_ids:
                delivery_status = 'PARTIAL'
            else:
                delivery_status = 'PENDING'

            existing_component = response['Item']
            is_first_delivery = 'Original_Delivery_Date' not in existing_component

            update_expr = 'SET Delivery_Status = :status, Date_Last_Delivered = :last_delivered'
            expr_attr_values = {
                ':status': delivery_status,
                ':last_delivered': current_time
            }

            if is_first_delivery:
                update_expr += ', Original_Delivery_Date = :original_delivery'
                expr_attr_values[':original_delivery'] = current_time

            self.component_table.update_item(
                Key=component_key,
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_attr_values
            )

            logger.info(f"[COMP_STATUS] Updated component delivery status: DA={da_id}, Component={component_id}, Status={delivery_status}")

        except ClientError as e:
            logger.error(f"Error updating component status: {e}", exc_info=True)
            raise

    def update_da_delivery_status(self, da_id: str) -> None:
        try:
            components = self._get_components_for_da(da_id)
            if not components:
                logger.warning(f"[DA_STATUS] No components found for DA: {da_id}")
                return

            component_statuses = [c.get('Delivery_Status', 'PENDING') for c in components]

            if all(status == 'COMPLETE' for status in component_statuses):
                delivery_status = 'COMPLETE'
            elif any(status in ['COMPLETE', 'PARTIAL'] for status in component_statuses):
                delivery_status = 'PARTIAL'
            else:
                delivery_status = 'PENDING'

            current_time = get_current_zulu()

            da_response = self.da_table.get_item(Key={'ID': da_id})
            if 'Item' not in da_response:
                logger.warning(f"[DA_STATUS] DA not found: {da_id}")
                return

            existing_da = da_response['Item']
            is_first_delivery = 'Original_Delivery_Date' not in existing_da

            update_expr = 'SET Delivery_Status = :status, Date_Last_Delivered = :last_delivered'
            expr_attr_values = {
                ':status': delivery_status,
                ':last_delivered': current_time
            }

            if is_first_delivery:
                update_expr += ', Original_Delivery_Date = :original_delivery'
                expr_attr_values[':original_delivery'] = current_time

            self.da_table.update_item(
                Key={'ID': da_id},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_attr_values
            )

            logger.info(f"[DA_STATUS] Updated DA delivery status: DA={da_id}, Status={delivery_status}")

        except ClientError as e:
            logger.error(f"Error updating DA status: {e}", exc_info=True)
            raise

    def _get_file_tracker(self, da_id: str, asset_id: str) -> Optional[Dict]:
        try:
            if not asset_id:
                logger.debug(f"[GET_TRACKER] Empty asset_id passed for DA={da_id}")
                return None
            response = self.file_tracker_table.get_item(Key={'DA_ID': da_id, 'Asset_Id': asset_id})
            item = response.get('Item')
            logger.debug(f"[GET_TRACKER] get_item response for DA={da_id}, Asset_Id={asset_id}: {item}")
            return item
        except ClientError as e:
            logger.error(f"Error getting file tracker: {e}", exc_info=True)
            return None

    def _get_licensee_id_for_da(self, da_id: str) -> str:
        try:
            response = self.da_table.get_item(Key={'ID': da_id})
            if 'Item' in response:
                return response['Item'].get('Licensee_ID', '')
            return ''
        except ClientError as e:
            logger.error(f"Error getting licensee ID: {e}", exc_info=True)
            return ''

    def _infer_component_id(self, asset: Dict) -> str:
        folder_path = asset.get('Folder_Path', '').replace('\\', '/').strip('/')
        title_id = asset.get('Title_ID', '')
        version_id = asset.get('Version_ID', '')

        prefix_candidates = [f"{title_id}.{version_id}/", f"{title_id}_{version_id}/"]

        for prefix in prefix_candidates:
            if folder_path.startswith(prefix):
                folder_path = folder_path[len(prefix):]
                break

        try:
            response = self.dynamodb_client.scan(
                TableName=settings.DYNAMODB_COMPONENT_CONFIG_TABLE,
                FilterExpression='contains(#folder, :folder)',
                ExpressionAttributeNames={'#folder': 'Folder Structure'},
                ExpressionAttributeValues={':folder': {'S': folder_path}}
            )

            items = response.get('Items', [])
            if items:
                component_id = items[0].get('ComponentId', {}).get('S', '')
                return component_id
        except Exception as e:
            logger.warning(f"Could not infer component ID for folder {folder_path}: {e}")

        return 'UNKNOWN'

    def _get_components_for_da(self, da_id: str) -> List[Dict]:
        try:
            response = self.component_table.query(
                KeyConditionExpression='ID = :id',
                ExpressionAttributeValues={':id': da_id}
            )
            return response.get('Items', [])
        except ClientError as e:
            logger.error(f"Error getting components for DA {da_id}: {e}", exc_info=True)
            return []

    def _get_expected_assets_for_component(self, title_id: str, version_id: str, component_id: str) -> List[Dict]:
        try:
            component_config_response = self.dynamodb_client.scan(
                TableName=settings.DYNAMODB_COMPONENT_CONFIG_TABLE,
                FilterExpression='ComponentId = :comp_id',
                ExpressionAttributeValues={':comp_id': {'S': component_id}}
            )

            items = component_config_response.get('Items', [])
            if not items:
                return []

            folder_structure = items[0].get('Folder Structure', {}).get('S', '').replace('\\', '/').strip('/')

            response = self.asset_table.query(
                IndexName='Title_ID-Version_ID-index',
                KeyConditionExpression='Title_ID = :title_id AND Version_ID = :version_id',
                ExpressionAttributeValues={
                    ':title_id': title_id,
                    ':version_id': version_id
                }
            )

            all_assets = response.get('Items', [])

            matching_assets = []
            prefix_candidates = [f"{title_id}.{version_id}/", f"{title_id}_{version_id}/"]

            for asset in all_assets:
                folder_path = asset.get('Folder_Path', '').replace('\\', '/').strip('/')

                for prefix in prefix_candidates:
                    if folder_path.startswith(prefix):
                        folder_path = folder_path[len(prefix):]
                        break

                if folder_path.startswith(folder_structure):
                    matching_assets.append(asset)

            return matching_assets

        except ClientError as e:
            logger.error(f"Error getting expected assets for component {component_id}: {e}", exc_info=True)
            return []
