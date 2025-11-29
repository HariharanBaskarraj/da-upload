"""
File Delivery Service with enhanced version tracking and status aggregation.
"""
import logging
import boto3
from typing import Dict, List, Optional
from django.conf import settings
from botocore.exceptions import ClientError
from da_processor.utils.date_utils import get_current_zulu

logger = logging.getLogger(__name__)


class FileDeliveryService:
    """Service for tracking file deliveries with version-based status updates."""

    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name=settings.AWS_REGION)
        self.dynamodb_client = boto3.client('dynamodb', region_name=settings.AWS_REGION)
        self.file_tracker_table = self.dynamodb.Table(settings.DYNAMODB_FILE_DELIVERY_TABLE)
        self.component_table = self.dynamodb.Table(settings.DYNAMODB_COMPONENT_TABLE)
        self.da_table = self.dynamodb.Table(settings.DYNAMODB_DA_TABLE)
        self.asset_table = self.dynamodb.Table(settings.DYNAMODB_ASSET_TABLE)

    def track_file_delivery(self, da_id: str, asset: Dict, file_status: str) -> Dict:
        """Track file delivery with version comparison from asset-info table."""
        try:
            asset_id = asset.get('Asset_ID') or ''
            filename = asset.get('Filename', '')
            checksum = asset.get('Checksum', '')
            new_version = int(asset.get('Version', 1))

            if not asset_id:
                msg = f"Invalid tracking request: empty Asset_ID for DA {da_id}"
                logger.error(msg)
                raise ValueError(msg)

            current_time = get_current_zulu()
            existing = self._get_file_tracker(da_id, asset_id)

            if existing:
                old_version = int(existing.get('Version', 1))
                
                if new_version > old_version:
                    file_status_calc = 'REVISED'
                else:
                    file_status_calc = 'NO_CHANGE'

                update_expr = 'SET #status = :status, Checksum = :checksum, Date_Last_Delivered = :last_delivered, Version = :version'
                expr_attr_names = {'#status': 'File_Status'}
                expr_attr_values = {
                    ':status': file_status_calc,
                    ':checksum': checksum,
                    ':last_delivered': current_time,
                    ':version': new_version
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

                logger.info(f"[TRACK] Updated: DA={da_id}, Asset={asset_id}, Status={file_status_calc}, Version={old_version}->{new_version}")
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
                    'Version': new_version,
                    'Revision_Count': 0,
                    'Folder_Path': asset.get('Folder_Path', ''),
                    'Studio_Asset_ID': asset.get('Studio_Asset_ID', ''),
                    'Studio_Revision_Notes': asset.get('Studio_Revision_Notes', ''),
                    'Studio_Revision_Urgency': asset.get('Studio_Revision_Urgency', '')
                }

                self.file_tracker_table.put_item(Item=item)
                logger.info(f"[TRACK] Created NEW: DA={da_id}, Asset={asset_id}, Version={new_version}, Component={item['Component_ID']}")
                file_status = 'NEW'

            return {
                'asset_id': asset_id,
                'file_status': file_status,
                'delivered_at': current_time
            }

        except Exception as e:
            logger.error(f"Error tracking file delivery: {e}", exc_info=True)
            raise

    def update_component_delivery_status(self, da_id: str, component_id: str, title_id: str, version_id: str) -> None:
        """Update component status based on delivered vs expected assets."""
        try:
            component_key = {'ID': da_id, 'Component_ID': component_id}
            response = self.component_table.get_item(Key=component_key)
            
            if 'Item' not in response:
                logger.warning(f"[COMP_STATUS] Component not found: DA={da_id}, Component={component_id}")
                return

            # Get delivered files for this component
            component_files = self.get_files_by_component(da_id, component_id)
            
            # Get expected assets for this component
            expected_assets = self._get_expected_assets_for_component(title_id, version_id, component_id)

            logger.info(
                f"[COMP_STATUS] Component {component_id}: "
                f"Expected={len(expected_assets)}, Delivered={len(component_files)}"
            )

            if not expected_assets:
                logger.warning(f"[COMP_STATUS] No expected assets for component {component_id}, skipping status update")
                return

            delivered_asset_ids = {f.get('Asset_Id') for f in component_files}
            expected_asset_ids = {a.get('AssetId') or a.get('Asset_Id') for a in expected_assets}

            logger.debug(f"[COMP_STATUS] Expected IDs: {expected_asset_ids}")
            logger.debug(f"[COMP_STATUS] Delivered IDs: {delivered_asset_ids}")

            # Get Is_Active
            da_info = self.da_table.get_item(Key={'ID': da_id}).get('Item', {})
            is_active = da_info.get('Is_Active', False)

            current_time = get_current_zulu()

            all_delivered = expected_asset_ids.issubset(delivered_asset_ids)
            
            if not delivered_asset_ids:
                delivery_status = 'PENDING'
            elif all_delivered and is_active:
                delivery_status = 'COMPLETED'
            else:
                delivery_status = 'PARTIAL'

            update_expr = 'SET Delivery_Status = :status, Date_Last_Delivered = :last_delivered'
            expr_attr_values = {
                ':status': delivery_status,
                ':last_delivered': current_time
            }

            existing_component = response['Item']
            is_first_delivery = 'Original_Delivery_Date' not in existing_component

            if is_first_delivery and delivered_asset_ids:
                update_expr += ', Original_Delivery_Date = :original_delivery'
                expr_attr_values[':original_delivery'] = current_time

            self.component_table.update_item(
                Key=component_key,
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_attr_values
            )

            logger.info(
                f"[COMP_STATUS] Updated: DA={da_id}, Component={component_id}, "
                f"Status={delivery_status}, Is_Active={is_active}, "
                f"All_Delivered={all_delivered}"
            )

        except Exception as e:
            logger.error(f"Error updating component status: {e}", exc_info=True)
            raise

    def update_da_delivery_status(self, da_id: str) -> None:
        """Update DA-level status based on all components."""
        try:
            components = self._get_components_for_da(da_id)
            if not components:
                logger.warning(f"[DA_STATUS] No components found for DA: {da_id}")
                return

            component_statuses = [c.get('Delivery_Status', 'PENDING') for c in components]
            
            logger.info(f"[DA_STATUS] Component statuses for DA {da_id}: {component_statuses}")
            
            da_info = self.da_table.get_item(Key={'ID': da_id}).get('Item', {})
            is_active = da_info.get('Is_Active', False)

            all_completed = all(status == 'COMPLETED' for status in component_statuses)
            all_pending = all(status == 'PENDING' for status in component_statuses)

            if all_pending:
                delivery_status = 'PENDING'
            elif all_completed and is_active:
                delivery_status = 'COMPLETED'
            else:
                delivery_status = 'PARTIAL'

            current_time = get_current_zulu()

            existing_da = da_info
            is_first_delivery = 'Original_Delivery_Date' not in existing_da

            update_expr = 'SET Delivery_Status = :status, Date_Last_Delivered = :last_delivered'
            expr_attr_values = {
                ':status': delivery_status,
                ':last_delivered': current_time
            }

            if is_first_delivery and any(s != 'PENDING' for s in component_statuses):
                update_expr += ', Original_Delivery_Date = :original_delivery'
                expr_attr_values[':original_delivery'] = current_time

            self.da_table.update_item(
                Key={'ID': da_id},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_attr_values
            )

            logger.info(
                f"[DA_STATUS] Updated: DA={da_id}, Status={delivery_status}, "
                f"Is_Active={is_active}, All_Completed={all_completed}, All_Pending={all_pending}"
            )

        except Exception as e:
            logger.error(f"Error updating DA status: {e}", exc_info=True)
            raise

    def get_files_for_da(self, da_id: str) -> List[Dict]:
        try:
            response = self.file_tracker_table.query(
                KeyConditionExpression='DA_ID = :da_id',
                ExpressionAttributeValues={':da_id': da_id}
            )
            return response.get('Items', [])
        except ClientError as e:
            logger.error(f"Error getting files for DA {da_id}: {e}")
            return []

    def get_files_by_component(self, da_id: str, component_id: str) -> List[Dict]:
        files = self.get_files_for_da(da_id)
        return [f for f in files if f.get('Component_ID') == component_id]

    def _get_file_tracker(self, da_id: str, asset_id: str) -> Optional[Dict]:
        try:
            if not asset_id:
                return None
            response = self.file_tracker_table.get_item(Key={'DA_ID': da_id, 'Asset_Id': asset_id})
            return response.get('Item')
        except ClientError as e:
            logger.error(f"Error getting file tracker: {e}")
            return None

    def _get_licensee_id_for_da(self, da_id: str) -> str:
        try:
            response = self.da_table.get_item(Key={'ID': da_id})
            if 'Item' in response:
                return response['Item'].get('Licensee_ID', '')
            return ''
        except ClientError as e:
            logger.error(f"Error getting licensee ID: {e}")
            return ''

    def _infer_component_id(self, asset: Dict) -> str:
        folder_path = asset.get('Folder_Path', '').replace('\\', '/').strip('/')
        title_id = asset.get('Title_ID', '')
        version_id = asset.get('Version_ID', '')
        filename = asset.get('Filename', '')

        logger.info(f"Asset: {asset}")
        
        # Remove filename from folder_path if present
        if folder_path.endswith(f"/{filename}"):
            folder_path = folder_path[:-len(f"/{filename}")]
        elif folder_path.endswith(filename):
            folder_path = folder_path[:-len(filename)].rstrip('/')
        logger.debug(folder_path)
        # Remove title.version prefix
        prefix_candidates = [f"{title_id}.{version_id}/", f"{title_id}_{version_id}/"]
        
        normalized_path = folder_path
        for prefix in prefix_candidates:
            if normalized_path.startswith(prefix):
                normalized_path = normalized_path[len(prefix):]
                break

        logger.debug(f"[INFER_COMP] Original: {folder_path}, Normalized: {normalized_path}")

        try:
            response = self.dynamodb_client.scan(
                TableName=settings.DYNAMODB_COMPONENT_CONFIG_TABLE
            )

            items = response.get('Items', [])
            
            best_match = None
            best_length = -1

            for item in items:
                component_id = item.get('ComponentId', {}).get('S', '')
                folder_structure = item.get('Folder Structure', {}).get('S', '').replace('\\', '/').strip('/')

                logger.debug(f"[INFER_COMP] Checking component {component_id} with folder {folder_structure}")

                # Match exact or prefix match
                if normalized_path == folder_structure or normalized_path.startswith(folder_structure + '/'):
                    # Choose the *longest* match to avoid generic-folder overrides
                    if len(folder_structure) > best_length:
                        best_match = component_id
                        best_length = len(folder_structure)

            if best_match:
                logger.info(f"[INFER_COMP] BEST MATCH: {best_match} for path: {normalized_path}")
                return best_match

            logger.warning(f"[INFER_COMP] No match found for path: {normalized_path}")

        except Exception as e:
            logger.error(f"[INFER_COMP] Error inferring component: {e}", exc_info=True)

        return 'UNKNOWN'
    def _get_components_for_da(self, da_id: str) -> List[Dict]:
        try:
            response = self.component_table.query(
                KeyConditionExpression='ID = :id',
                ExpressionAttributeValues={':id': da_id}
            )
            return response.get('Items', [])
        except ClientError as e:
            logger.error(f"Error getting components for DA {da_id}: {e}")
            return []

    def _get_expected_assets_for_component(self, title_id: str, version_id: str, component_id: str) -> List[Dict]:
        try:
            # Get component folder structure
            component_config_response = self.dynamodb_client.scan(
                TableName=settings.DYNAMODB_COMPONENT_CONFIG_TABLE,
                FilterExpression='ComponentId = :comp_id',
                ExpressionAttributeValues={':comp_id': {'S': component_id}}
            )

            items = component_config_response.get('Items', [])
            if not items:
                logger.warning(f"[EXPECTED_ASSETS] No component config for: {component_id}")
                return []

            folder_structure = items[0].get('Folder Structure', {}).get('S', '').replace('\\', '/').strip('/')
            logger.info(f"[EXPECTED_ASSETS] Component {component_id} folder structure: {folder_structure}")

            # Get all assets for this title/version
            response = self.asset_table.query(
                IndexName='Title_ID-Version_ID-index',
                KeyConditionExpression='Title_ID = :title_id AND Version_ID = :version_id',
                ExpressionAttributeValues={
                    ':title_id': title_id,
                    ':version_id': version_id
                }
            )

            all_assets = response.get('Items', [])
            logger.info(f"[EXPECTED_ASSETS] Total assets for {title_id}/{version_id}: {len(all_assets)}")

            matching_assets = []
            prefix_candidates = [f"{title_id}.{version_id}/", f"{title_id}_{version_id}/"]

            for asset in all_assets:
                folder_path = asset.get('Folder_Path', '').replace('\\', '/').strip('/')

                # Remove title/version prefix
                for prefix in prefix_candidates:
                    if folder_path.startswith(prefix):
                        folder_path = folder_path[len(prefix):]
                        break

                # Check if matches component folder structure
                if folder_path.startswith(folder_structure):
                    matching_assets.append(asset)
                    logger.debug(f"[EXPECTED_ASSETS] Matched asset: {asset.get('Filename')} for component {component_id}")

            logger.info(f"[EXPECTED_ASSETS] Matched {len(matching_assets)} assets for component {component_id}")
            return matching_assets

        except ClientError as e:
            logger.error(f"Error getting expected assets for component {component_id}: {e}")
            return []