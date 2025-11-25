import logging
import boto3
from typing import Dict, Optional
from datetime import datetime, timedelta
from django.conf import settings
from da_processor.services.file_delivery_service import FileDeliveryService
from da_processor.services.manifest_service import ManifestService
from da_processor.services.sqs_service import SQSService
from da_processor.utils.date_utils import parse_date, get_current_zulu

logger = logging.getLogger(__name__)


class DeliveryOrchestratorService:

    def __init__(self):
        self.file_delivery_service = FileDeliveryService()
        self.manifest_service = ManifestService()
        self.sqs_service = SQSService()
        self.dynamodb = boto3.resource('dynamodb', region_name=settings.AWS_REGION)
        self.da_table = self.dynamodb.Table(settings.DYNAMODB_DA_TABLE)
        self.licensee_table = self.dynamodb.Table(settings.DYNAMODB_LICENSEE_TABLE)

    def process_delivery_for_da(self, da_id: str) -> Dict:
        try:
            logger.info(f"[DELIVERY] Starting delivery process for DA: {da_id}")

            da_info = self._get_da_info(da_id)
            if not da_info:
                raise ValueError(f"DA not found: {da_id}")

            if not self._is_within_delivery_window(da_info):
                logger.info(f"[DELIVERY] DA {da_id} is outside delivery window, skipping")
                return {'success': False, 'reason': 'outside_delivery_window', 'da_id': da_id}

            manifest = self.manifest_service.generate_manifest(da_id)
            assets = manifest.get('assets', [])

            if not assets:
                logger.info(f"[DELIVERY] No assets to deliver for DA {da_id}")
                return {'success': False, 'reason': 'no_assets', 'da_id': da_id}

            for asset_data in assets:
                # asset_data includes both 'asset_id' and 'Asset_Id' (populated by ManifestService)
                asset_id = asset_data.get('Asset_Id') or asset_data.get('asset_id') or ''
                if not asset_id:
                    logger.error(f"[DELIVERY] Skipping asset with missing id in manifest asset_data: {asset_data}")
                    continue

                # Folder_Path already stored in file_path or asset_data; preserve original Folder_Path if present
                folder_path = asset_data.get('file_path') or asset_data.get('file_path', '')
                # We prefer the original Folder_Path (which ManifestService stored under file_path already)
                folder_path_db = asset_data.get('file_path') or asset_data.get('file_path', '')

                asset_dict = {
                    'Asset_Id': asset_id,
                    'Filename': asset_data.get('file_name', ''),
                    'Checksum': asset_data.get('checksum', ''),
                    'Title_ID': manifest['main_body'].get('title_id', ''),
                    'Version_ID': manifest['main_body'].get('version_id', ''),
                    'Version': asset_data.get('revision_id', 1),
                    # Use the Folder_Path as-is (manifest stores file_path that is the DB Folder_Path)
                    'Folder_Path': asset_data.get('file_path', ''),
                    'Studio_Asset_ID': asset_data.get('studio_asset_id', ''),
                    'Studio_Revision_Notes': asset_data.get('studio_revision_notes', ''),
                    'Studio_Revision_Urgency': asset_data.get('studio_revision_urgency', '')
                }

                file_status = asset_data.get('file_status', 'NEW')

                logger.debug(f"[DELIVERY] Tracking asset: DA={da_id}, Asset_Id={asset_id}, Filename={asset_dict['Filename']}, Status={file_status}")
                try:
                    self.file_delivery_service.track_file_delivery(da_id, asset_dict, file_status)
                except Exception as e:
                    logger.error(f"[DELIVERY] Error tracking file delivery for DA={da_id}, Asset_Id={asset_id}: {e}", exc_info=True)

            # Update components and DA status
            components = self.file_delivery_service._get_components_for_da(da_id)
            for component in components:
                component_id = component.get('Component_ID')
                title_id = component.get('Title_ID')
                version_id = component.get('Version_ID')

                try:
                    self.file_delivery_service.update_component_delivery_status(
                        da_id, component_id, title_id, version_id
                    )
                except Exception as e:
                    logger.error(f"[DELIVERY] Error updating component status for DA={da_id}, Component={component_id}: {e}", exc_info=True)

            try:
                self.file_delivery_service.update_da_delivery_status(da_id)
            except Exception as e:
                logger.error(f"[DELIVERY] Error updating DA status for DA={da_id}: {e}", exc_info=True)

            new_or_revised_count = sum(1 for asset in assets if asset.get('file_status') in ['New', 'NEW', 'Revised', 'REVISED', 'Revised', 'NEW'])

            if new_or_revised_count > 0:
                licensee_id = da_info.get('Licensee_ID')

                if self._should_send_manifest(da_id, licensee_id):
                    enriched_manifest = self._enrich_manifest_with_file_status(manifest, da_id)
                    success = self.sqs_service.send_manifest_to_licensee(licensee_id, enriched_manifest)

                    if success:
                        self._update_next_manifest_check(da_id, licensee_id)
                        logger.info(f"[DELIVERY] Manifest sent successfully for DA {da_id}")
                        return {
                            'success': True,
                            'da_id': da_id,
                            'manifest_sent': True,
                            'new_or_revised_files': new_or_revised_count,
                            'total_files': len(assets)
                        }
                    else:
                        logger.error(f"[DELIVERY] Failed to send manifest for DA {da_id}")
                        return {'success': False, 'reason': 'sqs_send_failed', 'da_id': da_id}
                else:
                    logger.info(f"[DELIVERY] Skipping manifest send due to frequency limit for DA {da_id}")
                    return {'success': True, 'da_id': da_id, 'manifest_sent': False, 'reason': 'frequency_limit'}
            else:
                logger.info(f"[DELIVERY] No new or revised files for DA {da_id}, skipping manifest send")
                return {'success': True, 'da_id': da_id, 'manifest_sent': False, 'reason': 'no_changes'}

        except Exception as e:
            logger.error(f"[DELIVERY] Error processing delivery for DA {da_id}: {e}", exc_info=True)
            raise

    def _get_da_info(self, da_id: str) -> Optional[Dict]:
        try:
            response = self.da_table.get_item(Key={'ID': da_id})
            return response.get('Item')
        except Exception as e:
            logger.error(f"Error getting DA info: {e}")
            return None

    def _is_within_delivery_window(self, da_info: Dict) -> bool:
        earliest_delivery = da_info.get('Earliest_Delivery_Date')
        license_end = da_info.get('License_Period_End')

        if not earliest_delivery or not license_end:
            logger.warning("Missing delivery window dates")
            return False

        earliest_dt = parse_date(earliest_delivery)
        end_dt = parse_date(license_end)
        current_dt = datetime.now(earliest_dt.tzinfo if earliest_dt else None)

        if not earliest_dt or not end_dt:
            return False

        is_within = earliest_dt <= current_dt <= end_dt

        if not is_within:
            logger.info(f"Current time {current_dt} is outside window {earliest_dt} to {end_dt}")

        return is_within

    def _should_send_manifest(self, da_id: str, licensee_id: str) -> bool:
        try:
            da_info = self._get_da_info(da_id)
            next_check = da_info.get('Next_Manifest_Check') if da_info else None

            if not next_check:
                return True

            next_check_dt = parse_date(next_check)
            current_dt = datetime.now(next_check_dt.tzinfo if next_check_dt else None)

            if not next_check_dt:
                return True

            can_send = current_dt >= next_check_dt

            if not can_send:
                logger.info(f"Next manifest check for DA {da_id} is at {next_check_dt}, current: {current_dt}")

            return can_send

        except Exception as e:
            logger.error(f"Error checking manifest frequency: {e}")
            return True

    def _update_next_manifest_check(self, da_id: str, licensee_id: str) -> None:
        try:
            licensee_response = self.licensee_table.get_item(Key={'Licensee_ID': licensee_id})

            if 'Item' not in licensee_response:
                logger.warning(f"Licensee {licensee_id} not found, using default frequency")
                manifest_frequency = 1800
            else:
                manifest_frequency = int(licensee_response['Item'].get('Manifest_Frequency', 1800))

            next_check_dt = datetime.now(datetime.now().astimezone().tzinfo) + timedelta(seconds=manifest_frequency)
            next_check = next_check_dt.isoformat().replace('+00:00', 'Z')

            self.da_table.update_item(
                Key={'ID': da_id},
                UpdateExpression='SET Next_Manifest_Check = :next_check',
                ExpressionAttributeValues={':next_check': next_check}
            )

            logger.info(f"Next manifest check for DA {da_id} set to {next_check}")

        except Exception as e:
            logger.error(f"Error updating next manifest check: {e}")

    def _enrich_manifest_with_file_status(self, manifest: Dict, da_id: str) -> Dict:
        enriched_manifest = manifest.copy()
        enriched_assets = []

        tracked_files = self.file_delivery_service.get_files_for_da(da_id)
        # tracked_files items are Dynamo format - FileDeliveryService returns raw Items
        file_status_map = {f.get('Asset_Id') or f.get('AssetId') or f.get('Asset_Id'): f.get('File_Status', 'NEW') for f in tracked_files}

        for asset in manifest.get('assets', []):
            asset_copy = asset.copy()
            asset_id = asset_copy.get('asset_id') or asset_copy.get('Asset_Id') or ''
            file_status = file_status_map.get(asset_id, 'NEW')

            if file_status.upper() in ['NO_CHANGE', 'NO CHANGE']:
                asset_copy['file_status'] = 'No Change'
            elif file_status.upper() in ['REVISED']:
                asset_copy['file_status'] = 'Revised'
            else:
                asset_copy['file_status'] = 'New'

            enriched_assets.append(asset_copy)

        enriched_manifest['assets'] = enriched_assets
        return enriched_manifest
