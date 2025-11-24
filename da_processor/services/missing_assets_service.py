import logging
import boto3
from typing import Dict, List, Optional
from django.conf import settings
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class MissingAssetsService:

    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name=settings.AWS_REGION)
        self.dynamodb_client = boto3.client('dynamodb', region_name=settings.AWS_REGION)
        self.s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
        
        self.da_table = self.dynamodb.Table(settings.DYNAMODB_DA_TABLE)
        self.title_table = self.dynamodb.Table(settings.DYNAMODB_TITLE_TABLE)
        self.component_table = self.dynamodb.Table(settings.DYNAMODB_COMPONENT_TABLE)
        self.asset_table = self.dynamodb.Table(settings.DYNAMODB_ASSET_TABLE)

    def check_missing_assets_for_da(self, da_id: str) -> Dict:
        try:
            logger.info(f"[MISSING_ASSETS] Checking missing assets for DA: {da_id}")
            
            da_info = self._get_da_info(da_id)
            if not da_info:
                raise ValueError(f"DA not found: {da_id}")
            
            title_id = da_info.get('Title_ID')
            version_id = da_info.get('Version_ID')
            
            title_info = self._get_title_info(title_id, version_id)
            
            components = self._get_components_for_da(da_id)
            
            all_missing_components = []

            logger.info(f"components: {components}")
            
            for component in components:
                component_id = component.get('Component_ID')
                required_flag = component.get('Required_Flag', 'FALSE')
                
                if required_flag.upper() != 'TRUE':
                    logger.info(f"[MISSING_ASSETS] Skipping non-required component: {component_id}")
                    continue
                
                logger.info(f"[MISSING_ASSETS] Checking required component: {component_id}")
                
                missing_assets = self._check_component_assets(
                    title_id, version_id, component_id
                )

                logger.info(f"missing_assets: {missing_assets}")
                
                if missing_assets:
                    all_missing_components.append({
                        'component_id': component_id,
                        'missing_assets': missing_assets
                    })
                    logger.warning(
                        f"[MISSING_ASSETS] Component {component_id} has {len(missing_assets)} missing assets"
                    )
            logger.info(f"Check for the result")

            result = {
                'da_id': da_id,
                'title_id': title_id,
                'title_name': title_info.get('Title_Name', ''),
                'version_id': version_id,
                'version_name': title_info.get('Version_Name', ''),
                'licensee_id': da_info.get('Licensee_ID', ''),
                'exception_recipients': da_info.get('Exception_Recipients', ''),
                'has_missing_assets': len(all_missing_components) > 0,
                'missing_components': all_missing_components,
                'total_missing_count': sum(len(c['missing_assets']) for c in all_missing_components)
            }

            logger.info(f"Result of missing asset info {result}")
            
            logger.info(
                f"[MISSING_ASSETS] DA {da_id}: {result['total_missing_count']} missing assets "
                f"across {len(all_missing_components)} components"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"[MISSING_ASSETS] Error checking missing assets for DA {da_id}: {e}", exc_info=True)
            raise

    def _check_component_assets(self, title_id: str, version_id: str, component_id: str) -> List[Dict]:
        try:
            folder_structure = self._get_component_folder_structure(component_id)
            logger.info(f"Expected_Assets: {folder_structure}")
            if not folder_structure:
                logger.warning(f"[MISSING_ASSETS] No folder structure found for component: {component_id}")
                return []
            
            expected_assets = self._get_expected_assets_for_component(
                title_id, version_id, folder_structure
            )

            logger.info(f"Expected_Assets: {expected_assets}")
            
            if not expected_assets:
                logger.info(f"[MISSING_ASSETS] No expected assets for component: {component_id}")
                return []
            
            missing_assets = []
            
            for asset in expected_assets:
                asset_id = asset.get('Asset_ID', '')
                filename = asset.get('Filename', '')
                folder_path = asset.get('Folder_Path', '').replace('\\', '/').strip('/')
                
                exists = self._check_asset_in_s3(filename, folder_path)
                logger.info(f"inside exists: {exists}")
                if not exists:
                    missing_assets.append({
                        'asset_id': asset_id,
                        'filename': filename,
                        'folder_path': folder_path,
                        'full_path': f"{folder_path}/{filename}" if folder_path else filename
                    })
                    logger.warning(f"[MISSING_ASSETS] Missing asset: {filename}")
            

            logger.info(f"inside missing_assets: {missing_assets}")
            return missing_assets
            
        except Exception as e:
            logger.error(f"[MISSING_ASSETS] Error checking component assets: {e}", exc_info=True)
            return []

    def _check_asset_in_s3(self, filename: str, folder_path: str) -> bool:
        if filename.lower().endswith('.mov'):
            bucket = settings.AWS_WATERMARKED_BUCKET
        else:
            bucket = settings.AWS_ASSET_REPO_BUCKET
        
        s3_key = f"{folder_path}/{filename}".replace('//', '/')
        
        logger.debug(f"[MISSING_ASSETS] Checking S3: bucket={bucket}, key={s3_key}")
        
        try:
            self.s3_client.head_object(Bucket=bucket, Key=s3_key)
            return True
        except self.s3_client.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ('404', 'NotFound'):
                return False
            logger.error(f"[MISSING_ASSETS] S3 error for {s3_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"[MISSING_ASSETS] Unexpected error checking S3 for {s3_key}: {e}")
            return False

    def _get_component_folder_structure(self, component_id: str) -> Optional[str]:
        try:
            response = self.dynamodb_client.scan(
                TableName=settings.DYNAMODB_COMPONENT_CONFIG_TABLE,
                FilterExpression='ComponentId = :comp_id',
                ExpressionAttributeValues={':comp_id': {'S': component_id}}
            )
            
            items = response.get('Items', [])
            if not items:
                return None
            
            folder_structure = items[0].get('Folder Structure', {}).get('S', '')
            return folder_structure.replace('\\', '/').strip('/')
            
        except Exception as e:
            logger.error(f"[MISSING_ASSETS] Error getting folder structure: {e}")
            return None

    def _get_expected_assets_for_component(
        self, title_id: str, version_id: str, folder_structure: str
    ) -> List[Dict]:
        try:
            response = self.asset_table.query(
                IndexName='Title_ID-Version_ID-index',
                KeyConditionExpression='Title_ID = :title_id AND Version_ID = :version_id',
                ExpressionAttributeValues={
                    ':title_id': title_id,
                    ':version_id': version_id
                }
            )
            logger.info(f"Response of the AssetTable: {response}")
            
            all_assets = response.get('Items', [])
            
            logger.info(f"All_assets: {all_assets}")
            matching_assets = []
            prefix_candidates = [f"{title_id}.{version_id}/", f"{title_id}_{version_id}/"]
            
            logger.info(f"prefix_candidates: {prefix_candidates}")
            for asset in all_assets:
                folder_path = asset.get('Folder_Path', '').replace('\\', '/').strip('/')
                logger.info(f"folder_path: {folder_path}")
                for prefix in prefix_candidates:
                    if folder_path.startswith(prefix):
                        folder_path = folder_path[len(prefix):]
                        logger.info(f"inside startwith prefix folder_path: {folder_path}")
                        break
                
                if folder_path.startswith(folder_structure):
                    logger.info(f"inside startwith folder_structure folder_path: {folder_path} --{folder_structure}")
                    matching_assets.append(asset)
                

            logger.info(f"inside smatching_assets: {matching_assets} --{folder_structure}")
            return matching_assets
            
        except Exception as e:
            logger.error(f"[MISSING_ASSETS] Error getting expected assets: {e}")
            return []

    def _get_da_info(self, da_id: str) -> Optional[Dict]:
        try:
            response = self.da_table.get_item(Key={'ID': da_id})
            return response.get('Item')
        except Exception as e:
            logger.error(f"[MISSING_ASSETS] Error getting DA info: {e}")
            return None

    def _get_title_info(self, title_id: str, version_id: str) -> Dict:
        try:
            response = self.title_table.get_item(
                Key={'Title_ID': title_id, 'Version_ID': version_id}
            )
            return response.get('Item', {})
        except Exception as e:
            logger.error(f"[MISSING_ASSETS] Error getting title info: {e}")
            return {}

    def _get_components_for_da(self, da_id: str) -> List[Dict]:
        try:
            response = self.component_table.query(
                KeyConditionExpression='ID = :id',
                ExpressionAttributeValues={':id': da_id}
            )
            return response.get('Items', [])
        except Exception as e:
            logger.error(f"[MISSING_ASSETS] Error getting components: {e}")
            return []