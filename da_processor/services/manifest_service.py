import json
import logging
import boto3
from typing import Dict, List
from django.conf import settings
from da_processor.utils.date_utils import get_current_zulu
from da_processor.services.s3_service import S3Service

logger = logging.getLogger(__name__)


class ManifestService:
    """
    ManifestService
    - Generates a manifest JSON for a given DA ID.
    - Reads DA/title/licensee/components/assets from DynamoDB.
    - Filters assets by component folder mapping and S3 existence.
    - Returns manifest dict with main_body + assets list.
    """

    def __init__(self):
        logger.info("[INIT] Initializing ManifestService")
        self.dynamodb = boto3.client('dynamodb', region_name=settings.AWS_REGION)
        self.s3_service = S3Service()
        self.s3_client = boto3.client('s3', region_name=settings.AWS_REGION)

    # ----------------------------------------------------------------------
    # Public
    # ----------------------------------------------------------------------
    def generate_manifest(self, da_id: str) -> Dict:
        logger.info(f"[MANIFEST] Generating manifest for DA ID: {da_id}")

        # Fetch base metadata
        da_info = self._get_da_info(da_id)
        logger.info(f"[MANIFEST] DA Info retrieved for ID={da_id}")

        title_id = da_info.get('Title_ID')
        version_id = da_info.get('Version_ID')
        licensee_id = da_info.get('Licensee_ID')

        title_info = self._get_title_info(title_id, version_id)
        logger.info(f"[MANIFEST] Title Info retrieved for {title_id}/{version_id}")

        licensee_info = self._get_licensee_info(licensee_id)
        logger.info(f"[MANIFEST] Licensee Info retrieved for {licensee_id}")

        studio_config = self._get_studio_config(da_info.get('Internal_Studio_ID', settings.DEFAULT_STUDIO_ID))
        logger.info(f"[MANIFEST] Studio Config resolved: {studio_config.get('Studio_Name', 'Unknown')}")

        # Components -> component folders
        components = self._get_components_for_da(da_id)
        component_folders = self._get_component_folders(components)
        logger.info(f"[MANIFEST] Component folders resolved: {len(component_folders)} entries")

        # Assets (filtering by folder + S3)
        assets = self._get_assets_for_title_and_components(title_id, version_id, component_folders)
        logger.info(f"[MANIFEST] Final filtered assets count: {len(assets)}")

        # Build manifest
        manifest = self._build_manifest(da_info, title_info, licensee_info, studio_config, assets)
        logger.info(f"[MANIFEST] Manifest generated successfully for DA ID: {da_id}")
        return manifest

    # ----------------------------------------------------------------------
    # DynamoDB helpers
    # ----------------------------------------------------------------------
    def _get_da_info(self, da_id: str) -> Dict:
        logger.info(f"[DA] Fetching DA Info for ID={da_id}")
        response = self.dynamodb.get_item(
            TableName=settings.DYNAMODB_DA_TABLE,
            Key={'ID': {'S': da_id}}
        )

        if 'Item' not in response:
            logger.warning(f"[DA] DA not found: {da_id}")
            raise ValueError(f"DA not found: {da_id}")

        item = self._deserialize_item(response['Item'])
        return item

    def _get_title_info(self, title_id: str, version_id: str) -> Dict:
        logger.info(f"[TITLE] Fetching Title Info for {title_id}/{version_id}")
        response = self.dynamodb.get_item(
            TableName=settings.DYNAMODB_TITLE_TABLE,
            Key={
                'Title_ID': {'S': title_id},
                'Version_ID': {'S': version_id}
            }
        )

        if 'Item' not in response:
            logger.warning(f"[TITLE] Title not found: {title_id}/{version_id}")
            raise ValueError(f"Title not found: {title_id}/{version_id}")

        item = self._deserialize_item(response['Item'])
        return item

    def _get_licensee_info(self, licensee_id: str) -> Dict:
        logger.info(f"[LICENSEE] Fetching Licensee Info for {licensee_id}")
        response = self.dynamodb.get_item(
            TableName=settings.DYNAMODB_LICENSEE_TABLE,
            Key={'Licensee_ID': {'S': licensee_id}}
        )

        if 'Item' not in response:
            logger.warning(f"[LICENSEE] Licensee not found: {licensee_id}")
            raise ValueError(f"Licensee not found: {licensee_id}")

        item = self._deserialize_item(response['Item'])
        return item

    def _get_studio_config(self, studio_id: str) -> Dict:
        logger.info(f"[STUDIO] Fetching Studio Config for ID={studio_id}")
        response = self.dynamodb.get_item(
            TableName=settings.DYNAMODB_STUDIO_CONFIG_TABLE,
            Key={'Studio_ID': {'S': studio_id}}
        )

        if 'Item' not in response:
            logger.warning(f"[STUDIO] No config found for {studio_id}, using fallback")
            return {'Studio_ID': studio_id, 'Studio_Name': 'Unknown Studio'}

        item = self._deserialize_item(response['Item'])
        return item

    # ----------------------------------------------------------------------
    # Component handling
    # ----------------------------------------------------------------------
    def _get_components_for_da(self, da_id: str) -> List[Dict]:
        logger.info(f"[COMPONENTS] Querying components for DA ID: {da_id}")
        response = self.dynamodb.query(
            TableName=settings.DYNAMODB_COMPONENT_TABLE,
            KeyConditionExpression='ID = :id',
            ExpressionAttributeValues={':id': {'S': da_id}}
        )

        items = response.get('Items', [])
        components = [self._deserialize_item(item) for item in items]
        logger.info(f"[COMPONENTS] Found {len(components)} components for DA: {da_id}")
        return components

    def _get_component_folders(self, components: List[Dict]) -> List[str]:
        """
        Returns a list of folder strings (normalized, without leading/trailing slashes)
        Example result: ["Video/DV HDR", "Video/UHD SDR"]
        """
        folders = []
        for comp in components:
            component_id = comp.get('Component_ID')
            if not component_id:
                continue

            response = self.dynamodb.scan(
                TableName=settings.DYNAMODB_COMPONENT_CONFIG_TABLE,
                FilterExpression='ComponentId = :comp_id',
                ExpressionAttributeValues={':comp_id': {'S': component_id}}
            )

            items = response.get('Items', [])
            if not items:
                logger.warning(f"[FOLDERS] Component config not found for: {component_id}")
                continue

            record = self._deserialize_item(items[0])
            folder = record.get('Folder Structure', '').replace("\\", "/").strip("/")

            if folder:
                folders.append(folder)
            else:
                logger.warning(f"[FOLDERS] Component {component_id} has empty folder configuration")

        return folders

    # ----------------------------------------------------------------------
    # Asset retrieval + filtering
    # ----------------------------------------------------------------------
    def _asset_exists_in_s3(self, filename: str, folder_path: str) -> bool:
        bucket = (
            settings.AWS_WATERMARKED_BUCKET
            if filename.lower().endswith('.mov')
            else settings.AWS_ASSET_REPO_BUCKET
        )

        # Build the real S3 key
        s3_key = f"{folder_path}/{filename}".replace("//", "/")

        logger.info(f"[S3] Checking S3 for bucket={bucket}, key={s3_key}")

        try:
            self.s3_client.head_object(Bucket=bucket, Key=s3_key)
            return True
        except self.s3_client.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NotFound"):
                logger.warning(f"[S3] Asset not found in bucket {bucket} key {s3_key}")
                return False
            logger.error(f"[S3] ClientError checking object {s3_key} in bucket {bucket}: {e}")
            return False
        except Exception as e:
            logger.error(f"[S3] Unexpected error checking object {s3_key} in bucket {bucket}: {e}")
            return False


    def _get_assets_for_title_and_components(self, title_id: str, version_id: str, component_folders: List[str]) -> List[Dict]:
        logger.info(f"[ASSETS] Querying assets for Title={title_id}, Version={version_id}")

        response = self.dynamodb.query(
            TableName=settings.DYNAMODB_ASSET_TABLE,
            IndexName="Title_ID-Version_ID-index",
            KeyConditionExpression="Title_ID = :title_id AND Version_ID = :version_id",
            ExpressionAttributeValues={
                ":title_id": {"S": title_id},
                ":version_id": {"S": version_id},
            },
        )

        all_assets_raw = response.get("Items", [])
        logger.info(f"[ASSETS] DynamoDB returned {len(all_assets_raw)} assets")

        all_assets = [self._deserialize_item(item) for item in all_assets_raw]
        filtered_assets = []

        # Prepare prefix(s) we expect and will remove for matching
        prefix_candidates = [f"{title_id}.{version_id}/", f"{title_id}_{version_id}/"]

        for asset in all_assets:
            filename = asset.get("Filename", "")
            asset_id = asset.get("AssetId", "")
            raw_folder_path = asset.get("Folder_Path", "") or ""
            folder_path = raw_folder_path.replace("\\", "/").strip("/")

            # Normalize by stripping known Title.Version prefix if present
            for prefix in prefix_candidates:
                if folder_path.startswith(prefix):
                    folder_path = folder_path[len(prefix):]
                    break

            # Determine if folder_path matches any configured component folders
            matched = False
            for comp_folder in component_folders:
                if folder_path.startswith(comp_folder):
                    matched = True
                    break

            if not matched:
                # Asset doesn't belong to any requested component folder
                logger.info(f"[ASSETS] REJECT '{filename}': folder '{raw_folder_path}' does not map to components")
                continue

            # Check S3 - use raw_folder_path as-is (it already contains Title.Version prefix)
            # Just clean up any backslashes and extra slashes
            full_s3_path = raw_folder_path.replace("\\", "/").strip("/")
            if not self._asset_exists_in_s3(filename, full_s3_path):
                logger.info(f"[ASSETS] REJECT '{filename}': not present in S3")
                continue

            # Accept asset
            logger.info(f"[ASSETS] ACCEPT '{filename}'")
            filtered_assets.append(asset)

        logger.info(f"[ASSETS] Filtered Assets Count: {len(filtered_assets)}")
        return filtered_assets

    # ----------------------------------------------------------------------
    # Manifest build
    # ----------------------------------------------------------------------
    def _build_manifest(self, da_info: Dict, title_info: Dict, licensee_info: Dict, studio_config: Dict, assets: List[Dict]) -> Dict:
        manifest = {
            "main_body": {
                "distribution_authorization_id": da_info.get('ID', ''),
                "payload_creation": get_current_zulu(),
                "studio_id": studio_config.get('Studio_ID', ''),
                "studio_name": studio_config.get('Studio_Name', ''),
                "licensee_id": da_info.get('Licensee_ID', ''),
                "licensee_name": licensee_info.get('Licensee_Name', ''),
                "da_description": da_info.get('DA_Description', ''),
                "due_date": da_info.get('Due_Date', ''),
                "earliest_delivery_date": da_info.get('Earliest_Delivery_Date', ''),
                "delivery_end_date": da_info.get('License_Period_End', ''),
                "title_id": title_info.get('Title_ID', ''),
                "title_name": title_info.get('Title_Name', ''),
                "title_eidr_id": title_info.get('Title_EIDR_ID', ''),
                "version_id": title_info.get('Version_ID', ''),
                "version_name": title_info.get('Version_Name', ''),
                "version_eidr_id": title_info.get('Version_EIDR_ID', ''),
                "release_year": int(title_info.get('Release_Year', 0)) if title_info.get('Release_Year') else None
            },
            "assets": []
        }

        for asset in assets:
            manifest["assets"].append(self._build_asset_data(asset))

        return manifest

    def _build_asset_data(self, asset: Dict) -> Dict:
        filename = asset.get('Filename', '')
        folder_path = asset.get('Folder_Path', '').replace("\\", "/") or ""
        version = int(asset.get('Version', 1)) if asset.get('Version') is not None else 1
        file_status = "New" if version == 1 else "Revised"
        asset_id = asset.get('AssetId', '')

        file_size_mb = self._get_file_size_from_s3(filename, asset_id)

        return {
            "file_status": file_status,
            "file_name": filename,
            "file_path": f"{folder_path}{filename}",
            "checksum": asset.get('Checksum', ''),
            "file_size_mb": file_size_mb,
            "studio_revision_number": asset.get('Studio_Revision_Number', ''),
            "studio_revision_notes": asset.get('Studio_Revision_Notes', ''),
            "studio_revision_urgency": asset.get('Studio_Revision_Urgency', ''),
            "revision_id": version,
        }

    def _get_file_size_from_s3(self, filename: str, asset_id: str) -> float:
        bucket = settings.AWS_WATERMARKED_BUCKET if filename.lower().endswith(".mov") else settings.AWS_ASSET_REPO_BUCKET
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=asset_id)
            size_bytes = response.get("ContentLength", 0)
            size_mb = round(size_bytes / (1024 * 1024), 2)
            return size_mb
        except Exception as e:
            logger.warning(f"[S3_SIZE] Unable to get size for '{filename}' ({asset_id}): {e}")
            return 0.0

    # ----------------------------------------------------------------------
    # DynamoDB deserializer
    # ----------------------------------------------------------------------
    def _deserialize_item(self, item: Dict) -> Dict:
        parsed = {}
        for key, value in item.items():
            if 'S' in value:
                parsed[key] = value['S']
            elif 'N' in value:
                parsed[key] = value['N']
            elif 'SS' in value:
                parsed[key] = value['SS']
            elif 'BOOL' in value:
                parsed[key] = value['BOOL']
        return parsed