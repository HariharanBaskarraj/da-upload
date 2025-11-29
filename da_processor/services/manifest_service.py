"""
Manifest Service for Distribution Authorization (DA) payload generation.

This service generates comprehensive delivery manifests containing DA metadata,
asset information, and file delivery details for licensee distribution workflows.
"""
import json
import logging
import boto3
import re
from typing import Dict, List
from django.conf import settings
from da_processor.utils.date_utils import get_current_zulu
from da_processor.services.s3_service import S3Service

logger = logging.getLogger(__name__)


class ManifestService:
    """
    Service for generating distribution manifest payloads for DAs.

    This service:
    - Aggregates DA, title, licensee, and studio information
    - Retrieves and filters assets based on component requirements
    - Validates asset availability in S3 buckets
    - Determines file status (New, Revised, No Change)
    - Generates comprehensive manifest JSON for delivery orchestration
    """

    def __init__(self):
        logger.info("[INIT] Initializing ManifestService")
        self.dynamodb = boto3.client(
            'dynamodb', region_name=settings.AWS_REGION)
        self.s3_service = S3Service()
        self.s3_client = boto3.client('s3', region_name=settings.AWS_REGION)

    # ----------------------------------------------------------------------
    # Public
    # ----------------------------------------------------------------------
    def generate_manifest(self, da_id: str) -> Dict:
        """
        Generate a complete delivery manifest for a Distribution Authorization.

        Orchestrates the manifest generation workflow:
        1. Retrieve DA, title, licensee, and studio information
        2. Query component configurations
        3. Retrieve and filter assets based on components
        4. Validate asset presence in S3
        5. Build final manifest JSON

        Args:
            da_id: Distribution Authorization ID

        Returns:
            Dictionary containing:
                - main_body: DA and title metadata
                - assets: List of asset dictionaries with delivery information

        Raises:
            ValueError: If DA or title not found
            Exception: If manifest generation fails
        """
        logger.info(f"[MANIFEST] Generating manifest for DA ID: {da_id}")

        da_info = self._get_da_info(da_id)
        logger.info(f"[MANIFEST] DA Info retrieved for ID={da_id}")

        title_id = da_info.get('Title_ID')
        version_id = da_info.get('Version_ID')
        licensee_id = da_info.get('Licensee_ID')

        title_info = self._get_title_info(title_id, version_id)
        logger.info(
            f"[MANIFEST] Title Info retrieved for {title_id}/{version_id}")

        licensee_info = self._get_licensee_info(licensee_id)
        logger.info(f"[MANIFEST] Licensee Info retrieved for {licensee_id}")

        studio_id = da_info.get(
            "Internal_Studio_ID") or settings.DEFAULT_STUDIO_ID
        studio_config = self._get_studio_config(studio_id)

        logger.info(
            f"[MANIFEST] Studio Config resolved: {studio_config.get('Studio_Name', 'NBCU')}")

        components = self._get_components_for_da(da_id)
        component_folders = self._get_component_folders(components)
        logger.info(
            f"[MANIFEST] Component folders resolved: {len(component_folders)} entries")

        assets = self._get_assets_for_title_and_components(
            title_id, version_id, component_folders)
        logger.info(f"[MANIFEST] Final filtered assets count: {len(assets)}")

        if not assets:
            logger.warning(
                f"[MANIFEST] No assets available for DA {da_id}, manifest will not be sent")

        manifest = self._build_manifest(
            da_info, title_info, licensee_info, studio_config, assets)
        logger.info(
            f"[MANIFEST] Manifest generated successfully for DA ID: {da_id}")
        return manifest

    # ----------------------------------------------------------------------
    # DynamoDB helpers
    # ----------------------------------------------------------------------
    def _get_da_info(self, da_id: str) -> Dict:
        """
        Retrieve Distribution Authorization record from DynamoDB.

        Args:
            da_id: Distribution Authorization ID

        Returns:
            Deserialized DA record dictionary

        Raises:
            ValueError: If DA not found
        """
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
        """
        Retrieve title information record from DynamoDB.

        Args:
            title_id: Title identifier
            version_id: Version identifier

        Returns:
            Deserialized title record dictionary

        Raises:
            ValueError: If title not found
        """
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
        """
        Retrieve licensee information from DynamoDB.

        Args:
            licensee_id: Licensee identifier

        Returns:
            Deserialized licensee record dictionary

        Raises:
            ValueError: If licensee not found
        """
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
        """
        Retrieve studio configuration from DynamoDB.

        Args:
            studio_id: Studio identifier

        Returns:
            Deserialized studio config dictionary (returns fallback if not found)
        """
        logger.info(f"[STUDIO] Fetching Studio Config for ID={studio_id}")
        response = self.dynamodb.get_item(
            TableName=settings.DYNAMODB_STUDIO_CONFIG_TABLE,
            Key={'Studio_ID': {'S': studio_id}}
        )

        if 'Item' not in response:
            logger.warning(
                f"[STUDIO] No config found for {studio_id}, using fallback")
            return {'Studio_ID': studio_id, 'Studio_Name': 'Unknown Studio'}

        item = self._deserialize_item(response['Item'])
        return item

    # ----------------------------------------------------------------------
    # Component handling
    # ----------------------------------------------------------------------
    def _get_components_for_da(self, da_id: str) -> List[Dict]:
        """
        Retrieve all components for a Distribution Authorization.

        Args:
            da_id: Distribution Authorization ID

        Returns:
            List of deserialized component dictionaries
        """
        logger.info(f"[COMPONENTS] Querying components for DA ID: {da_id}")
        response = self.dynamodb.query(
            TableName=settings.DYNAMODB_COMPONENT_TABLE,
            KeyConditionExpression='ID = :id',
            ExpressionAttributeValues={':id': {'S': da_id}}
        )

        items = response.get('Items', [])
        components = [self._deserialize_item(item) for item in items]
        logger.debug(f"[COMPONENTS] Found components: {components}")
        return components

    def _get_component_folders(self, components: List[Dict]) -> List[str]:
        """
        Extract folder structures for components from configuration table.

        Args:
            components: List of component dictionaries

        Returns:
            List of normalized folder path strings
        """
        folders = []
        for comp in components:
            component_id = comp.get('Component_ID')
            if not component_id:
                logger.debug(
                    "[FOLDERS] component record missing Component_ID, skipping")
                continue

            response = self.dynamodb.scan(
                TableName=settings.DYNAMODB_COMPONENT_CONFIG_TABLE,
                FilterExpression='ComponentId = :comp_id',
                ExpressionAttributeValues={':comp_id': {'S': component_id}}
            )
            logger.debug(
                f"component-config-response: {response} -- {component_id}")

            items = response.get('Items', [])
            if not items:
                logger.warning(
                    f"[FOLDERS] Component config not found for: {component_id}")
                continue

            record = self._deserialize_item(items[0])
            folder = record.get('Folder Structure', '').replace(
                "\\", "/").strip("/")

            if folder:
                folders.append(folder)
            else:
                logger.warning(
                    f"[FOLDERS] Component {component_id} has empty folder configuration")

        return folders
        """
            # ----------------------------------------------------------------------
            # Asset retrieval + filtering
            # ----------------------------------------------------------------------
            def _asset_exists_in_s3(self, filename: str, folder_path: str) -> bool:
        # 
                Check if asset file exists in appropriate S3 bucket.

                Routes .mov files to watermarked bucket, others to asset repository.

                Args:
                    filename: Asset filename
                    folder_path: S3 folder path

                Returns:
                    True if asset exists, False otherwise
        # 
                bucket = (
                    settings.AWS_WATERMARKED_BUCKET
                    if filename.lower().endswith('.mov')
                    else settings.AWS_ASSET_REPO_BUCKET
                )
                if filename.lower().endswith('.mov'):
                    file_name= filename.strip('.mov')
                    wm_filename = f"{file_name}_WM1.mov"
                    s3_key = f"{folder_path}".replace("//", "/")+f"/{wm_filename}"
                else:
                    s3_key = f"{folder_path}".replace("//", "/")+f"/{filename}"
                logger.debug(
                    f"[S3] Checking S3 existence for bucket={bucket}, key={s3_key}")

                try:
                    self.s3_client.head_object(Bucket=bucket, Key=s3_key)
                    return True
                except self.s3_client.exceptions.ClientError as e:
                    code = e.response.get("Error", {}).get("Code", "")
                    if code in ("404", "NotFound"):
                        logger.warning(
                            f"[S3] Asset not found in bucket {bucket} key {s3_key}")
                        return False
                    logger.error(
                        f"[S3] ClientError checking object {s3_key} in bucket {bucket}: {e}")
                    return False
                except Exception as e:
                    logger.error(
                        f"[S3] Unexpected error checking object {s3_key} in bucket {bucket}: {e}")
                    return False
        """
    def _asset_exists_in_s3(self, filename: str, folder_path: str) -> bool:
        bucket = (
            settings.AWS_WATERMARKED_BUCKET
            if filename.lower().endswith('.mov')
            else settings.AWS_ASSET_REPO_BUCKET
        )

        # ---------------------------------------------------------------
        # If MOV → dynamically find the lowest WM version in watermark bucket
        # ---------------------------------------------------------------
        if filename.lower().endswith('.mov'):
            base_name = filename[:-4]  # remove .mov
            prefix = f"{folder_path}/{base_name}_WM".replace("//", "/")

            logger.debug(f"[S3] Searching dynamic WM versions with prefix={prefix}")

            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=settings.AWS_WATERMARKED_BUCKET,
                    Prefix=prefix
                )

                if "Contents" not in response:
                    logger.warning(f"[S3] No WM files found for prefix {prefix}")
                    return False

                # Extract all WM versions
                versions = []
                for obj in response["Contents"]:
                    key = obj["Key"]
                    match = re.search(r"_WM(\d+)\.mov$", key, re.IGNORECASE)
                    if match:
                        versions.append((int(match.group(1)), key))

                if not versions:
                    logger.warning(f"[S3] No valid WM version files found for MOV asset {filename}")
                    return False

                # Pick the lowest WM version
                versions.sort(key=lambda x: x[0])
                lowest_version, lowest_key = versions[0]

                logger.debug(f"[S3] Found WM version {lowest_version} for {filename}: {lowest_key}")

                # Check existence of lowest WM file
                try:
                    self.s3_client.head_object(
                        Bucket=settings.AWS_WATERMARKED_BUCKET,
                        Key=lowest_key
                    )
                    return True
                except self.s3_client.exceptions.ClientError:
                    logger.warning(f"[S3] Lowest WM file not found: {lowest_key}")
                    return False

            except Exception as e:
                logger.error(f"[S3] Error listing WM versions for prefix {prefix}: {e}")
                return False

        # ---------------------------------------------------------------
        # For all non-MOV assets → direct filename check
        # ---------------------------------------------------------------
        s3_key = f"{folder_path}".replace("//", "/")
        logger.debug(f"[S3] Checking non-MOV asset: bucket={bucket}, key={s3_key}")

        try:
            self.s3_client.head_object(Bucket=bucket, Key=s3_key)
            return True
        except self.s3_client.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NotFound"):
                logger.warning(f"[S3] Asset not found in bucket={bucket}, key={s3_key}")
                return False
            logger.error(f"[S3] Unexpected client error: {e}")
            return False
        except Exception as e:
            logger.error(f"[S3] Unexpected error: {e}")
            return False


    def _get_assets_for_title_and_components(self, title_id: str, version_id: str, component_folders: List[str]) -> List[Dict]:
        """
        Retrieve and filter assets for title matching component folder structures.

        Queries assets by title/version and filters based on:
        - Component folder path matching
        - S3 availability verification

        Args:
            title_id: Title identifier
            version_id: Version identifier
            component_folders: List of folder paths from component configs

        Returns:
            List of filtered asset dictionaries with AssetId populated
        """
        logger.info(
            f"[ASSETS] Querying assets for Title={title_id}, Version={version_id}")

        response = self.dynamodb.query(
            TableName=settings.DYNAMODB_ASSET_TABLE,
            IndexName="Title_ID-Version_ID-index",
            KeyConditionExpression="Title_ID = :title_id AND Version_ID = :version_id",
            ExpressionAttributeValues={
                ":title_id": {"S": title_id},
                ":version_id": {"S": version_id},
            },
        )

        logger.debug(
            f"_get_assets_for_title_and_components response: {response}")

        all_assets_raw = response.get("Items", [])
        all_assets = [self._deserialize_item(item) for item in all_assets_raw]

        filtered_assets = []
        prefix_candidates = [
            f"{title_id}.{version_id}/", f"{title_id}_{version_id}/"]

        for asset in all_assets:
            filename = asset.get("Filename", "")
            asset_id_from_table = asset.get("AssetId") or asset.get(
                "Asset_ID") or asset.get("Asset_Id") or ""
            raw_folder_path = asset.get("Folder_Path", "") or ""
            folder_path = raw_folder_path.replace("\\", "/").strip("/")

            logger.debug(
                f"Asset raw folder_path: '{raw_folder_path}' -> normalized '{folder_path}' (filename={filename}, assetId={asset_id_from_table})")

            # normalize prefix removal for matching components
            normalized_for_match = folder_path
            for prefix in prefix_candidates:
                if normalized_for_match.startswith(prefix):
                    normalized_for_match = normalized_for_match[len(prefix):]
                    break

            matched = False
            for comp_folder in component_folders:
                if normalized_for_match.startswith(comp_folder):
                    matched = True
                    break

            if not matched:
                logger.info(
                    f"[ASSETS] REJECT '{filename}': folder '{raw_folder_path}' does not map to components")
                continue

            full_s3_path = raw_folder_path.replace("\\", "/").strip("/")
            logger.debug(f"Checking S3 for full path: {full_s3_path}")
            if not self._asset_exists_in_s3(filename, full_s3_path):
                logger.info(
                    f"[ASSETS] REJECT '{filename}': not present in S3 at {full_s3_path}")
                continue

            # attach the canonical AssetId as found in asset record as AssetId
            asset['AssetId'] = asset_id_from_table
            logger.info(
                f"[ASSETS] ACCEPT '{filename}' (AssetId={asset_id_from_table})")
            filtered_assets.append(asset)

        logger.info(f"[ASSETS] Filtered Assets Count: {len(filtered_assets)}")
        return filtered_assets

    # ----------------------------------------------------------------------
    # Manifest build
    # ----------------------------------------------------------------------
    def _build_manifest(self, da_info: Dict, title_info: Dict, licensee_info: Dict, studio_config: Dict, assets: List[Dict]) -> Dict:
        """
        Build final manifest JSON from aggregated DA data.

        Args:
            da_info: DA record dictionary
            title_info: Title record dictionary
            licensee_info: Licensee record dictionary
            studio_config: Studio configuration dictionary
            assets: List of filtered asset dictionaries

        Returns:
            Complete manifest dictionary with main_body and assets sections
        """
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
        """
        Build asset dict for manifest with correct folder_path and file_path.
        """
        filename = asset.get('Filename', '')
        folder_path_raw = (asset.get('Folder_Path', '') or '').replace("\\", "/")
        version = int(asset.get('Version', 1)) if asset.get('Version') is not None else 1

        asset_id = asset.get('AssetId') or asset.get('Asset_ID') or asset.get('Asset_Id') or ''
        
        if not asset_id:
            logger.error(f"[BUILD_ASSET] Missing AssetId for asset record: {asset}")
        else:
            logger.debug(f"[BUILD_ASSET] Using AssetId={asset_id} for filename={filename}")

        checksum = asset.get('Checksum', '')

        # CRITICAL FIX: Remove filename from folder_path if present
        folder_path = folder_path_raw.strip("/")
        
        # If folder_path ends with the filename, remove it
        if folder_path.endswith(f"/{filename}"):
            folder_path = folder_path[:-len(f"/{filename}")]
            logger.debug(f"[BUILD_ASSET] Removed filename from folder_path: {folder_path_raw} -> {folder_path}")
        elif folder_path.endswith(filename) and not folder_path.endswith(f"/{filename}"):
            # Handle case without slash
            folder_path = folder_path[:-len(filename)].rstrip('/')
            logger.debug(f"[BUILD_ASSET] Removed filename (no slash) from folder_path: {folder_path_raw} -> {folder_path}")

        # Compute file size from S3
        file_size_mb = self._get_file_size_from_s3(filename, asset)

        # Construct proper file_path
        file_path = f"{folder_path}/{filename}"

        return {
            "asset_id": asset_id,
            "file_status": self._determine_file_status(asset_id, version),  # ← Use version, not checksum
            "file_name": filename,
            "folder_path": folder_path,  # ← Just the folder
            "file_path": file_path,      # ← Folder + filename
            "checksum": checksum,
            "file_size_mb": file_size_mb,
            "studio_asset_id": asset.get('Studio_Asset_ID', ''),
            "studio_revision_number": asset.get('Studio_Revision_Number', ''),
            "studio_revision_notes": asset.get('Studio_Revision_Notes', ''),
            "studio_revision_urgency": asset.get('Studio_Revision_Urgency', ''),
            "revision_id": version,
        }

    def _determine_file_status(self, asset_id: str, current_version: int) -> str:
        """
        Determine file delivery status by comparing versions and respecting existing status.
        """
        try:
            if not asset_id:
                logger.debug("[FILE_STATUS] Empty asset_id -> treat as New")
                return "New"

            response = self.dynamodb.scan(
                TableName=settings.DYNAMODB_FILE_DELIVERY_TABLE,
                FilterExpression='Asset_Id = :asset_id',
                ExpressionAttributeValues={':asset_id': {'S': asset_id}}
            )

            items = response.get('Items', [])

            if not items:
                logger.debug(f"[FILE_STATUS] No tracker record for asset {asset_id} -> New")
                return "New"

            existing_item = self._deserialize_item(items[0])
            existing_status = existing_item.get('File_Status', 'NEW')
            existing_version = int(existing_item.get('Version', 1))

            logger.debug(
                f"[FILE_STATUS] Tracker record found: Status={existing_status}, "
                f"Version={existing_version}, Current Version={current_version}"
            )

            # If status is still "NEW", keep it "NEW"
            if existing_status.upper() == 'NEW':
                logger.debug(f"[FILE_STATUS] Status is NEW, keeping as New")
                return "New"

            # Compare versions
            if current_version > existing_version:
                logger.debug(f"[FILE_STATUS] Version increased {existing_version} -> {current_version} -> Revised")
                return "Revised"
            else:
                logger.debug(f"[FILE_STATUS] Version unchanged ({current_version}) -> No Change")
                return "No Change"

        except Exception as e:
            logger.warning(f"Could not determine file status for asset {asset_id}: {e}")
            return "New"
        
    def _get_file_size_from_s3(self, filename: str, asset: dict) -> float:
        """
        Retrieve file size from S3 using asset folder path.

        Args:
            filename: Asset filename
            asset: Asset dictionary containing Folder_Path

        Returns:
            File size in megabytes (MB), or 0.0 if unavailable
        """
        try:
            bucket = settings.AWS_WATERMARKED_BUCKET if filename.lower().endswith(
                ".mov") else settings.AWS_ASSET_REPO_BUCKET
            s3_key = (asset.get("Folder_Path", "")
                      or "").replace("\\", "/").strip("/")

            if not s3_key:
                logger.warning(
                    f"[S3_SIZE] Missing Folder_Path for asset {asset.get('AssetId', '')} (filename={filename})")
                return 0.0

            logger.debug(
                f"[S3_SIZE] Checking S3 size for bucket={bucket}, key={s3_key}")
            response = self.s3_client.head_object(Bucket=bucket, Key=s3_key)
            size_bytes = response.get("ContentLength", 0)
            size_mb = round(size_bytes / (1024 * 1024), 2)
            return size_mb
        except Exception as e:
            logger.warning(
                f"[S3_SIZE] Unable to get size for '{filename}' ({asset.get('Folder_Path', '')}): {e}")
            return 0.0

    # ----------------------------------------------------------------------
    # DynamoDB deserializer
    # ----------------------------------------------------------------------
    def _deserialize_item(self, item: Dict) -> Dict:
        """
        Deserialize DynamoDB item format to standard Python dictionary.

        Args:
            item: DynamoDB item with typed values (e.g., {'S': '...', 'N': '...'})

        Returns:
            Flattened dictionary with plain values
        """
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
