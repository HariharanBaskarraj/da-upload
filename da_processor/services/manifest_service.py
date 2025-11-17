import json
import logging
import boto3
from typing import Dict, List
from django.conf import settings
from da_processor.utils.date_utils import get_current_zulu
from da_processor.services.s3_service import S3Service

logger = logging.getLogger(__name__)


class ManifestService:
    
    def __init__(self):
        self.dynamodb = boto3.client('dynamodb', region_name=settings.AWS_REGION)
        self.s3_service = S3Service()
        self.s3_client = boto3.client('s3', region_name=settings.AWS_REGION)
    
    def generate_manifest(self, da_id: str) -> Dict:
        logger.info(f"Generating manifest for DA ID: {da_id}")
        
        da_info = self._get_da_info(da_id)
        title_id = da_info['Title_ID']
        version_id = da_info['Version_ID']
        licensee_id = da_info['Licensee_ID']
        
        title_info = self._get_title_info(title_id, version_id)
        licensee_info = self._get_licensee_info(licensee_id)
        studio_config = self._get_studio_config(da_info.get('Internal_Studio_ID', settings.DEFAULT_STUDIO_ID))
        assets = self._get_assets_for_title(title_id, version_id)
        
        manifest = self._build_manifest(da_info, title_info, licensee_info, studio_config, assets)
        
        logger.info(f"Manifest generated successfully for DA ID: {da_id}")
        return manifest
    
    def _get_da_info(self, da_id: str) -> Dict:
        response = self.dynamodb.get_item(
            TableName=settings.DYNAMODB_DA_TABLE,
            Key={'ID': {'S': da_id}}
        )
        
        if 'Item' not in response:
            raise ValueError(f"DA not found: {da_id}")
        
        return self._deserialize_item(response['Item'])
    
    def _get_title_info(self, title_id: str, version_id: str) -> Dict:
        response = self.dynamodb.get_item(
            TableName=settings.DYNAMODB_TITLE_TABLE,
            Key={
                'Title_ID': {'S': title_id},
                'Version_ID': {'S': version_id}
            }
        )
        
        if 'Item' not in response:
            raise ValueError(f"Title not found: {title_id}/{version_id}")
        
        return self._deserialize_item(response['Item'])
    
    def _get_licensee_info(self, licensee_id: str) -> Dict:
        response = self.dynamodb.get_item(
            TableName=settings.DYNAMODB_LICENSEE_TABLE,
            Key={'Licensee_ID': {'S': licensee_id}}
        )
        
        if 'Item' not in response:
            raise ValueError(f"Licensee not found: {licensee_id}")
        
        return self._deserialize_item(response['Item'])
    
    def _get_studio_config(self, studio_id: str) -> Dict:
        response = self.dynamodb.get_item(
            TableName=settings.DYNAMODB_STUDIO_CONFIG_TABLE,
            Key={'Studio_ID': {'S': studio_id}}
        )
        
        if 'Item' not in response:
            logger.warning(f"Studio config not found for: {studio_id}, using defaults")
            return {
                'Studio_ID': studio_id,
                'Studio_Name': 'Unknown Studio'
            }
        
        return self._deserialize_item(response['Item'])
    
    def _get_assets_for_title(self, title_id: str, version_id: str) -> List[Dict]:
        response = self.dynamodb.query(
            TableName=settings.DYNAMODB_ASSET_TABLE,
            IndexName='Title_ID-Version_ID-index',
            KeyConditionExpression='Title_ID = :title_id AND Version_ID = :version_id',
            ExpressionAttributeValues={
                ':title_id': {'S': title_id},
                ':version_id': {'S': version_id}
            }
        )
        
        assets = []
        for item in response.get('Items', []):
            assets.append(self._deserialize_item(item))
        
        return assets
    
    def _build_manifest(self, da_info: Dict, title_info: Dict, licensee_info: Dict, 
                       studio_config: Dict, assets: List[Dict]) -> Dict:
        
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
            asset_data = self._build_asset_data(asset)
            manifest["assets"].append(asset_data)
        
        return manifest
    
    def _build_asset_data(self, asset: Dict) -> Dict:
        version = int(asset.get('Version', 1))
        file_status = 'New' if version == 1 else 'Revised'
        
        folder_path = asset.get('Folder_Path', '')
        filename = asset.get('Filename', '')
        file_path = f"{folder_path}{filename}".replace('\\', '/')
        
        file_size_mb = self._get_file_size_from_s3(filename, asset.get('AssetId', ''))
        
        return {
            "file_status": file_status,
            "file_name": filename,
            "file_path": file_path,
            "checksum": asset.get('Checksum', ''),
            "file_size_mb": file_size_mb,
            "studio_revision_number": asset.get('Studio_Revision_Number', ''),
            "studio_revision_notes": asset.get('Studio_Revision_Notes', ''),
            "studio_revision_urgency": asset.get('Studio_Revision_Urgency', ''),
            "revision_id": version
        }
    
    def _get_file_size_from_s3(self, filename: str, asset_id: str) -> float:
        try:
            if filename.lower().endswith('.mov'):
                bucket = settings.AWS_WATERMARKED_BUCKET
            else:
                bucket = settings.AWS_ASSET_REPO_BUCKET
            
            response = self.s3_client.head_object(Bucket=bucket, Key=asset_id)
            size_bytes = response['ContentLength']
            size_mb = round(size_bytes / (1024 * 1024), 2)
            
            return size_mb
        except Exception as e:
            logger.error(f"Error getting file size for {filename}: {e}")
            return 0.0
    
    def _deserialize_item(self, item: Dict) -> Dict:
        result = {}
        for key, value in item.items():
            if 'S' in value:
                result[key] = value['S']
            elif 'N' in value:
                result[key] = value['N']
            elif 'SS' in value:
                result[key] = value['SS']
            elif 'BOOL' in value:
                result[key] = value['BOOL']
        return result