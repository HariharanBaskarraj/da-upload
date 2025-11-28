import json
import re
from uuid import uuid4
import boto3
import logging
import requests
from typing import Optional
from django.conf import settings
from botocore.exceptions import ClientError
from da_processor.services.s3_service import S3Service
from da_processor.services.dynamodb_service import DynamoDBService

from datetime import datetime

logger = logging.getLogger(__name__)


class WatermarkCacheService:

    def __init__(self):
        self.s3_service = S3Service()
        self.s3 = boto3.client("s3")
        self.dynamo_service = DynamoDBService()
        self.api_url = settings.WATERMARKING_API_URL
        self.bearer_token = settings.WATERMARKING_API_BEARER_TOKEN
        self.headers = {
            'Authorization': f'Bearer {self.bearer_token}',
            'Content-Type': 'application/json',
        }
        

    def get_next_watermark_version(self, bucket: str, folder_prefix: str, base_filename: str) -> int:
        """
        Scans S3 folder and finds highest WM index, returns next index.

        Example:
        - Existing: video_WM1.mov, video_WM2.mov, video_WM3.mov
        - Returns: 4
        """

        logger.info(f"get_next_Version_Executes: ")
        response = self.s3.list_objects_v2(
            Bucket=bucket,
            Prefix=folder_prefix
        )

        max_index = 0
        pattern = re.compile(r"_WM(\d+)\.mov$", re.IGNORECASE)

        for obj in response.get("Contents", []):
            key = obj["Key"]
            match = pattern.search(key)
            if match:
                idx = int(match.group(1))
                max_index = max(max_index, idx)

        return max_index + 1

    def generate_next_watermark(self, bucket: str, source_key: str, preset_id: str):
        """
        Creates ONE new watermark job with next WM index.
        """
        logger.info(f"Generate_next_watermark_file executes")
        # Parse names
        filename = source_key.split("/")[-1]                 # FirstLook.mov
        base_filename = filename.rsplit(".", 1)[0]           # FirstLook
        
        folder_prefix = "/".join(source_key.split("/")[:-1]) # path/without/file

        logger.info(f"watermark file folder_prefix: {folder_prefix}")
        
        next_index = self.get_next_watermark_version(bucket, folder_prefix, base_filename)

        logger.info(f"next index for watermark: {next_index}")

        new_wm_filename = f"{base_filename}_WM{next_index}.mov"
        dest_key = f"{folder_prefix}/{new_wm_filename}"

        logger.info(f"Creating new dynamic watermark: {dest_key}")

        # Reuse your existing create_watermark_job()
        self.create_watermark_job(
            bucket,
            source_key,
            wm_type=f"WM{next_index}",
            preset_id=preset_id
        )

        logger.info(f"watermark created: {new_wm_filename}")

        return new_wm_filename


    def create_watermark_job(self, source_bucket, source_key, wm_type, preset_id):
        """Create watermark job and call API"""
        try:
            job_id = str(uuid4())
            
            # ============ NEW LOGIC ============
            # Extract file path components
            # source_key: "uploads/video.mov"
            # Split into directory and filename
            
            if '/' in source_key:
                directory = '/'.join(source_key.split('/')[:-1])  # "uploads"
                filename_with_ext = source_key.split('/')[-1]     # "video.mov"
            else:
                directory = ''
                filename_with_ext = source_key
            
            # Split filename and extension
            if '.' in filename_with_ext:
                filename, extension = filename_with_ext.rsplit('.', 1)  # "video", "mov"
            else:
                filename = filename_with_ext
                extension = 'mov'
            
            # Create new filename with watermark type appended
            watermarked_filename = f"{filename}_{wm_type}.{extension}"  # "video_WM1.mov"
            
            # Build output key with SAME folder structure
            if directory:
                output_key = f"{directory}/{watermarked_filename}"  # "uploads/video_WM1.mov"
            else:
                output_key = watermarked_filename
            
            # Both input and output URIs use WATERMARK_CACHE_BUCKET
            input_uri = f"s3://{source_bucket}/{source_key}"  # Original source
            output_uri = f"s3://{settings.AWS_WATERMARKED_BUCKET}/{output_key}"  # Watermarked output
            
            logger.info(f"Input URI: {input_uri}")
            logger.info(f"Output URI: {output_uri}")
            # ============ END NEW LOGIC ============
            

            #To create the watermark entry in the dynamo table

            job_record = {
            "job_id": job_id,
            "source_bucket": source_bucket,
            "source_key": source_key,
            "watermark_type": wm_type,
            "status": "created",
            "created_at": datetime.utcnow().isoformat(),
            "preset_id": preset_id,
            "output_key": output_key,
            "output_uri": output_uri
            }

            self.dynamo_service.create_job(job_record)
            logger.info(f"DynamoDB: Created job record {job_id}")

            api_response = self.api_service.submit_watermark_job(
                input_uri=input_uri,
                output_uri=output_uri,
                watermark_preset_id=preset_id
            )

            # Extract response data
            api_job_id = api_response.get('id')
            api_status = api_response.get('status')
            
            outputs = api_response.get('outputs', [])
            api_wmid = outputs[0].get('wmid', '') if outputs else ''

            #Update the watermark table with data
            self.dynamo_service.update_job(job_id, {
            "api_job_id": api_job_id,
            "wmid": api_wmid,
            "status": api_status,
            "updated_at": datetime.utcnow().isoformat()
            })

            logger.info(f"DynamoDB: Updated job {job_id} with API details")


        except Exception as e:
            logger.error(f"Error creating watermark job: {str(e)}")
            """ if watermark_job:
                watermark_job.status = 'failed'
                watermark_job.error_message = str(e)
                watermark_job.save() """
            # Fail-safe update
            self.dynamo_service.update_job(job_id, {
                "status": "failed",
                "error": str(e),
                "updated_at": datetime.utcnow().isoformat()
            })
            raise

    def submit_watermark_job(self, input_uri, output_uri, watermark_preset_id):
        """
        Submit watermarking job to external API
        
        Request format:
        POST /api/v3/jobs
        {
            "watermark": {"wm_preset": {"id": "preset_id"}},
            "input": {"uri": "s3://bucket/input.mov"},
            "outputs": [{"uri": "s3://bucket/output.mov"}]
        }
        """
        try:
            payload = {
                "watermark": {
                    "wm_preset": {
                        "id": watermark_preset_id
                    }
                },
                "input": {
                    "uri": input_uri
                },
                "outputs": [
                    {
                        "uri": output_uri
                    }
                ]
            }

            logger.info(f"Submitting watermark job: {json.dumps(payload)}")
            
            response = requests.post(
                f"{self.api_url}/api/v3/jobs?autostart=true",
                json=payload,
                headers=self.headers,
                timeout=30
            )

            response.raise_for_status()
            result = response.json()
            
            logger.info(f"API Response: {json.dumps(result)}")
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"API request error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error submitting watermark job: {str(e)}")
            raise

    def get_job_status(self, job_id):
        """Check watermarking job status"""
        try:
            response = requests.get(
                f"{self.api_url}/api/v3/jobs/{job_id}",
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting job status: {str(e)}")
            raise
