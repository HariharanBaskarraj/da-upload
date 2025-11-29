"""
Manifest Worker Management Command.

This command runs as a worker that polls SQS queue for manifest generation requests,
generates delivery manifests, sends them to licensees, and triggers delivery tracking.
"""
from datetime import datetime
import json
import logging
from django.core.management.base import BaseCommand
from django.conf import settings
from da_processor.services.dynamodb_service import DynamoDBService
from da_processor.services.sqs_processor_service import SQSProcessorService
from da_processor.services.manifest_service import ManifestService
from da_processor.services.sqs_service import SQSService
from da_processor.services.scheduler_service import SchedulerService
from da_processor.services.s3_service import S3Service
from da_processor.services.watermark_cache_service import WatermarkCacheService
from da_processor.utils.date_utils import parse_date

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Django management command for manifest generation worker.

    This worker:
    - Polls SQS queue for manifest generation messages
    - Generates comprehensive delivery manifests
    - Sends manifests to licensee-specific SQS queues
    - Triggers delivery tracking workflows
    - Deletes manifest schedules after processing
    - Handles failures by sending messages to DLQ
    """
    help = 'Start manifest generation worker that polls SQS queue'

    def handle(self, *args, **options):
        """
        Execute the manifest generation worker.

        Args:
            *args: Variable length argument list
            **options: Arbitrary keyword arguments
        """
        self.stdout.write(self.style.SUCCESS('Starting Manifest Generation Worker...'))

        queue_url = settings.AWS_SQS_MANIFEST_QUEUE_URL

        if not queue_url:
            self.stdout.write(self.style.ERROR('AWS_SQS_MANIFEST_QUEUE_URL not configured'))
            return

        def process_manifest_message(message: dict):
            try:
                da_id = message.get('da_id')
                licensee_id = message.get('licensee_id')
                
                if not da_id or not licensee_id:
                    logger.error(f"Missing da_id or licensee_id in message: {message}")
                    return
                
                logger.info(f"[MANIFEST] Processing DA: {da_id}, Licensee: {licensee_id}")
                
                db_service = DynamoDBService()
                manifest_service = ManifestService()
                sqs_service = SQSService()
                scheduler_service = SchedulerService()
                s3_service = S3Service()
                wm_service = WatermarkCacheService()
                
                da_info = db_service.get_da_record(da_id)
                if not da_info:
                    logger.error(f"[MANIFEST] DA not found: {da_id}")
                    return
                
                current_time = datetime.now(datetime.now().astimezone().tzinfo)
                earliest_delivery = parse_date(da_info.get('Earliest_Delivery_Date'))
                license_end = parse_date(da_info.get('License_Period_End'))
                
                # Check license end FIRST
                if license_end and current_time >= license_end:
                    logger.info(f"[MANIFEST] License ended for DA {da_id}, setting Is_Active=False")
                    db_service.set_da_inactive(da_id)
                    scheduler_service.delete_schedule(da_id)
                    scheduler_service.delete_exception_schedule(da_id)
                    logger.info(f"[MANIFEST] Deleted all schedulers for DA {da_id}")
                    return
                
                # Check if before earliest delivery
                if earliest_delivery and current_time < earliest_delivery:
                    logger.info(f"[MANIFEST] Before earliest delivery date for DA {da_id}, skipping")
                    return
                
                # Activate DA if not already active
                is_active = da_info.get('Is_Active', False)
                if not is_active:
                    logger.info(f"[MANIFEST] Activating DA {da_id} (earliest delivery date reached)")
                    db_service.set_da_active(da_id)
                
                # Generate manifest
                manifest = manifest_service.generate_manifest(da_id)
                assets = manifest.get('assets', [])
                
                logger.info(f"[MANIFEST] Manifest generated: {len(assets)} assets")
                
                if not assets:
                    logger.warning(f"[MANIFEST] No assets for DA {da_id}, skipping")
                    return
                
                # ALWAYS trigger delivery worker
                if settings.AWS_SQS_DELIVERY_QUEUE_URL:
                    try:
                        sqs_service.sqs_client.send_message(
                            QueueUrl=settings.AWS_SQS_DELIVERY_QUEUE_URL,
                            MessageBody=json.dumps({'da_id': da_id})
                        )
                        logger.info(f"[MANIFEST] Delivery tracking triggered for DA: {da_id}")
                    except Exception as e:
                        logger.error(f"[MANIFEST] Failed to trigger delivery tracking: {e}")
                
                # Check if we need to send to licensee
                has_changes = any(
                    asset.get('file_status', '').upper() in ['NEW', 'REVISED'] 
                    for asset in assets
                )
                
                if not has_changes:
                    logger.info(f"[MANIFEST] No changed assets for DA {da_id}, skipping manifest send to licensee")
                    return  # ← STOP HERE, don't send to licensee
                
                # MOV file handling
                moved_details = s3_service.move_mov_files(manifest)
                logger.info(f"[MANIFEST] Moved {len(moved_details)} MOV files")

                if moved_details:
                    for moved in moved_details:
                        lowest_key = moved["lowest_key"]
                        
                        new_file = wm_service.generate_next_watermark(
                            bucket=settings.AWS_WATERMARKED_BUCKET,
                            source_key=lowest_key,
                            preset_id=settings.WATERMARK_PRESET_ID
                        )
                        logger.info(f"[MANIFEST] New WM version: {new_file}")
                
                # Send to licensee queue
                # success = sqs_service.send_manifest_to_licensee(licensee_id, manifest)
                
                # if success:
                #     logger.info(f"[MANIFEST] Manifest sent successfully for DA: {da_id}")
                #     self.stdout.write(self.style.SUCCESS(f"Manifest sent for DA {da_id}: {len(assets)} assets"))
                # else:
                #     logger.error(f"[MANIFEST] Failed to send manifest for DA: {da_id}")
                #     sqs_service.send_to_dlq(
                #         {'da_id': da_id, 'licensee_id': licensee_id, 'manifest': manifest},
                #         f'Failed to send manifest for DA {da_id}'
                #     )
                
            except Exception as e:
                logger.error(f"[MANIFEST] Error processing message: {e}", exc_info=True)
                try:
                    sqs_service = SQSService()
                    sqs_service.send_to_dlq(
                        {'da_id': message.get('da_id'), 'licensee_id': message.get('licensee_id')},
                        str(e)
                    )
                except Exception as dlq_error:
                    logger.error(f"[MANIFEST] Failed to send to DLQ: {dlq_error}")
        
        processor = SQSProcessorService(queue_url, process_manifest_message)
        
        try:
            processor.start_polling()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('Shutting down manifest worker...'))
            processor.stop_polling()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Manifest worker error: {str(e)}'))
            logger.error(f'Manifest worker error: {str(e)}', exc_info=True)