"""
Manifest Worker Management Command.

This command runs as a worker that polls SQS queue for manifest generation requests,
generates delivery manifests, sends them to licensees, and triggers delivery tracking.
"""
import json
import logging
from django.core.management.base import BaseCommand
from django.conf import settings
from da_processor.services.sqs_processor_service import SQSProcessorService
from da_processor.services.manifest_service import ManifestService
from da_processor.services.sqs_service import SQSService
from da_processor.services.scheduler_service import SchedulerService
from da_processor.services.s3_service import S3Service
from da_processor.services.watermark_cache_service import WatermarkCacheService

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
            """
            Process manifest generation message from SQS queue.

            Generates manifest, sends to licensee queue, triggers delivery tracking,
            and deletes schedule. Sends failures to DLQ.

            Args:
                message: SQS message containing da_id and licensee_id
            """
            try:
                da_id = message.get('da_id')
                licensee_id = message.get('licensee_id')
                
                if not da_id or not licensee_id:
                    logger.error(f"Missing da_id or licensee_id in message: {message}")
                    return
                
                logger.info(f"Generating manifest for DA: {da_id}, Licensee: {licensee_id}")
                
                manifest_service = ManifestService()
                sqs_service = SQSService()
                scheduler_service = SchedulerService()
                s3_service = S3Service()
                wm_service = WatermarkCacheService()
                
                manifest = manifest_service.generate_manifest(da_id)
                
                assets_count = len(manifest.get('assets', []))
                logger.info(f"Manifest generated: Title={manifest['main_body']['title_id']}, Assets={assets_count}")
                
                logger.info(f"Obtained Manifest:{manifest}")
                if assets_count == 0:
                    logger.warning(f"No assets available for DA {da_id}, skipping manifest send")
                    self.stdout.write(
                        self.style.WARNING(
                            f"DA {da_id}: No assets available, manifest not sent"
                        )
                    )
                    return
                
                #If the assets have mov file then move the file from watermarkcache to Licensee Cache and submit one more watermark
                #MOv files move the file from watermarkcache to Licensee Cache
                moved_details = s3_service.move_mov_files(manifest)
                logger.info(f"MOVED DETAILS: {moved_details}")

                if moved_details:
                    logger.info(f"{len(moved_details)} MOV file(s) moved to Licensee Cache for DA {da_id}")

                    for moved in moved_details:
                        lowest_key = moved["lowest_key"]   # watermarkcache/.../FirstLook_WM1.mov
                        base_file = moved["base_file"]     # FirstLook.mov

                        logger.info(f"Generating next WM for: {base_file} using {lowest_key}")

                        new_file = wm_service.generate_next_watermark(
                            bucket=settings.AWS_WATERMARKED_BUCKET,
                            source_key=lowest_key,
                            preset_id=settings.WATERMARK_PRESET_ID
                        )

                        logger.info(f"New WM version created for {base_file}: {new_file}")

                else:
                    logger.info("No MOV files to move.")

                                

                success = sqs_service.send_manifest_to_licensee(licensee_id, manifest)
                
                if success:
                    logger.info(f"Manifest sent successfully for DA: {da_id}")
                    
                    if settings.AWS_SQS_DELIVERY_QUEUE_URL:
                        try:
                            sqs_service.sqs_client.send_message(
                                QueueUrl=settings.AWS_SQS_DELIVERY_QUEUE_URL,
                                MessageBody=json.dumps({'da_id': da_id})
                            )
                            logger.info(f"Delivery tracking triggered for DA: {da_id}")
                        except Exception as e:
                            logger.error(f"Failed to trigger delivery tracking: {e}")

                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Manifest sent to {licensee_id} for DA {da_id}: {assets_count} assets"
                        )
                    )
                    
                    #scheduler_service.delete_schedule(da_id)
                    logger.info(f"Schedule deleted for DA: {da_id}")
                else:
                    logger.error(f"Failed to send manifest for DA: {da_id}")
                    
                    sqs_service.send_to_dlq(
                        {'da_id': da_id, 'licensee_id': licensee_id, 'manifest': manifest},
                        f'Failed to send manifest for DA {da_id}'
                    )
                
            except Exception as e:
                logger.error(f"Error processing manifest message: {e}", exc_info=True)
                
                try:
                    sqs_service = SQSService()
                    sqs_service.send_to_dlq(
                        {'da_id': message.get('da_id'), 'licensee_id': message.get('licensee_id')},
                        str(e)
                    )
                except Exception as dlq_error:
                    logger.error(f"Failed to send to DLQ: {dlq_error}")
        
        processor = SQSProcessorService(queue_url, process_manifest_message)
        
        try:
            processor.start_polling()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('Shutting down manifest worker...'))
            processor.stop_polling()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Manifest worker error: {str(e)}'))
            logger.error(f'Manifest worker error: {str(e)}', exc_info=True)