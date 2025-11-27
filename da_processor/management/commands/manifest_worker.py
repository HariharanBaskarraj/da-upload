import json
import logging
from django.core.management.base import BaseCommand
from django.conf import settings
from da_processor.services.sqs_processor_service import SQSProcessorService
from da_processor.services.manifest_service import ManifestService
from da_processor.services.sqs_service import SQSService
from da_processor.services.scheduler_service import SchedulerService

from da_processor.utils.logging_utils import get_logger
logger = get_logger(__name__)

class Command(BaseCommand):
    help = 'Start manifest generation worker that polls SQS queue'

    def handle(self, *args, **options):
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
                    logger.error(
                        "Missing required fields in message",
                        extra={'event_type': 'ERROR', 'da_id': da_id, 'licensee_id': licensee_id}
                    )
                    return
                
                logger.info(
                    "Generating manifest",
                    extra={'event_type': 'PROCESS', 'da_id': da_id, 'licensee_id': licensee_id}
                )
                
                manifest_service = ManifestService()
                sqs_service = SQSService()
                scheduler_service = SchedulerService()
                
                manifest = manifest_service.generate_manifest(da_id)
                
                assets_count = len(manifest.get('assets', []))
                
                logger.info(
                    "Manifest generated",
                    extra={
                        'event_type': 'PROCESS',
                        'da_id': da_id,
                        'title_id': manifest['main_body']['title_id'],
                        'assets_count': assets_count
                    }
                )
                
                if assets_count == 0:
                    logger.warning(
                        "No assets available, skipping manifest send",
                        extra={'event_type': 'PROCESS', 'da_id': da_id}
                    )
                    self.stdout.write(
                        self.style.WARNING(
                            f"DA {da_id}: No assets available, manifest not sent"
                        )
                    )
                    return
                
                success = sqs_service.send_manifest_to_licensee(licensee_id, manifest)
                
                if success:
                    logger.info(
                        "Manifest sent successfully",
                        extra={'event_type': 'PROCESS', 'da_id': da_id, 'licensee_id': licensee_id}
                    )
                    
                    if settings.AWS_SQS_DELIVERY_QUEUE_URL:
                        try:
                            sqs_service.sqs_client.send_message(
                                QueueUrl=settings.AWS_SQS_DELIVERY_QUEUE_URL,
                                MessageBody=json.dumps({'da_id': da_id})
                            )
                            logger.info(
                                f"Delivery tracking triggered for DA: {da_id}",
                                extra={'event_type': 'PROCESS', 'da_id': da_id, 'licensee_id': licensee_id}
                            )
                        except Exception as e:
                            logger.error(
                                "Failed to trigger delivery tracking",
                                extra={'event_type': 'ERROR', 'da_id': da_id},
                                exc_info=True
                            )

                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Manifest sent to {licensee_id} for DA {da_id}: {assets_count} assets"
                        )
                    )
                    
                    scheduler_service.delete_schedule(da_id)
                    logger.info(
                        f"Schedule deleted for DA: {da_id}",
                        extra={'event_type': 'PROCESS', 'da_id': da_id, 'licensee_id': licensee_id}
                    )
                else:
                    logger.error(
                        "Failed to send manifest",
                        extra={'event_type': 'ERROR', 'da_id': da_id, 'licensee_id': licensee_id}
                    )
                    
                    sqs_service.send_to_dlq(
                        {'da_id': da_id, 'licensee_id': licensee_id, 'manifest': manifest},
                        f'Failed to send manifest for DA {da_id}'
                    )
                
            except Exception as e:
                logger.error(
                    "Error processing manifest message",
                    extra={'event_type': 'ERROR', 'da_id': message.get('da_id')},
                    exc_info=True
                )
                
                try:
                    sqs_service = SQSService()
                    sqs_service.send_to_dlq(
                        {'da_id': message.get('da_id'), 'licensee_id': message.get('licensee_id')},
                        str(e)
                    )
                except Exception as dlq_error:
                    logger.error("Failed to send to DLQ", extra={'event_type': 'ERROR'}, exc_info=True)
        
        processor = SQSProcessorService(queue_url, process_manifest_message)
        
        try:
            processor.start_polling()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('Shutting down manifest worker...'))
            processor.stop_polling()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Manifest worker error: {str(e)}'))
            logger.error(f'Manifest worker error: {str(e)}', exc_info=True)