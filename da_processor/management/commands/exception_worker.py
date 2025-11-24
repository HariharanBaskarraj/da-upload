import logging
from django.core.management.base import BaseCommand
from django.conf import settings
from da_processor.services.sqs_processor_service import SQSProcessorService
from da_processor.services.missing_assets_service import MissingAssetsService
from da_processor.services.email_notification_service import EmailNotificationService
from da_processor.services.scheduler_service import SchedulerService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Start exception notification worker that polls SQS queue'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting Exception Notification Worker...'))
        
        queue_url = settings.AWS_SQS_EXCEPTION_QUEUE_URL
        
        if not queue_url:
            self.stdout.write(self.style.ERROR('AWS_SQS_EXCEPTION_QUEUE_URL not configured'))
            return
        
        def process_exception_message(message: dict):
            try:
                da_id = message.get('da_id')
                
                if not da_id:
                    logger.error(f"No DA ID provided in message: {message}")
                    return
                
                logger.info(f"[EXCEPTION] Checking missing assets for DA: {da_id}")
                
                missing_assets_service = MissingAssetsService()
                email_service = EmailNotificationService()
                scheduler_service = SchedulerService()
                
                missing_assets_info = missing_assets_service.check_missing_assets_for_da(da_id)

                logger.info(f"missing_assets_info: {missing_assets_info}")
                
                if missing_assets_info.get('has_missing_assets'):
                    logger.warning(
                        f"[EXCEPTION] DA {da_id} has {missing_assets_info.get('total_missing_count')} "
                        f"missing assets across {len(missing_assets_info.get('missing_components', []))} components"
                    )
                    
                    self.stdout.write(
                        self.style.WARNING(
                            f"DA {da_id}: {missing_assets_info.get('total_missing_count')} missing assets"
                        )
                    )
                    
                    success = email_service.send_missing_assets_notification(missing_assets_info)
                    
                    if success:
                        logger.info(f"[EXCEPTION] Email notification sent for DA: {da_id}")
                        self.stdout.write(
                            self.style.SUCCESS(f"Email sent for DA {da_id}")
                        )
                    else:
                        logger.error(f"[EXCEPTION] Failed to send email for DA: {da_id}")
                else:
                    logger.info(f"[EXCEPTION] DA {da_id} has no missing assets")
                    self.stdout.write(
                        self.style.SUCCESS(f"DA {da_id}: All assets available")
                    )
                
                scheduler_service.delete_exception_schedule(da_id)
                logger.info(f"[EXCEPTION] Deleted exception schedule for DA: {da_id}")
                
            except Exception as e:
                logger.error(f"[EXCEPTION] Error processing exception message: {e}", exc_info=True)
        
        processor = SQSProcessorService(queue_url, process_exception_message)
        
        try:
            processor.start_polling()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('Shutting down exception worker...'))
            processor.stop_polling()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Exception worker error: {str(e)}'))
            logger.error(f'Exception worker error: {str(e)}', exc_info=True)