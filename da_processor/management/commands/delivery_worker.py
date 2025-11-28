"""
Delivery Worker Management Command.

This command runs as a worker that polls SQS queue for delivery tracking requests,
processes file delivery status updates, and sends manifests to licensees.
"""
import logging
from django.core.management.base import BaseCommand
from django.conf import settings
from da_processor.services.sqs_processor_service import SQSProcessorService
from da_processor.services.delivery_orchestrator_service import DeliveryOrchestratorService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Django management command for delivery tracking worker.

    This worker:
    - Polls SQS queue for delivery tracking messages
    - Validates delivery windows
    - Generates manifests with asset information
    - Tracks file delivery statuses
    - Updates component and DA delivery statuses
    - Sends enriched manifests to licensees via SQS
    """
    help = 'Start delivery tracking worker that polls SQS queue'

    def handle(self, *args, **options):
        """Execute the delivery tracking worker."""
        self.stdout.write(self.style.SUCCESS('Starting Delivery Tracking Worker...'))
        
        queue_url = settings.AWS_SQS_DELIVERY_QUEUE_URL
        
        if not queue_url:
            self.stdout.write(self.style.ERROR('AWS_SQS_DELIVERY_QUEUE_URL not configured'))
            return
        
        def process_delivery_message(message: dict):
            try:
                da_id = message.get('da_id')
                
                if not da_id:
                    logger.error(f"No DA ID provided in message: {message}")
                    return
                
                logger.info(f"Processing delivery for DA: {da_id}")
                
                orchestrator = DeliveryOrchestratorService()
                result = orchestrator.process_delivery_for_da(da_id)
                
                if result.get('success'):
                    if result.get('manifest_sent'):
                        logger.info(
                            f"Delivery processed: DA {da_id} - Manifest sent with "
                            f"{result.get('new_or_revised_files', 0)} new/revised files"
                        )
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"DA {da_id}: Manifest sent ({result.get('new_or_revised_files', 0)} changed)"
                            )
                        )
                    else:
                        reason = result.get('reason', 'unknown')
                        logger.info(f"Delivery processed: DA {da_id} - No manifest sent ({reason})")
                        self.stdout.write(
                            self.style.WARNING(f"DA {da_id}: No manifest sent ({reason})")
                        )
                else:
                    reason = result.get('reason', 'unknown')
                    logger.warning(f"Delivery not processed for DA {da_id}: {reason}")
                
            except Exception as e:
                logger.error(f"Error processing delivery message: {e}", exc_info=True)
        
        processor = SQSProcessorService(queue_url, process_delivery_message)
        
        try:
            processor.start_polling()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('Shutting down delivery worker...'))
            processor.stop_polling()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Delivery worker error: {str(e)}'))
            logger.error(f'Delivery worker error: {str(e)}', exc_info=True)