import logging
from django.core.management.base import BaseCommand
from django.conf import settings
from da_processor.services.sqs_processor_service import SQSProcessorService
from da_processor.processors.csv_processor import CSVProcessor
from da_processor.services.s3_service import S3Service

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Start CSV processing worker that polls SQS queue'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS(
            'Starting CSV Processing Worker...'))

        queue_url = settings.AWS_SQS_CSV_QUEUE_URL

        if not queue_url:
            self.stdout.write(self.style.ERROR(
                'AWS_SQS_CSV_QUEUE_URL not configured'))
            return

        def process_csv_message(message: dict):
            s3_key = None
            try:
                s3_key = message.get('s3_key')
                bucket = message.get('bucket') or settings.AWS_DA_BUCKET

                if not s3_key:
                    logger.error("No S3 key provided in message")
                    return

                if not bucket:
                    logger.error("No bucket name provided")
                    return

                logger.info(f"Processing CSV: {s3_key} from bucket: {bucket}")

                s3_service = S3Service()
                csv_content = s3_service.get_csv_content(s3_key)

                if not csv_content:
                    logger.error(
                        f"Failed to retrieve CSV content for {s3_key}")
                    s3_service.move_file_to_error(s3_key)
                    return

                processor = CSVProcessor()
                result = processor.process(csv_content)

                logger.info(
                    f"Successfully processed DA: ID={result['id']}, Title={result['title_id']}")
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Processed DA {result['id']}: {result['components_count']} components"
                    )
                )

                moved = s3_service.move_file_to_processed(s3_key)
                if moved:
                    logger.info(
                        f"Moved processed file to 'Processed/': {s3_key}")
                else:
                    logger.warning(
                        f"Failed to move file to 'Processed/': {s3_key}")

            except Exception as e:
                logger.error(
                    f"Error processing CSV message: {e}", exc_info=True)

                if s3_key:
                    try:
                        s3_service = S3Service()
                        moved = s3_service.move_file_to_error(s3_key)
                        if moved:
                            logger.info(
                                f"Moved failed file to 'Error/': {s3_key}")
                        else:
                            logger.error(
                                f"Failed to move file to 'Error/': {s3_key}")
                    except Exception as move_error:
                        logger.error(f"Error moving failed file: {move_error}")

        processor = SQSProcessorService(queue_url, process_csv_message)

        try:
            processor.start_polling()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING(
                'Shutting down CSV worker...'))
            processor.stop_polling()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'CSV worker error: {str(e)}'))
            logger.error(f'CSV worker error: {str(e)}', exc_info=True)
