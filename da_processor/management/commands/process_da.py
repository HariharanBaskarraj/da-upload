import os
import sys
import logging
from django.core.management.base import BaseCommand
from da_processor.processors.csv_processor import CSVProcessor
from da_processor.services.s3_service import S3Service

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Process Distribution Authorization from S3 CSV file'

    def add_arguments(self, parser):
        parser.add_argument(
            '--s3-key',
            type=str,
            help='S3 object key to process'
        )
        parser.add_argument(
            '--bucket',
            type=str,
            help='S3 bucket name'
        )

    def handle(self, *args, **options):
        """Process CSV file from S3"""
        try:
            # Get parameters from command line or environment variables
            s3_key = options.get('s3_key') or os.environ.get('S3_FILE_KEY')
            bucket = options.get('bucket') or os.environ.get('AWS_S3_BUCKET')

            if not s3_key:
                self.stdout.write(self.style.ERROR('S3 key not provided'))
                sys.exit(1)

            if not bucket:
                self.stdout.write(self.style.ERROR('Bucket name not provided'))
                sys.exit(1)

            self.stdout.write(
                f'Processing CSV file: {s3_key} from bucket: {bucket}')

            # Download CSV from S3
            s3_service = S3Service()
            csv_content = s3_service.get_csv_content(s3_key)

            # Process CSV
            processor = CSVProcessor()
            result = processor.process(csv_content)

            self.stdout.write(self.style.SUCCESS(
                f'Successfully processed DA:'))
            self.stdout.write(f"  Title ID: {result['title_id']}")
            self.stdout.write(f"  Version ID: {result['version_id']}")
            self.stdout.write(f"  Licensee ID: {result['licensee_id']}")
            self.stdout.write(f"  Components: {result['components_count']}")

            # Optionally delete the file after successful processing
            delete_after_process = os.environ.get(
                'DELETE_AFTER_PROCESS', 'true').lower() == 'true'
            if delete_after_process:
                s3_service.delete_file(s3_key)
                self.stdout.write(self.style.SUCCESS(
                    f'Deleted processed file: {s3_key}'))

            sys.exit(0)

        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f'Error processing DA: {str(e)}'))
            logger.error(f'Error processing DA: {str(e)}', exc_info=True)
            sys.exit(1)
