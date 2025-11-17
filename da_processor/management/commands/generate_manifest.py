import os
import sys
import logging
from django.core.management.base import BaseCommand
from da_processor.services.manifest_service import ManifestService
from da_processor.services.sqs_service import SQSService
from da_processor.services.scheduler_service import SchedulerService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Generate and send manifest to licensee SQS queue'

    def add_arguments(self, parser):
        parser.add_argument(
            '--da-id',
            type=str,
            help='Distribution Authorization ID'
        )
        parser.add_argument(
            '--licensee-id',
            type=str,
            help='Licensee ID'
        )

    def handle(self, *args, **options):
        da_id = options.get('da_id') or os.environ.get('DA_ID')
        licensee_id = options.get('licensee_id') or os.environ.get('LICENSEE_ID')

        if not da_id:
            self.stdout.write(self.style.ERROR('DA ID not provided'))
            sys.exit(1)

        if not licensee_id:
            self.stdout.write(self.style.ERROR('Licensee ID not provided'))
            sys.exit(1)

        self.stdout.write(f'Generating manifest for DA: {da_id}, Licensee: {licensee_id}')

        manifest_service = ManifestService()
        sqs_service = SQSService()
        scheduler_service = SchedulerService()

        try:
            manifest = manifest_service.generate_manifest(da_id)
            
            self.stdout.write(self.style.SUCCESS('Manifest generated successfully'))
            self.stdout.write(f"Title ID: {manifest['main_body']['title_id']}")
            self.stdout.write(f"Version ID: {manifest['main_body']['version_id']}")
            self.stdout.write(f"Assets Count: {len(manifest['assets'])}")
            
            success = sqs_service.send_manifest_to_licensee(licensee_id, manifest)
            
            if success:
                self.stdout.write(self.style.SUCCESS(
                    f'Manifest sent to licensee queue: {licensee_id}'))
                
                scheduler_service.delete_schedule(da_id)
                self.stdout.write(self.style.SUCCESS(
                    f'Schedule deleted for DA: {da_id}'))
                
                sys.exit(0)
            else:
                self.stdout.write(self.style.ERROR(
                    'Failed to send manifest to SQS'))
                
                sqs_service.send_to_dlq(
                    {'da_id': da_id, 'licensee_id': licensee_id, 'manifest': manifest},
                    f'Failed to send manifest for DA {da_id}'
                )
                
                sys.exit(1)

        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f'Error generating manifest: {str(e)}'))
            logger.error(f'Error generating manifest: {str(e)}', exc_info=True)
            
            try:
                sqs_service.send_to_dlq(
                    {'da_id': da_id, 'licensee_id': licensee_id},
                    str(e)
                )
            except Exception as dlq_error:
                logger.error(f'Failed to send to DLQ: {dlq_error}')
            
            sys.exit(1)