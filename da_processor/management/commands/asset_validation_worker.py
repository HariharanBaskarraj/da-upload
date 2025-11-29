"""
Asset Validation Worker Management Command.

This command runs as a worker that polls SQS queue for asset validation requests,
validates uploaded asset packages, processes CSV manifests, and moves files to
appropriate locations (success/error) based on validation results.
"""
import logging
import traceback
from datetime import datetime, timezone, timedelta
from django.core.management.base import BaseCommand
from django.conf import settings
from da_processor.services.sqs_processor_service import SQSProcessorService
from da_processor.services.asset_ingest_dynamodb_service import AssetIngestDynamoDBService
from da_processor.services.asset_ingest_s3_service import AssetIngestS3Service
from da_processor.processors.asset_csv_processor import AssetCSVProcessor
from da_processor.processors.asset_sidecar_processor import AssetSidecarProcessor

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Django management command for asset validation processing.

    This worker:
    - Polls SQS queue for validation trigger messages (sent by EventBridge Scheduler)
    - Scans DynamoDB for packages with VALID_STRUCTURE status
    - Validates CSV presence and structure
    - Compares uploaded files against CSV manifest
    - Validates checksums
    - Processes successful packages to asset repository
    - Moves failed packages to error location
    """
    help = 'Start asset validation worker that polls SQS queue'

    def handle(self, *args, **options):
        """Execute the asset validation worker."""
        self.stdout.write(self.style.SUCCESS('Starting Asset Validation Worker...'))

        queue_url = settings.AWS_SQS_ASSET_VALIDATION_QUEUE_URL

        if not queue_url:
            self.stdout.write(self.style.ERROR('AWS_SQS_ASSET_VALIDATION_QUEUE_URL not configured'))
            return

        # Initialize services
        try:
            ingest_bucket = getattr(settings, 'INGEST_S3_BUCKET', None)
            asset_repo_bucket = getattr(settings, 'ASSET_REPO_S3_BUCKET', None)
            ingest_table_name = getattr(settings, 'INGEST_ASSET_TABLE', None)
            title_info_table_name = getattr(settings, 'TITLE_INFO_TABLE', None)
            asset_info_table_name = getattr(settings, 'ASSET_INFO_TABLE', None)

            if not all([ingest_bucket, asset_repo_bucket, ingest_table_name, title_info_table_name, asset_info_table_name]):
                self.stdout.write(self.style.ERROR('Required environment variables not configured'))
                logger.error('Missing required configuration for asset validation worker')
                return

            s3_service = AssetIngestS3Service(ingest_bucket, asset_repo_bucket)
            db_service = AssetIngestDynamoDBService(ingest_table_name, title_info_table_name, asset_info_table_name)
            csv_processor = AssetCSVProcessor(db_service)
            sidecar_processor = AssetSidecarProcessor(s3_service, csv_processor)

            logger.info('Asset validation worker initialized successfully')

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Initialization error: {e}'))
            logger.error(f'Failed to initialize asset validation worker: {e}', exc_info=True)
            return

        def process_validation_message(message: dict):
            """
            Process asset validation trigger message from SQS queue.

            Expected message format from EventBridge Scheduler:
            {
                "trigger": "asset_validation_check"
            }

            This triggers a scan of all packages with VALID_STRUCTURE status.

            Args:
                message: SQS message containing validation trigger
            """
            try:
                trigger = message.get('trigger')

                if trigger != 'asset_validation_check':
                    logger.warning(f"Unknown trigger type: {trigger}")
                    return

                logger.info("Asset validation check triggered - scanning for VALID_STRUCTURE packages")

                # Calculate cutoff time
                now = datetime.now(timezone.utc)
                cutoff_time = now - timedelta(minutes=getattr(settings, 'ASSET_VALIDATION_CUTOFF_MINUTES', 1))
                logger.info(f"Checking for packages older than {cutoff_time.isoformat()}")

                # Scan for all packages with VALID_STRUCTURE status
                try:
                    packages = db_service.scan_ingest_items(status="VALID_STRUCTURE")
                    logger.info(f"Found {len(packages)} packages with VALID_STRUCTURE status")
                except Exception as e:
                    logger.error(f"Error scanning ingest table: {e}")
                    return

                if not packages:
                    logger.info("No packages found for processing")
                    return

                # Process each package
                processed_count = 0
                failed_count = 0

                for pkg in packages:
                    try:
                        created_str = pkg.get("CreatedDate")
                        folder_prefix = pkg.get("AssetPath")

                        if not created_str or not folder_prefix:
                            logger.warning(f"Skipping package with missing CreatedDate or AssetPath")
                            continue

                        created_at = datetime.fromisoformat(created_str)
                        if created_at > cutoff_time:
                            logger.debug(f"Package {folder_prefix} not yet past cutoff time, skipping")
                            continue

                        logger.info(f"Processing package: {folder_prefix} (created: {created_at})")

                        self._process_single_package(
                            pkg, s3_service, db_service, csv_processor, sidecar_processor,
                            ingest_bucket, asset_repo_bucket
                        )

                        processed_count += 1

                    except Exception as e:
                        logger.error(f"Error processing package {pkg.get('AssetPath')}: {e}")
                        logger.error(f"Traceback: {traceback.format_exc()}")
                        failed_count += 1

                self.stdout.write(
                    self.style.SUCCESS(
                        f"Validation check complete: {processed_count} processed, {failed_count} failed"
                    )
                )

            except Exception as e:
                logger.error(f"Error processing validation trigger message: {e}", exc_info=True)
                raise

        processor = SQSProcessorService(queue_url, process_validation_message)

        try:
            processor.start_polling()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('Shutting down asset validation worker...'))
            processor.stop_polling()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Asset validation worker error: {str(e)}'))
            logger.error(f'Asset validation worker error: {str(e)}', exc_info=True)

    def _process_single_package(self, pkg, s3_service, db_service, csv_processor, sidecar_processor, ingest_bucket, asset_repo_bucket):
        """
        Process a single package through the validation pipeline.

        Args:
            pkg: Package dictionary from DynamoDB
            s3_service: S3 service instance
            db_service: DynamoDB service instance
            csv_processor: CSV processor instance
            sidecar_processor: Sidecar processor instance
            ingest_bucket: Ingest S3 bucket name
            asset_repo_bucket: Asset repository S3 bucket name
        """
        folder_prefix = pkg.get("AssetPath")
        process_status = pkg.get("ProcessStatus")

        if not folder_prefix:
            logger.warning(f"Skipping package with missing AssetPath")
            return

        logger.info(f"Processing package: {folder_prefix}")

        # Get title ID and list all objects
        title_id = folder_prefix.split('/')[0]
        all_objects = s3_service.list_objects(ingest_bucket, f'Upload/{title_id}')
        file_keys = [obj['Key'] for obj in all_objects] if all_objects else []

        # Check for CSV presence
        has_csv = any(key.lower().endswith('.csv') for key in file_keys)
        logger.info(f"CSV present for {folder_prefix}: {has_csv}")

        if not has_csv:
            logger.warning(f"No CSV found for {folder_prefix}, marking as FAILED")
            self._handle_missing_csv(db_service, s3_service, folder_prefix, file_keys, ingest_bucket)
            return

        # Process CSV validation
        if has_csv and process_status == "VALID_STRUCTURE":
            self._validate_and_process_csv(
                folder_prefix, file_keys, s3_service, db_service, csv_processor,
                sidecar_processor, ingest_bucket, asset_repo_bucket
            )

    def _handle_missing_csv(self, db_service, s3_service, folder_prefix, file_keys, ingest_bucket):
        """Handle packages missing CSV files."""
        for key in file_keys:
            dest_key = key.replace("Upload/", "Error/", 1)
            s3_service.copy_object(ingest_bucket, key, ingest_bucket, dest_key)
        db_service.update_process_status(folder_prefix, 'FAILED')
        logger.info(f"Moved all files to Error/ for {folder_prefix}")

    def _validate_and_process_csv(self, folder_prefix, file_keys, s3_service, db_service, csv_processor, sidecar_processor, ingest_bucket, asset_repo_bucket):
        """
        Validate CSV and process package.

        Args:
            folder_prefix: Folder prefix for the package
            file_keys: List of all file keys in the package
            s3_service: S3 service instance
            db_service: DynamoDB service instance
            csv_processor: CSV processor instance
            sidecar_processor: Sidecar processor instance
            ingest_bucket: Ingest bucket name
            asset_repo_bucket: Asset repository bucket name
        """
        csv_keys = [k for k in file_keys if k.lower().endswith('.csv')]
        csv_key = csv_keys[0] if csv_keys else None

        if not csv_key:
            logger.error(f"CSV key not found for {folder_prefix}")
            return

        logger.info(f"Processing CSV: {csv_key}")

        # Read and validate CSV
        csv_content = s3_service.get_object_content(ingest_bucket, csv_key)
        is_valid = csv_processor.validate_csv(csv_content)

        if not is_valid:
            logger.error(f"CSV validation failed for {folder_prefix}")
            self._move_to_error(file_keys, s3_service, db_service, folder_prefix, ingest_bucket, "INVALID_CSV")
            return

        # Process CSV metadata
        csv_processor.process_csv(csv_content, folder_prefix)
        logger.info(f"Processed CSV metadata successfully for {folder_prefix}")

        # Parse and compare files
        data_rows, csv_row_count = sidecar_processor.parse_csv_data_rows(csv_content)
        asset_files = [k for k in file_keys if not k.lower().endswith('.csv')]

        files_to_copy, status = sidecar_processor.compare_files_with_csv(
            data_rows, csv_row_count, asset_files, csv_key
        )

        # Handle validation results
        if status == "EXTRA_FILES":
            self._handle_extra_files(files_to_copy, asset_files, s3_service, db_service, folder_prefix, ingest_bucket, data_rows, sidecar_processor)
        elif status == "PENDING_CHECKSUM":
            files_to_copy, status, _ = sidecar_processor.validate_checksums(data_rows, asset_files, ingest_bucket, csv_key)
            self._finalize_processing(files_to_copy, status, s3_service, db_service, folder_prefix, ingest_bucket, asset_repo_bucket)
        else:
            self._finalize_processing(files_to_copy, status, s3_service, db_service, folder_prefix, ingest_bucket, asset_repo_bucket)

    def _move_to_error(self, file_keys, s3_service, db_service, folder_prefix, ingest_bucket, status):
        """Move files to error location."""
        for key in file_keys:
            dest_key = key.replace("Upload/", "Error/", 1)
            s3_service.copy_object(ingest_bucket, key, ingest_bucket, dest_key)
        db_service.update_process_status(folder_prefix, status)
        logger.info(f"Moved files to Error/ with status {status}")

    def _handle_extra_files(self, files_to_copy, asset_files, s3_service, db_service, folder_prefix, ingest_bucket, data_rows, sidecar_processor):
        """Handle packages with extra files."""
        for key in files_to_copy:
            dest_key = key.replace("Upload/", "Error/", 1)
            s3_service.copy_object(ingest_bucket, key, ingest_bucket, dest_key)
            s3_service.delete_object(ingest_bucket, key)

        # Update DB for extra files
        for key in files_to_copy:
            normalized_path = key.replace("Upload/", "", 1)
            db_service.update_process_status(normalized_path, "EXTRA_FILES")

        logger.info(f"Handled extra files for {folder_prefix}")

    def _finalize_processing(self, files_to_copy, status, s3_service, db_service, folder_prefix, ingest_bucket, asset_repo_bucket):
        """
        Finalize package processing based on status.

        Args:
            files_to_copy: List of files to process
            status: Final processing status
            s3_service: S3 service instance
            db_service: DynamoDB service instance
            folder_prefix: Folder prefix
            ingest_bucket: Ingest bucket name
            asset_repo_bucket: Asset repository bucket name
        """
        if status == "SUCCESS":
            # Move to asset repository
            for key in files_to_copy:
                dest_key = key.replace("Upload/", "", 1)
                s3_service.copy_object(ingest_bucket, key, asset_repo_bucket, dest_key)
                db_service.update_process_status(dest_key, status)
            logger.info(f"Successfully processed package: {folder_prefix}")
        elif status in ("MISMATCH_CHECKSUM", "INVALID_CSV", "MISSING_FILES"):
            # Move to error
            self._move_to_error(files_to_copy, s3_service, db_service, folder_prefix, ingest_bucket, status)

        # Update overall package status
        db_service.update_process_status(folder_prefix, status)

        # Verify and delete source files
        folder_path_to_delete = files_to_copy[0].replace("Upload/", "", 1).split('/')[0] if files_to_copy else None
        if folder_path_to_delete:
            if status in ("MISMATCH_CHECKSUM", "INVALID_CSV"):
                s3_service.verify_and_delete_error_folder(ingest_bucket, folder_path_to_delete)
            elif status == "SUCCESS":
                s3_service.verify_and_delete_folder(ingest_bucket, asset_repo_bucket, folder_path_to_delete)

        logger.info(f"Finalized processing for {folder_prefix} with status {status}")
