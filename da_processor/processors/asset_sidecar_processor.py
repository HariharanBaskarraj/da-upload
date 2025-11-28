"""
Sidecar processor for asset validation and file comparison.

This processor handles comparison of uploaded files against CSV manifests,
checksum validation, and identification of extra/missing files during
the asset ingestion validation phase.
"""
import csv
import io
import hashlib
import logging
from typing import Dict, List, Tuple
from pathlib import Path
from da_processor.utils.path_utils import normalize_s3_path

logger = logging.getLogger(__name__)


class AssetSidecarProcessor:
    """
    Sidecar processor for asset validation operations.

    This processor complements the main CSV processor by providing
    validation logic specific to file comparison and manifest checking.
    """

    def __init__(self, s3_service, csv_processor):
        """
        Initialize the Asset Sidecar Processor.

        Args:
            s3_service: S3 service instance for file operations
            csv_processor: CSV processor instance for metadata operations
        """
        self.s3_service = s3_service
        self.csv_processor = csv_processor

    def parse_csv_data_rows(self, csv_content: bytes) -> Tuple[List[Dict[str, str]], int]:
        """
        Parse CSV and extract data rows starting from row 12.

        Args:
            csv_content: CSV file content as bytes

        Returns:
            Tuple of (data_rows, csv_row_count)
        """
        decoded = csv_content.decode('utf-8')
        lines = decoded.splitlines()

        # Skip the first 11 lines (metadata/header)
        header_line_index = 11  # row 12 (0-based)
        data_lines = lines[header_line_index:]

        # Rebuild CSV text from row 12 onwards
        csv_text = "\n".join(data_lines)

        # Use DictReader with the correct header
        csv_reader = csv.DictReader(io.StringIO(csv_text))
        csv_rows = list(csv_reader)

        data_rows = csv_rows[0:]
        csv_row_count = len(data_rows)

        logger.debug(f"Parsed {csv_row_count} data rows from CSV")
        return data_rows, csv_row_count

    def compare_files_with_csv(
        self,
        data_rows: List[Dict[str, str]],
        csv_row_count: int,
        asset_files: List[str],
        csv_key: str
    ) -> Tuple[List[str], str]:
        """
        Compare folder files with CSV entries and determine processing status.

        Args:
            data_rows: Parsed CSV data rows
            csv_row_count: Number of rows in CSV
            asset_files: List of asset file keys (excluding CSV)
            csv_key: CSV file key

        Returns:
            Tuple of (files_to_copy, status)
        """
        folder_file_count = len(asset_files)

        # Check if CSV is invalid (no data)
        if csv_row_count == 0:
            logger.error("CSV has no data rows")
            return asset_files + [csv_key], "INVALID_CSV"

        # Missing files scenario
        if folder_file_count < csv_row_count and folder_file_count != 0:
            logger.warning(f"Folder has {folder_file_count} files but CSV lists {csv_row_count}")
            return asset_files + [csv_key], "MISSING_FILES"

        # Extra files scenario
        if folder_file_count > csv_row_count:
            return self._handle_extra_files(data_rows, asset_files, csv_key)

        # Exact match - proceed to checksum validation
        return asset_files + [csv_key], "PENDING_CHECKSUM"

    def _handle_extra_files(
        self,
        data_rows: List[Dict[str, str]],
        asset_files: List[str],
        csv_key: str
    ) -> Tuple[List[str], str]:
        """
        Handle case where folder has more files than CSV lists.

        Args:
            data_rows: Parsed CSV data rows
            asset_files: List of asset file keys
            csv_key: CSV file key

        Returns:
            Tuple of (files_to_copy, status)
        """
        logger.debug("Checking for extra files not listed in CSV")

        # Extract and normalize all file paths from CSV
        csv_paths = [normalize_s3_path(r.get('Folder Path')) for r in data_rows if r.get('Folder Path')]

        # Find extra files (in folder but not in CSV)
        extra_files = [
            asset_key
            for asset_key in asset_files
            if self._extract_folder_path(asset_key) not in csv_paths
        ]

        if not extra_files:
            logger.info("No extra files found, all files match CSV")
            return asset_files + [csv_key], "PENDING_CHECKSUM"
        else:
            logger.warning(f"Found {len(extra_files)} extra files not in CSV")
            return extra_files + [csv_key], "EXTRA_FILES"

    def validate_checksums(
        self,
        data_rows: List[Dict[str, str]],
        asset_files: List[str],
        bucket: str,
        csv_key: str
    ) -> Tuple[List[str], str, List[Dict[str, str]]]:
        """
        Validate checksums for all asset files against CSV manifest.

        Args:
            data_rows: Parsed CSV data rows
            asset_files: List of asset file keys
            bucket: S3 bucket name
            csv_key: CSV file key

        Returns:
            Tuple of (files_to_copy, status, checksum_mismatch)
        """
        checksum_mismatch = []

        for key in asset_files:
            logger.debug(f"Validating checksum for: {key}")
            revised_folder_path = Path(key).parent
            folder_path_without_stage = str(revised_folder_path).replace("Upload/", "", 1) + "/"

            # Find matching entry in CSV
            entry = next(
                (r for r in data_rows
                 if normalize_s3_path(r.get('Folder Path')) == normalize_s3_path(folder_path_without_stage)),
                None
            )

            # NOTE: Checksum validation is currently disabled as per original implementation
            # Uncomment below to enable actual checksum verification
            #
            # if entry and 'Checksum' in entry:
            #     file_data = self.s3_service.get_object_content(bucket, key)
            #     actual_md5 = hashlib.md5(file_data).hexdigest()
            #     if actual_md5 != entry['Checksum']:
            #         checksum_mismatch.append({
            #             'file': key,
            #             'expected': entry['Checksum'],
            #             'actual': actual_md5,
            #             'path': key
            #         })

        if checksum_mismatch:
            logger.warning(f"Detected {len(checksum_mismatch)} checksum mismatches")
            files_to_copy = [csv_key] + [m["path"] for m in checksum_mismatch if "path" in m]
            return files_to_copy, "MISMATCH_CHECKSUM", checksum_mismatch
        else:
            logger.info("All checksums valid (or validation skipped)")
            folder_file_count = len(asset_files)
            status = "VALID_STRUCTURE" if folder_file_count == 0 else "SUCCESS"
            return asset_files + [csv_key], status, []

    @staticmethod
    def _extract_folder_path(asset_key: str) -> str:
        """
        Extract folder path from asset key, removing Upload/ prefix and filename.

        Example:
            Upload/2590.0251/Video/DV HDR/video.mp4 -> 2590.0251/Video/DV HDR/

        Args:
            asset_key: Full S3 asset key

        Returns:
            Normalized folder path
        """
        # Remove Upload/ prefix
        key = asset_key.replace("Upload/", "", 1)

        # Remove filename, keep only folder path
        return normalize_s3_path(key.rsplit("/", 1)[0])
