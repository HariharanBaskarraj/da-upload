import csv
import logging
from io import StringIO
from typing import Dict, List, Tuple
from .base_processor import BaseDAProcessor

logger = logging.getLogger(__name__)


class CSVProcessor(BaseDAProcessor):
    """Process DA from CSV format"""

    REQUIRED_MAIN_FIELDS = ['Licensee ID', 'Title ID', 'Version ID', 'Release Year',
                            'License Period Start', 'License Period End']
    REQUIRED_COMPONENT_FIELDS = ['Component ID', 'Required Flag']

    def parse_csv(self, csv_content: str) -> Tuple[Dict, List[Dict]]:
        """Parse CSV content into main body and components"""
        csv_reader = csv.reader(StringIO(csv_content))
        all_rows = list(csv_reader)

        # Find the divider row (Component section start)
        divider_index = None
        for i, row in enumerate(all_rows):
            if len(row) >= 3 and row[0] == 'Component ID' and row[1] == 'Required Flag':
                divider_index = i
                break

        if divider_index is None:
            raise ValueError(
                "CSV format invalid: Component section divider not found")

        # Parse main body (2-column structure)
        main_body = {}
        for i in range(1, divider_index):
            row = all_rows[i]
            if len(row) >= 2 and row[0].strip():
                field_name = row[0].strip()
                value = row[1].strip() if len(row) > 1 else ''
                main_body[field_name] = value

        # Parse components (3-column structure)
        components = []
        for i in range(divider_index + 1, len(all_rows)):
            row = all_rows[i]
            if len(row) >= 2 and row[0].strip():
                component = {
                    'Component ID': row[0].strip(),
                    'Required Flag': row[1].strip() if len(row) > 1 else 'FALSE',
                    'Watermark Required': row[2].strip() if len(row) > 2 else 'FALSE'
                }
                components.append(component)

        logger.info(
            f"Parsed CSV: {len(main_body)} main fields, {len(components)} components")
        return main_body, components

    def validate_main_body(self, main_body: Dict) -> None:
        """Validate main body has all required fields"""
        missing_fields = []
        for field in self.REQUIRED_MAIN_FIELDS:
            if field not in main_body or not main_body[field]:
                missing_fields.append(field)

        if missing_fields:
            error_msg = f"Missing required fields: {', '.join(missing_fields)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def validate_components(self, components: List[Dict]) -> None:
        """Validate components have required fields"""
        if not components:
            raise ValueError("No components found in CSV")

        for idx, component in enumerate(components):
            for field in self.REQUIRED_COMPONENT_FIELDS:
                if field not in component or not component[field]:
                    error_msg = f"Component at row {idx + 1} missing required field: {field}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

    def normalize_data(self, main_body: Dict, components: List[Dict]) -> Tuple[Dict, List[Dict]]:
        """Normalize field names to match database schema"""
        normalized_main = {
            'TitleID': main_body.get('Title ID', ''),
            'TitleName': main_body.get('Title Name', ''),
            'TitleEIDRID': main_body.get('Title EIDR ID', ''),
            'VersionID': main_body.get('Version ID', ''),
            'VersionName': main_body.get('Version Name', ''),
            'VersionEIDRID': main_body.get('Version EIDR ID', ''),
            'ReleaseYear': main_body.get('Release Year', ''),
            'LicenseeID': main_body.get('Licensee ID', ''),
            'DADescription': main_body.get('DA Description', ''),
            'DueDate': main_body.get('Due Date', ''),
            'EarliestDeliveryDate': main_body.get('Earliest Delivery Date', ''),
            'LicensePeriodStart': main_body.get('License Period Start', ''),
            'LicensePeriodEnd': main_body.get('License Period End', ''),
            'Territories': main_body.get('Territories', ''),
            'ExceptionNotificationDate': main_body.get('Exception Notification Date', ''),
            'ExceptionRecipients': main_body.get('Exception Recipients', ''),
            'InternalStudioID': main_body.get('Internal Studio ID', ''),
            'StudioSystemID': main_body.get('Studio System ID', ''),
        }

        normalized_components = [
            {
                'ComponentID': comp['Component ID'],
                'RequiredFlag': comp['Required Flag'].upper(),
                'WatermarkRequired': comp['Watermark Required'].upper(),
            }
            for comp in components
        ]

        return normalized_main, normalized_components

    def process(self, csv_content: str) -> Dict:
        """Main processing method"""
        try:
            # Parse CSV
            main_body, components = self.parse_csv(csv_content)

            # Validate
            self.validate_main_body(main_body)
            self.validate_components(components)

            # Normalize
            normalized_main, normalized_components = self.normalize_data(
                main_body, components)

            # Apply defaults
            normalized_main = self.default_service.apply_defaults(
                normalized_main,
                normalized_main['LicenseeID']
            )

            # Store in DynamoDB
            title_result = self.db_service.create_or_update_title(
                normalized_main)
            record_id = title_result['ID']

            # Store components linked to this record
            for component in normalized_components:
                self.db_service.create_component(
                    record_id, normalized_main['TitleID'], component)

            logger.info(f"✅ Successfully processed DA upload: ID={record_id}")

            return {
                'success': True,
                'id': record_id,
                'title_id': normalized_main['TitleID'],
                'version_id': normalized_main['VersionID'],
                'licensee_id': normalized_main['LicenseeID'],
                'components_count': len(normalized_components)
            }

        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")
            self.send_exception_notification(
                str(e), main_body if 'main_body' in locals() else {})
            raise
