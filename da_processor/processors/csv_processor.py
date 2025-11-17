import csv
import logging
from io import StringIO
from typing import Dict, List, Tuple
from django.conf import settings
from .base_processor import BaseDAProcessor
from da_processor.services.scheduler_service import SchedulerService

logger = logging.getLogger(__name__)


class CSVProcessor(BaseDAProcessor):

    REQUIRED_MAIN_FIELDS = ['Licensee ID', 'Title ID', 'Version ID', 'Release Year',
                            'License Period Start', 'License Period End']
    REQUIRED_COMPONENT_FIELDS = ['Component ID', 'Required Flag']

    def __init__(self):
        super().__init__()
        self.scheduler_service = SchedulerService()

    def parse_csv(self, csv_content: str) -> Tuple[Dict, List[Dict]]:
        csv_reader = csv.reader(StringIO(csv_content))
        all_rows = list(csv_reader)

        divider_index = None
        for i, row in enumerate(all_rows):
            if len(row) >= 3 and row[0] == 'Component ID' and row[1] == 'Required Flag':
                divider_index = i
                break

        if divider_index is None:
            raise ValueError(
                "CSV format invalid: Component section divider not found")

        main_body = {}
        for i in range(1, divider_index):
            row = all_rows[i]
            if len(row) >= 2 and row[0].strip():
                field_name = row[0].strip()
                value = row[1].strip() if len(row) > 1 else ''
                main_body[field_name] = value

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
        missing_fields = []
        for field in self.REQUIRED_MAIN_FIELDS:
            if field not in main_body or not main_body[field]:
                missing_fields.append(field)

        if missing_fields:
            error_msg = f"Missing required fields: {', '.join(missing_fields)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def validate_components(self, components: List[Dict]) -> None:
        if not components:
            raise ValueError("No components found in CSV")

        for idx, component in enumerate(components):
            for field in self.REQUIRED_COMPONENT_FIELDS:
                if field not in component or not component[field]:
                    error_msg = f"Component at row {idx + 1} missing required field: {field}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

    def normalize_data(self, main_body: Dict, components: List[Dict]) -> Tuple[Dict, List[Dict]]:
        normalized_main = {
            'Title_ID': main_body.get('Title ID', ''),
            'Title_Name': main_body.get('Title Name', ''),
            'Title_EIDR_ID': main_body.get('Title EIDR ID', ''),
            'Version_ID': main_body.get('Version ID', ''),
            'Version_Name': main_body.get('Version Name', ''),
            'Version_EIDR_ID': main_body.get('Version EIDR ID', ''),
            'Release_Year': main_body.get('Release Year', ''),
            'Licensee_ID': main_body.get('Licensee ID', ''),
            'DA_Description': main_body.get('DA Description', ''),
            'Due_Date': main_body.get('Due Date', ''),
            'Earliest_Delivery_Date': main_body.get('Earliest Delivery Date', ''),
            'License_Period_Start': main_body.get('License Period Start', ''),
            'License_Period_End': main_body.get('License Period End', ''),
            'Territories': main_body.get('Territories', ''),
            'Exception_Notification_Date': main_body.get('Exception Notification Date', ''),
            'Exception_Recipients': main_body.get('Exception Recipients', ''),
            'Internal_Studio_ID': main_body.get('Internal Studio ID', ''),
            'Studio_System_ID': main_body.get('Studio System ID', ''),
        }

        normalized_components = [
            {
                'Component_ID': comp['Component ID'],
                'Required_Flag': comp['Required Flag'].upper(),
                'Watermark_Required': comp['Watermark Required'].upper(),
            }
            for comp in components
        ]

        return normalized_main, normalized_components

    def validate_final_data(self, normalized_main: Dict) -> None:
        required_normalized_fields = {
            'Title_ID': 'Title ID',
            'Version_ID': 'Version ID',
            'Licensee_ID': 'Licensee ID',
            'Release_Year': 'Release Year',
            'License_Period_Start': 'License Period Start',
            'License_Period_End': 'License Period End'
        }

        missing_fields = []
        for field_key, field_name in required_normalized_fields.items():
            if not normalized_main.get(field_key):
                missing_fields.append(field_name)

        if missing_fields:
            error_msg = f"Required fields are empty after processing: {', '.join(missing_fields)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def process(self, csv_content: str) -> Dict:
        try:
            main_body, components = self.parse_csv(csv_content)

            self.validate_main_body(main_body)
            self.validate_components(components)

            normalized_main, normalized_components = self.normalize_data(
                main_body, components)

            studio_id = normalized_main.get(
                'Internal_Studio_ID') or settings.DEFAULT_STUDIO_ID
            normalized_main = self.default_service.apply_defaults(
                normalized_main,
                studio_id
            )

            self.validate_final_data(normalized_main)

            self.db_service.create_if_not_exists_title_info(normalized_main)

            da_result = self.db_service.create_da_record(normalized_main)
            record_id = da_result['ID']

            for component in normalized_components:
                self.db_service.create_component(
                    record_id, normalized_main['Title_ID'], normalized_main['Version_ID'], component)

            earliest_delivery_date = normalized_main.get('Earliest_Delivery_Date')
            if earliest_delivery_date:
                try:
                    schedule_arn = self.scheduler_service.create_manifest_schedule(
                        da_id=record_id,
                        earliest_delivery_date=earliest_delivery_date,
                        licensee_id=normalized_main['Licensee_ID']
                    )
                    logger.info(f"Manifest schedule created: {schedule_arn}")
                except Exception as e:
                    logger.error(f"Failed to create manifest schedule: {e}")

            logger.info(f"Successfully processed DA upload: ID={record_id}")

            return {
                'success': True,
                'id': record_id,
                'title_id': normalized_main['Title_ID'],
                'version_id': normalized_main['Version_ID'],
                'licensee_id': normalized_main['Licensee_ID'],
                'components_count': len(normalized_components)
            }

        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")
            self.send_exception_notification(
                str(e), main_body if 'main_body' in locals() else {})
            raise