import logging
from typing import Dict, List
from django.conf import settings
from .base_processor import BaseDAProcessor
from da_processor.services.scheduler_service import SchedulerService

logger = logging.getLogger(__name__)


class JSONProcessor(BaseDAProcessor):

    REQUIRED_MAIN_FIELDS = ['Licensee ID', 'Title ID', 'Version ID', 'Release Year',
                            'License Period Start', 'License Period End']

    def __init__(self):
        super().__init__()
        self.scheduler_service = SchedulerService()

    def validate_payload(self, payload: Dict) -> None:
        logger.debug("[PROCESSOR] Validating payload structure")
        if 'main_body_attributes' not in payload:
            raise ValueError("Missing 'main_body_attributes' in payload")

        if 'components' not in payload:
            raise ValueError("Missing 'components' in payload")

        if not isinstance(payload['components'], list):
            raise ValueError("'components' must be a list")

    def validate_main_body(self, main_body: Dict) -> None:
        logger.debug("[PROCESSOR] Validating main body attributes")
        missing_fields = []
        for field in self.REQUIRED_MAIN_FIELDS:
            if field not in main_body or not main_body[field]:
                missing_fields.append(field)

        if missing_fields:
            error_msg = f"Missing required fields: {', '.join(missing_fields)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def validate_components(self, components: List[Dict]) -> None:
        logger.debug(f"[PROCESSOR] Validating {len(components)} components")
        if not components:
            raise ValueError("No components found in payload")

        for idx, component in enumerate(components):
            if 'Component ID' not in component or not component['Component ID']:
                error_msg = f"Component at index {idx} missing 'Component ID'"
                logger.error(error_msg)
                raise ValueError(error_msg)

    def extract_values(self, main_body_attrs: Dict) -> Dict:
        logger.debug("[PROCESSOR] Extracting main_body_attributes values")
        extracted = {}
        for key, data in main_body_attrs.items():
            if isinstance(data, dict):
                extracted[key] = data.get('Value', '')
            else:
                extracted[key] = data
        logger.debug(f"[PROCESSOR] Extracted values: {extracted}")
        return extracted

    def normalize_data(self, main_body_values: Dict, components: List[Dict]) -> tuple:
        logger.debug("[PROCESSOR] Normalizing data for DB storage")
        normalized_main = {
            'Title_ID': main_body_values.get('Title ID', ''),
            'Title_Name': main_body_values.get('Title Name', ''),
            'Title_EIDR_ID': main_body_values.get('Title EIDR ID', ''),
            'Version_ID': main_body_values.get('Version ID', ''),
            'Version_Name': main_body_values.get('Version Name', ''),
            'Version_EIDR_ID': main_body_values.get('Version EIDR ID', ''),
            'Release_Year': main_body_values.get('Release Year', ''),
            'Licensee_ID': main_body_values.get('Licensee ID', ''),
            'DA_Description': main_body_values.get('DA Description', ''),
            'Due_Date': main_body_values.get('Due Date', ''),
            'Earliest_Delivery_Date': main_body_values.get('Earliest Delivery Date', ''),
            'License_Period_Start': main_body_values.get('License Period Start', ''),
            'License_Period_End': main_body_values.get('License Period End', ''),
            'Territories': main_body_values.get('Territories', ''),
            'Exception_Notification_Date': main_body_values.get('Exception Notification Date', ''),
            'Exception_Recipients': main_body_values.get('Exception Recipients', ''),
            'Internal_Studio_ID': main_body_values.get('Internal Studio ID', ''),
            'Studio_System_ID': main_body_values.get('Studio System ID', ''),
        }

        normalized_components = [
            {
                'Component_ID': comp.get('Component ID', ''),
                'Required_Flag': comp.get('Required Flag', 'FALSE').upper(),
                'Watermark_Required': comp.get('Watermark Required', 'FALSE').upper(),
            }
            for comp in components
        ]

        logger.debug(f"[PROCESSOR] Normalized main body: {normalized_main}")
        logger.debug(f"[PROCESSOR] Normalized {len(normalized_components)} components")
        return normalized_main, normalized_components

    def validate_final_data(self, normalized_main: Dict) -> None:
        logger.debug("[PROCESSOR] Validating final normalized data before DB insert")
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

    def process(self, payload: Dict) -> Dict:
        try:
            logger.info("[PROCESSOR] Starting DA JSON processing")
            self.validate_payload(payload)

            main_body_values = self.extract_values(payload['main_body_attributes'])
            components = payload['components']

            self.validate_main_body(main_body_values)
            self.validate_components(components)

            normalized_main, normalized_components = self.normalize_data(main_body_values, components)
            logger.debug(f"[PROCESSOR] Normalized main before defaults: {normalized_main}")

            studio_id = normalized_main.get('Internal_Studio_ID') or settings.DEFAULT_STUDIO_ID
            logger.debug(f"[PROCESSOR] Applying defaults for Studio_ID={studio_id}")

            normalized_main = self.default_service.apply_defaults(normalized_main, studio_id)
            logger.debug(f"[PROCESSOR] Normalized main AFTER defaults applied: {normalized_main}")

            self.validate_final_data(normalized_main)

            self.db_service.create_if_not_exists_title_info(normalized_main)

            logger.debug(f"[PROCESSOR] Writing DA record to DB: {normalized_main}")
            da_result = self.db_service.create_da_record(normalized_main)
            logger.debug(f"[PROCESSOR] DB response for DA record creation: {da_result}")

            record_id = da_result['ID']

            for component in normalized_components:
                logger.debug(f"[PROCESSOR] Creating component record for DA ID={record_id}: {component}")
                self.db_service.create_component(record_id, normalized_main['Title_ID'], component)

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
            logger.error(f"Error processing JSON: {str(e)}")
            main_body_values = self.extract_values(payload.get('main_body_attributes', {})) if 'payload' in locals() else {}
            self.send_exception_notification(str(e), main_body_values)
            raise