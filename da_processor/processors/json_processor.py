import logging
from typing import Dict, List
from .base_processor import BaseDAProcessor

logger = logging.getLogger(__name__)


class JSONProcessor(BaseDAProcessor):
    """Process DA from JSON format"""

    REQUIRED_MAIN_FIELDS = ['Licensee ID', 'Title ID', 'Version ID', 'Release Year',
                            'License Period Start', 'License Period End']

    def validate_payload(self, payload: Dict) -> None:
        """Validate JSON payload structure"""
        if 'main_body_attributes' not in payload:
            raise ValueError("Missing 'main_body_attributes' in payload")

        if 'components' not in payload:
            raise ValueError("Missing 'components' in payload")

        if not isinstance(payload['components'], list):
            raise ValueError("'components' must be a list")

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
        """Validate components"""
        if not components:
            raise ValueError("No components found in payload")

        for idx, component in enumerate(components):
            if 'Component ID' not in component or not component['Component ID']:
                error_msg = f"Component at index {idx} missing 'Component ID'"
                logger.error(error_msg)
                raise ValueError(error_msg)

    def extract_values(self, main_body_attrs: Dict) -> Dict:
        """Extract values from main body attributes"""
        extracted = {}
        for key, data in main_body_attrs.items():
            if isinstance(data, dict):
                extracted[key] = data.get('Value', '')
            else:
                extracted[key] = data
        return extracted

    def normalize_data(self, main_body_values: Dict, components: List[Dict]) -> tuple:
        """Normalize field names to match database schema"""
        normalized_main = {
            'TitleID': main_body_values.get('Title ID', ''),
            'TitleName': main_body_values.get('Title Name', ''),
            'TitleEIDRID': main_body_values.get('Title EIDR ID', ''),
            'VersionID': main_body_values.get('Version ID', ''),
            'VersionName': main_body_values.get('Version Name', ''),
            'VersionEIDRID': main_body_values.get('Version EIDR ID', ''),
            'ReleaseYear': main_body_values.get('Release Year', ''),
            'LicenseeID': main_body_values.get('Licensee ID', ''),
            'DADescription': main_body_values.get('DA Description', ''),
            'DueDate': main_body_values.get('Due Date', ''),
            'EarliestDeliveryDate': main_body_values.get('Earliest Delivery Date', ''),
            'LicensePeriodStart': main_body_values.get('License Period Start', ''),
            'LicensePeriodEnd': main_body_values.get('License Period End', ''),
            'Territories': main_body_values.get('Territories', ''),
            'ExceptionNotificationDate': main_body_values.get('Exception Notification Date', ''),
            'ExceptionRecipients': main_body_values.get('Exception Recipients', ''),
            'InternalStudioID': main_body_values.get('Internal Studio ID', ''),
            'StudioSystemID': main_body_values.get('Studio System ID', ''),
        }

        normalized_components = [
            {
                'ComponentID': comp.get('Component ID', ''),
                'RequiredFlag': comp.get('Required Flag', 'FALSE').upper(),
                'WatermarkRequired': comp.get('Watermark Required', 'FALSE').upper(),
            }
            for comp in components
        ]

        return normalized_main, normalized_components

    def process(self, payload: Dict) -> Dict:
        """Main processing method"""
        try:
            # Validate structure
            self.validate_payload(payload)

            # Extract values
            main_body_values = self.extract_values(
                payload['main_body_attributes'])
            components = payload['components']

            # Validate
            self.validate_main_body(main_body_values)
            self.validate_components(components)

            # Normalize
            normalized_main, normalized_components = self.normalize_data(
                main_body_values, components)

            # Apply defaults
            normalized_main = self.default_service.apply_defaults(
                normalized_main,
                normalized_main['LicenseeID']
            )

            # Store in DynamoDB
            title_result = self.db_service.create_or_update_title(
                normalized_main)
            record_id = title_result['ID']

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
            logger.error(f"Error processing JSON: {str(e)}")
            main_body_values = self.extract_values(payload.get(
                'main_body_attributes', {})) if 'payload' in locals() else {}
            self.send_exception_notification(str(e), main_body_values)
            raise
