import logging
from typing import Dict, Optional
from datetime import datetime, timedelta, timezone
from da_processor.utils.date_utils import to_zulu

logger = logging.getLogger(__name__)


class DefaultValuesService:

    def __init__(self, db_service):
        self.db_service = db_service

    def apply_defaults(self, da_data: Dict, studio_id: str) -> Dict:
        result = da_data.copy()

        studio_config = self.db_service.get_studio_config(studio_id) or {}

        due_date_window = int(float(studio_config.get("DueDateWindow", 15)))
        earliest_delivery = int(float(studio_config.get("EarliestDelivery", 0)))
        exception_notification = int(float(studio_config.get("ExceptionNotification", 7)))
        exception_recipients = studio_config.get("ExceptionRecipients", [])
        default_components = studio_config.get("DefaultComponents", [])

        result = self._apply_system_defaults(
            result,
            due_date_window,
            earliest_delivery,
            exception_notification,
            exception_recipients
        )

        return result

    def _parse_date(self, value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None

        try:
            from dateutil import parser as date_parser
            dt = date_parser.parse(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception as e:
            logger.warning(f"Error parsing date '{value}': {e}")
            return None

    def _apply_system_defaults(
        self,
        da_data: Dict,
        due_date_window: int,
        earliest_delivery: int,
        exception_notification: int,
        exception_recipients: list
    ) -> Dict:
        result = da_data.copy()

        if result.get("LicensePeriodStart"):
            result["LicensePeriodStart"] = to_zulu(result["LicensePeriodStart"])
        
        if result.get("LicensePeriodEnd"):
            result["LicensePeriodEnd"] = to_zulu(result["LicensePeriodEnd"])

        if not result.get("DueDate") and result.get("LicensePeriodStart"):
            license_start = self._parse_date(result["LicensePeriodStart"])
            if license_start:
                due_date = license_start - timedelta(days=due_date_window)
                result["DueDate"] = to_zulu(due_date.isoformat())
                logger.debug(f"Calculated Due Date: {result['DueDate']}")
        elif result.get("DueDate"):
            result["DueDate"] = to_zulu(result["DueDate"])

        if not result.get("EarliestDeliveryDate") and result.get("DueDate"):
            due_date = self._parse_date(result["DueDate"])
            if due_date:
                earliest_delivery_date = due_date - timedelta(days=earliest_delivery)
                result["EarliestDeliveryDate"] = to_zulu(earliest_delivery_date.isoformat())
                logger.debug(f"Calculated Earliest Delivery Date: {result['EarliestDeliveryDate']}")
        elif result.get("EarliestDeliveryDate"):
            result["EarliestDeliveryDate"] = to_zulu(result["EarliestDeliveryDate"])

        if not result.get("ExceptionNotificationDate") and result.get("DueDate"):
            due_date = self._parse_date(result["DueDate"])
            if due_date:
                exception_date = due_date - timedelta(days=exception_notification)
                result["ExceptionNotificationDate"] = to_zulu(exception_date.isoformat())
                logger.debug(f"Calculated Exception Notification Date: {result['ExceptionNotificationDate']}")
        elif result.get("ExceptionNotificationDate"):
            result["ExceptionNotificationDate"] = to_zulu(result["ExceptionNotificationDate"])

        if not result.get("ExceptionRecipients") and exception_recipients:
            result["ExceptionRecipients"] = ",".join(exception_recipients)
            logger.debug(f"Applied default exception recipients: {result['ExceptionRecipients']}")

        if not result.get("DADescription"):
            title_name = result.get("TitleName") or result.get("TitleID", "Unknown")
            version_name = result.get("VersionName") or result.get("VersionID", "")
            licensee_name = result.get("LicenseeID", "Unknown")
            territories = result.get("Territories", "")

            description = f"{title_name}"
            if version_name:
                description += f" - {version_name}"
            description += f" to {licensee_name}"
            if territories:
                description += f" in {territories}"

            result["DADescription"] = description
            logger.debug(f"Generated DA Description: {description}")

        return result