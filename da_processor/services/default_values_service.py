import logging
from typing import Dict, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DefaultValuesService:
    """
    Service to apply licensee-specific and system default values
    for Delivery Authorization (DA) data.
    """

    def __init__(self, db_service):
        """
        :param db_service: Instance of DynamoDBService
        """
        self.db_service = db_service

    def apply_defaults(self, da_data: Dict, licensee_id: str) -> Dict:
        """Apply licensee-specific and system defaults to DA data."""
        result = da_data.copy()

        # Fetch licensee-specific defaults from DynamoDB
        licensee_defaults = self.db_service.get_licensee_defaults(licensee_id) or {}

        # Fallbacks if the record is missing or incomplete
        delivery_window_days = licensee_defaults.get("DefaultDeliveryWindowDays", 7)
        exception_notification_days = licensee_defaults.get("ExceptionNotificationDays", 2)
        exception_recipients = licensee_defaults.get("DefaultExceptionRecipients", [])

        delivery_window_days = int(float(delivery_window_days))
        exception_notification_days = int(float(exception_notification_days))

        # Apply computed defaults
        result = self._apply_system_defaults(
            result,
            delivery_window_days,
            exception_notification_days,
            exception_recipients
        )

        return result

    # -------------------- Helper: Date Parsing -------------------- #
    def _parse_date(self, value: Optional[str]) -> Optional[datetime]:
        """
        Parse a date string safely.
        Supports:
        - ISO-8601: 2025-06-15T07:00:00
        - MM/DD/YYYY HH:MM:SS (optionally with UTC)
        - DD-MM-YYYY HH:MM (used in some CSVs)
        Returns None if value is empty or can't be parsed.
        """
        if not value:
            return None

        v = value.strip().replace(" UTC", "").replace("Z", "")
        try:
            # Try ISO format first
            return datetime.fromisoformat(v)
        except ValueError:
            for fmt in ("%m/%d/%Y %H:%M:%S", "%d-%m-%Y %H:%M"):
                try:
                    return datetime.strptime(v, fmt)
                except ValueError:
                    continue
            logger.warning(f"Unrecognized date format: {value}")
            return None

    # -------------------- Core Default Logic -------------------- #
    def _apply_system_defaults(
        self,
        da_data: Dict,
        delivery_window_days: int,
        exception_notification_days: int,
        exception_recipients: list
    ) -> Dict:
        """Apply system-level defaults based on licensee configuration."""
        result = da_data.copy()

        # 1️⃣ Due Date
        if not result.get("DueDate") and result.get("LicensePeriodStart"):
            license_start = self._parse_date(result["LicensePeriodStart"])
            if license_start:
                due_date = license_start - timedelta(days=delivery_window_days)
                result["DueDate"] = due_date.strftime("%m/%d/%Y %H:%M:%S UTC")
                logger.debug(f"Calculated Due Date: {result['DueDate']}")
        elif result.get("DueDate"):
            parsed_due = self._parse_date(result["DueDate"])
            if parsed_due:
                result["DueDate"] = parsed_due.strftime("%m/%d/%Y %H:%M:%S UTC")

        # 2️⃣ Earliest Delivery Date
        if not result.get("EarliestDeliveryDate") and result.get("DueDate"):
            due_date = self._parse_date(result["DueDate"])
            if due_date:
                earliest_delivery = due_date - timedelta(days=delivery_window_days)
                result["EarliestDeliveryDate"] = earliest_delivery.strftime("%m/%d/%Y %H:%M:%S UTC")
                logger.debug(f"Calculated Earliest Delivery Date: {result['EarliestDeliveryDate']}")

        # 3️⃣ Exception Notification Date
        if not result.get("ExceptionNotificationDate") and result.get("DueDate"):
            due_date = self._parse_date(result["DueDate"])
            if due_date:
                exception_date = due_date - timedelta(days=exception_notification_days)
                result["ExceptionNotificationDate"] = exception_date.strftime("%m/%d/%Y %H:%M:%S UTC")
                logger.debug(f"Calculated Exception Notification Date: {result['ExceptionNotificationDate']}")

        # 4️⃣ Exception Recipients
        if not result.get("ExceptionRecipients") and exception_recipients:
            result["ExceptionRecipients"] = ",".join(exception_recipients)
            logger.debug(f"Applied default exception recipients: {result['ExceptionRecipients']}")

        # 5️⃣ DA Description
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
