import logging
from typing import Dict
from da_processor.utils.date_utils import to_zulu, subtract_days

logger = logging.getLogger(__name__)


class DefaultValuesService:

    def __init__(self, db_service):
        self.db_service = db_service

    def apply_defaults(self, da_data: Dict, studio_id: str) -> Dict:
        result = da_data.copy()

        studio_config = self.db_service.get_studio_config(studio_id) or {}

        due_date_window = int(float(studio_config.get("DueDateWindow", 0)))
        earliest_delivery = int(float(studio_config.get("EarliestDelivery", 0)))
        exception_notification = int(float(studio_config.get("ExceptionNotification", 0)))
        exception_recipients = studio_config.get("ExceptionRecipients", [])

        result = self._apply_system_defaults(
            result,
            due_date_window,
            earliest_delivery,
            exception_notification,
            exception_recipients
        )

        return result

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

        if not result.get("DueDate"):
            if result.get("LicensePeriodStart") and due_date_window > 0:
                calculated_due_date = subtract_days(result["LicensePeriodStart"], due_date_window)
                if calculated_due_date:
                    result["DueDate"] = calculated_due_date
                    logger.debug(f"Calculated Due Date from LicensePeriodStart - {due_date_window} days: {result['DueDate']}")
        else:
            result["DueDate"] = to_zulu(result["DueDate"])
            logger.debug(f"Due Date provided in DA payload: {result['DueDate']}")

        if not result.get("EarliestDeliveryDate"):
            if result.get("DueDate") and earliest_delivery > 0:
                calculated_earliest = subtract_days(result["DueDate"], earliest_delivery)
                if calculated_earliest:
                    result["EarliestDeliveryDate"] = calculated_earliest
                    logger.debug(f"Calculated Earliest Delivery Date from DueDate - {earliest_delivery} days: {result['EarliestDeliveryDate']}")
        else:
            result["EarliestDeliveryDate"] = to_zulu(result["EarliestDeliveryDate"])
            logger.debug(f"Earliest Delivery Date provided in DA payload: {result['EarliestDeliveryDate']}")

        if not result.get("ExceptionNotificationDate"):
            if result.get("DueDate") and exception_notification > 0:
                calculated_exception = subtract_days(result["DueDate"], exception_notification)
                if calculated_exception:
                    result["ExceptionNotificationDate"] = calculated_exception
                    logger.debug(f"Calculated Exception Notification Date from DueDate - {exception_notification} days: {result['ExceptionNotificationDate']}")
        else:
            result["ExceptionNotificationDate"] = to_zulu(result["ExceptionNotificationDate"])
            logger.debug(f"Exception Notification Date provided in DA payload: {result['ExceptionNotificationDate']}")

        if not result.get("ExceptionRecipients") and exception_recipients:
            result["ExceptionRecipients"] = ",".join(exception_recipients)
            logger.debug(f"Applied default exception recipients from studio config: {result['ExceptionRecipients']}")
        else:
            logger.debug(f"Exception Recipients provided in DA payload or no default available")

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