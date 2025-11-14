import logging
from typing import Dict
from da_processor.utils.date_utils import to_zulu, subtract_days

logger = logging.getLogger(__name__)


class DefaultValuesService:

    def __init__(self, db_service):
        self.db_service = db_service

    def apply_defaults(self, da_data: Dict, studio_id: str = None) -> Dict:
        result = da_data.copy()

        studio_config = self.db_service.get_studio_config() or {}

        due_date_window = int(float(studio_config.get("Due_Date_Window", 0)))
        earliest_delivery = int(float(studio_config.get("Earliest_Delivery", 0)))
        exception_notification = int(float(studio_config.get("Exception_Notification", 0)))
        exception_recipients = studio_config.get("Exception_Recipients", [])

        logger.debug(
            f"[DEFAULTS] Studio config → DueDateWindow={due_date_window}, "
            f"EarliestDelivery={earliest_delivery}, ExceptionNotification={exception_notification}"
        )

        result = self._apply_system_defaults(
            result,
            due_date_window,
            earliest_delivery,
            exception_notification,
            exception_recipients
        )

        logger.debug(f"[DEFAULTS] Final DA data: {result}")
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

        if result.get("License_Period_Start"):
            converted = to_zulu(result["License_Period_Start"])
            if converted is None:
                error_msg = f"Invalid License Period Start date: {result['License_Period_Start']}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            result["License_Period_Start"] = converted

        if result.get("License_Period_End"):
            converted = to_zulu(result["License_Period_End"])
            if converted is None:
                error_msg = f"Invalid License Period End date: {result['License_Period_End']}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            result["License_Period_End"] = converted

        if not result.get("Due_Date"):
            if result.get("License_Period_Start") and due_date_window > 0:
                calculated_due_date = subtract_days(result["License_Period_Start"], due_date_window)
                if calculated_due_date:
                    result["Due_Date"] = calculated_due_date
                    logger.debug(
                        f"[DEFAULTS] Set Due_Date = License_Period_Start - {due_date_window} days → {result['Due_Date']}"
                    )
        else:
            converted = to_zulu(result["Due_Date"])
            if converted is None:
                error_msg = f"Invalid Due Date: {result['Due_Date']}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            result["Due_Date"] = converted

        if not result.get("Earliest_Delivery_Date") and result.get("Due_Date"):
            if earliest_delivery > 0:
                calculated_earliest = subtract_days(result["Due_Date"], earliest_delivery)
                if calculated_earliest:
                    result["Earliest_Delivery_Date"] = calculated_earliest
                    logger.debug(
                        f"[DEFAULTS] Set Earliest_Delivery_Date = Due_Date - {earliest_delivery} days → {result['Earliest_Delivery_Date']}"
                    )
            else:
                result["Earliest_Delivery_Date"] = result["Due_Date"]
                logger.debug(
                    f"[DEFAULTS] EarliestDelivery=0 → Earliest_Delivery_Date = Due_Date ({result['Earliest_Delivery_Date']})"
                )
        elif result.get("Earliest_Delivery_Date"):
            converted = to_zulu(result["Earliest_Delivery_Date"])
            if converted is None:
                error_msg = f"Invalid Earliest Delivery Date: {result['Earliest_Delivery_Date']}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            result["Earliest_Delivery_Date"] = converted

        if not result.get("Exception_Notification_Date") and result.get("Due_Date") and exception_notification > 0:
            calculated_exception = subtract_days(result["Due_Date"], exception_notification)
            if calculated_exception:
                result["Exception_Notification_Date"] = calculated_exception
                logger.debug(
                    f"[DEFAULTS] Set Exception_Notification_Date = Due_Date - {exception_notification} days → {result['Exception_Notification_Date']}"
                )
        elif result.get("Exception_Notification_Date"):
            converted = to_zulu(result["Exception_Notification_Date"])
            if converted is None:
                error_msg = f"Invalid Exception Notification Date: {result['Exception_Notification_Date']}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            result["Exception_Notification_Date"] = converted

        if not result.get("Exception_Recipients") and exception_recipients:
            result["Exception_Recipients"] = ",".join(exception_recipients)
            logger.debug("[DEFAULTS] Applied default Exception_Recipients from studio config")

        if not result.get("DA_Description"):
            title_name = result.get("Title_Name") or result.get("Title_ID", "Unknown")
            version_name = result.get("Version_Name") or result.get("Version_ID", "")
            licensee_name = result.get("Licensee_ID", "Unknown")
            territories = result.get("Territories", "")

            description = f"{title_name}"
            if version_name:
                description += f" - {version_name}"
            description += f" to {licensee_name}"
            if territories:
                description += f" in {territories}"

            result["DA_Description"] = description
            logger.debug(f"[DEFAULTS] Generated DA_Description: {description}")

        return result