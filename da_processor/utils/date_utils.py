from datetime import datetime, timezone, timedelta
from dateutil import parser
import logging

logger = logging.getLogger(__name__)


def to_zulu(dt_str):
    """Convert any datetime string to Zulu time format (ISO 8601 with Z)"""
    if not dt_str:
        return None
    
    try:
        dt = parser.parse(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    except Exception as e:
        logger.error(f"Error parsing date '{dt_str}': {e}")
        return None


def get_current_zulu():
    """Get current datetime in Zulu format"""
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def parse_date(value):
    """Parse a date string and return datetime object in UTC"""
    if not value:
        return None
    
    try:
        dt = parser.parse(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception as e:
        logger.warning(f"Error parsing date '{value}': {e}")
        return None


def subtract_days(date_str, days):
    """Subtract days from a date string and return in Zulu format"""
    dt = parse_date(date_str)
    if dt and days > 0:
        result = dt - timedelta(days=days)
        return to_zulu(result.isoformat())
    return None