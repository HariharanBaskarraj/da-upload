"""
Production-grade JSON structured logging utility.
"""

import logging
import os
from datetime import datetime
from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """
    JSON formatter with standard fields for CloudWatch/ELK/Datadog.
    
    Automatically adds:
    - timestamp (UTC ISO)
    - service_type (from SERVICE_TYPE env var)
    - module, function, line
    - log_level
    """
    
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        
        # Standard fields
        log_record['timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        log_record['service_type'] = os.environ.get('SERVICE_TYPE', 'UNKNOWN')
        log_record['module'] = record.module
        log_record['function'] = record.funcName
        log_record['line'] = record.lineno
        log_record['log_level'] = record.levelname
        
        # Event type (if provided)
        if hasattr(record, 'event_type'):
            log_record['event_type'] = record.event_type
        
        # DA-specific fields (if provided)
        for field in ['da_id', 'title_id', 'licensee_id', 'component_id', 
                      'aws_message_id', 's3_key', 'asset_id']:
            if hasattr(record, field):
                log_record[field] = getattr(record, field)
        
        # Exception info
        if record.exc_info:
            log_record['exception'] = self.formatException(record.exc_info)


def get_logger(name):
    """
    Get logger instance with JSON formatting.
    
    Usage:
        logger = get_logger(__name__)
        logger.info("Processing DA", extra={'event_type': 'PROCESS', 'da_id': 'abc123'})
    """
    return logging.getLogger(name)