"""
Django REST Framework exception handler.
Maps exceptions to HTTP status codes with clean error messages.
"""

from rest_framework.views import exception_handler
from rest_framework.response import Response
from rest_framework import status
from da_processor.exceptions import ValidationError, NonRetryableError, RetryableError
from da_processor.utils.logging_utils import get_logger

logger = get_logger(__name__)


def custom_exception_handler(exc, context):
    """
    Custom DRF exception handler.
    
    - Maps custom exceptions to appropriate HTTP status codes
    - Sanitizes error messages (no internal details)
    - Logs all errors with context
    """
    # Call DRF's default handler first
    response = exception_handler(exc, context)
    
    if response is not None:
        # DRF handled it, just log and return
        request = context.get('request')
        logger.error(
            f"API error: {str(exc)}",
            extra={
                'event_type': 'ERROR',
                'status_code': response.status_code,
                'path': request.path if request else None,
                'method': request.method if request else None
            },
            exc_info=True
        )
        return response
    
    # Handle custom exceptions
    request = context.get('request')
    
    if isinstance(exc, ValidationError):
        status_code = status.HTTP_400_BAD_REQUEST
        error_message = str(exc)
    elif isinstance(exc, NonRetryableError):
        status_code = status.HTTP_400_BAD_REQUEST
        error_message = str(exc)
    elif isinstance(exc, RetryableError):
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        error_message = "Service temporarily unavailable. Please try again."
    elif isinstance(exc, ValueError):
        status_code = status.HTTP_400_BAD_REQUEST
        error_message = str(exc)
    else:
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        error_message = "An unexpected error occurred."
    
    # Log the error
    logger.error(
        f"API error: {str(exc)}",
        extra={
            'event_type': 'ERROR',
            'status_code': status_code,
            'path': request.path if request else None,
            'method': request.method if request else None,
            'error_type': type(exc).__name__
        },
        exc_info=True
    )
    
    return Response({'error': error_message}, status=status_code)