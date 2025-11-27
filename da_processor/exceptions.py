"""
Custom exception classes for intelligent error handling.
"""


class DAProcessingError(Exception):
    """Base exception for DA processing."""
    pass


# ============================================================================
# RETRYABLE ERRORS - Leave in SQS queue for retry
# ============================================================================

class RetryableError(DAProcessingError):
    """
    Transient errors that should be retried.
    Examples: DynamoDB throttling, S3 eventual consistency, network issues
    """
    pass


class DynamoDBThrottlingError(RetryableError):
    """DynamoDB throttling - will succeed on retry."""
    pass


class AWSServiceError(RetryableError):
    """Temporary AWS service issue."""
    pass


# ============================================================================
# NON-RETRYABLE ERRORS - Delete from queue, log error
# ============================================================================

class NonRetryableError(DAProcessingError):
    """
    Permanent errors that won't succeed on retry.
    Examples: Invalid data, missing required fields, bad format
    """
    pass


class ValidationError(NonRetryableError):
    """Data validation failed."""
    pass


class CSVFormatError(NonRetryableError):
    """CSV format is invalid."""
    pass


class MissingRequiredFieldError(NonRetryableError):
    """Required field is missing."""
    pass