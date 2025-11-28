"""
Path normalization utilities for S3 operations.

This module provides utilities for normalizing S3 paths to ensure consistent
handling of forward slashes, backslashes, and trailing slashes across all
services that interact with S3 storage.
"""


def normalize_s3_path(path: str) -> str:
    """
    Normalize S3 paths by converting backslashes to forward slashes.

    This function ensures consistent path formatting across all S3 operations
    by standardizing on forward slashes while preserving trailing slashes
    when they exist in the original path.

    Args:
        path: The S3 path to normalize. Can contain backslashes or forward
              slashes, and may or may not have a trailing slash.

    Returns:
        Normalized path with forward slashes. Preserves trailing slash if
        present in the original path. Returns empty string if input is None/empty.

    Examples:
        >>> normalize_s3_path("folder\\subfolder\\")
        "folder/subfolder/"

        >>> normalize_s3_path("folder\\file.txt")
        "folder/file.txt"

        >>> normalize_s3_path("")
        ""

        >>> normalize_s3_path("/leading/and/trailing/")
        "leading/and/trailing/"
    """
    if not path:
        return ""

    # Check if original path had a trailing slash (or backslash)
    has_trailing_slash = path.endswith("\\") or path.endswith("/")

    # Replace backslashes with forward slashes and remove leading/trailing slashes
    path = path.replace("\\", "/").strip("/")

    # Add trailing slash only if original path had it
    if has_trailing_slash and path:  # Don't add slash to empty string
        path += "/"

    return path
