"""
Django application configuration for Distribution Authorization Processor.

This module defines the Django app configuration for the DA processor application.
"""
from django.apps import AppConfig


class DaProcessorConfig(AppConfig):
    """
    Configuration class for the DA Processor Django application.

    Attributes:
        default_auto_field: Default primary key field type
        name: Application name
        verbose_name: Human-readable application name
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'da_processor'
    verbose_name = 'Distribution Authorization Processor'