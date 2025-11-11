from django.apps import AppConfig

class DaProcessorConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'da_processor'
    verbose_name = 'Distribution Authorization Processor'