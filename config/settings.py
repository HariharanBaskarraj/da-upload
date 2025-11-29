import os
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get('DJANGO_SECRET_KEY')

DEBUG = os.environ.get('DJANGO_DEBUG', 'False') == 'True'

ALLOWED_HOSTS = os.environ.get('DJANGO_ALLOWED_HOSTS', '*').split(',')

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'da_processor',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'config.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'config.wsgi.application'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True

STATIC_URL = 'static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': os.environ.get('DJANGO_LOG_LEVEL', 'INFO'),
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': os.environ.get('DJANGO_LOG_LEVEL', 'INFO'),
            'propagate': False,
        },
        'da_processor': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}

AWS_REGION = os.environ.get('AWS_REGION')
AWS_DA_BUCKET = os.environ.get('AWS_DA_BUCKET')
AWS_SQS_QUEUE_URL = os.environ.get('AWS_SQS_QUEUE_URL')
AWS_ASSET_REPO_BUCKET = os.environ.get('AWS_ASSET_REPO_BUCKET', 'routerunner-poc-asset-repo')
AWS_WATERMARKED_BUCKET = os.environ.get('AWS_WATERMARKED_BUCKET', 'routerunner-poc-watermarkcache') ##Added newly
AWS_LICENSEE_BUCKET = os.environ.get('AWS_LICENSEE_BUCKET','routerunner-poc-licenseecache') ##Added newly
WATERMARK_JOB_TABLE = os.environ.get('WATERMARK_JOB_TABLE','routerunner-poc-watermark-assets') ##Added newly
# API Configuration
WATERMARKING_API_URL = os.environ.get('WATERMARKING_API_URL', 'https://api.example.com') ##Added newly
WATERMARKING_API_BEARER_TOKEN = os.environ.get('WATERMARKING_API_BEARER_TOKEN') ##Added newly
WATERMARK_PRESET_ID = os.environ.get('WATERMARK_PRESET_ID','019a97649ff37ec1a71193dfe213b933') ##Added newly

DYNAMODB_DA_TABLE = os.environ.get('DYNAMODB_DA_TABLE')
DYNAMODB_TITLE_TABLE = os.environ.get('DYNAMODB_TITLE_TABLE')
DYNAMODB_COMPONENT_TABLE = os.environ.get('DYNAMODB_COMPONENT_TABLE')
DYNAMODB_STUDIO_CONFIG_TABLE = os.environ.get('DYNAMODB_STUDIO_CONFIG_TABLE')
DYNAMODB_LICENSEE_TABLE = os.environ.get('DYNAMODB_LICENSEE_TABLE', 'routerunner-poc-licensee-info')
DYNAMODB_ASSET_TABLE = os.environ.get('DYNAMODB_ASSET_TABLE', 'routerunner-poc-asset-info')
DYNAMODB_COMPONENT_CONFIG_TABLE = os.environ.get('DYNAMODB_COMPONENT_CONFIG_TABLE', 'routerunner-poc-component-configs')
DYNAMODB_FILE_DELIVERY_TABLE = os.environ.get('DYNAMODB_FILE_DELIVERY_TABLE', 'routerunner-poc-file-delivery-tracker')

# Asset Ingestion Configuration
INGEST_S3_BUCKET = os.environ.get('INGEST_S3_BUCKET', 'routerunner-poc-ingest')
INGEST_ASSET_TABLE = os.environ.get('INGEST_ASSET_TABLE', 'routerunner-poc-ingest-assets')
TITLE_INFO_TABLE = os.environ.get('TITLE_INFO_TABLE', 'routerunner-poc-title-info')
ASSET_INFO_TABLE = os.environ.get('ASSET_INFO_TABLE', 'routerunner-poc-asset-info')
ASSET_VALIDATION_CUTOFF_MINUTES = int(os.environ.get('ASSET_VALIDATION_CUTOFF_MINUTES', '1'))

AWS_SQS_PRIMEVIDEO_QUEUE_URL = os.environ.get('AWS_SQS_PRIMEVIDEO_QUEUE_URL')
AWS_SQS_DLQ_URL = os.environ.get('AWS_SQS_DLQ_URL')
AWS_SQS_CSV_QUEUE_URL = os.environ.get('AWS_SQS_CSV_QUEUE_URL')
AWS_SQS_EXCEPTION_QUEUE_URL = os.environ.get('AWS_SQS_EXCEPTION_QUEUE_URL')
AWS_SQS_MANIFEST_QUEUE_URL = os.environ.get('AWS_SQS_MANIFEST_QUEUE_URL')
AWS_SQS_DELIVERY_QUEUE_URL = os.environ.get('AWS_SQS_DELIVERY_QUEUE_URL')

EVENTBRIDGE_SCHEDULER_ROLE_ARN = os.environ.get('EVENTBRIDGE_SCHEDULER_ROLE_ARN')
LAMBDA_MANIFEST_GENERATOR_ARN = os.environ.get('LAMBDA_MANIFEST_GENERATOR_ARN')
LAMBDA_EXCEPTION_NOTIFIER_ARN = os.environ.get('LAMBDA_EXCEPTION_NOTIFIER_ARN')

DEFAULT_EXCEPTION_RECIPIENTS = os.environ.get('DEFAULT_EXCEPTION_RECIPIENTS', '').split(',')
DEFAULT_STUDIO_ID = os.environ.get('DEFAULT_STUDIO_ID', '1234')
MANIFEST_CHECK_INTERVAL = int(os.environ.get('MANIFEST_CHECK_INTERVAL', '1800'))

SES_FROM_EMAIL=os.environ.get('SES_FROM_EMAIL')

REST_FRAMEWORK = {
    'DEFAULT_PARSER_CLASSES': [
        'rest_framework.parsers.JSONParser',
        'rest_framework.parsers.MultiPartParser',
        'rest_framework.parsers.FormParser',
    ],
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
    ],
}