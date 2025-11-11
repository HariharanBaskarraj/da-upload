#!/bin/bash
set -e

# Check if this is a CSV processing task (triggered by Lambda)
if [ "$PROCESS_MODE" = "CSV" ]; then
    echo "Starting CSV processing mode..."
    echo "Processing file: $S3_FILE_KEY from bucket: $AWS_S3_BUCKET"
    
    # Run Django management command to process CSV
    python manage.py process_da --s3-key "$S3_FILE_KEY" --bucket "$AWS_S3_BUCKET"
    
    # Exit after processing
    exit_code=$?
    echo "CSV processing completed with exit code: $exit_code"
    exit $exit_code
else
    echo "Starting API server mode..."
    
    # Run migrations
    python manage.py migrate --noinput
    
    # Collect static files
    python manage.py collectstatic --noinput || true
    
    # Start Gunicorn
    exec gunicorn config.wsgi:application \
        --bind 0.0.0.0:8000 \
        --workers ${GUNICORN_WORKERS:-2} \
        --timeout ${GUNICORN_TIMEOUT:-120} \
        --access-logfile - \
        --error-logfile - \
        --log-level ${DJANGO_LOG_LEVEL:-info}
fi