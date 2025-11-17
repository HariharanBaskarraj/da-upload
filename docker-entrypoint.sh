#!/bin/bash
set -e

# Check if this is a CSV processing task (triggered by Lambda)
if [ "$PROCESS_MODE" = "CSV" ]; then
    echo "Starting CSV processing mode..."
    echo "Processing file: $S3_FILE_KEY from bucket: $AWS_DA_BUCKET"
    
    # Run Django management command to process CSV
    python manage.py process_da --s3-key "$S3_FILE_KEY" --bucket "$AWS_DA_BUCKET"
    
    # Exit after processing
    exit_code=$?
    echo "CSV processing completed with exit code: $exit_code"
    exit $exit_code

# Check if this is a manifest generation task (triggered by EventBridge)
elif [ "$PROCESS_MODE" = "MANIFEST" ]; then
    echo "Starting manifest generation mode..."
    echo "Generating manifest for DA: $DA_ID, Licensee: $LICENSEE_ID"
    
    # Run Django management command to generate manifest
    python manage.py generate_manifest --da-id "$DA_ID" --licensee-id "$LICENSEE_ID"
    
    # Exit after processing
    exit_code=$?
    echo "Manifest generation completed with exit code: $exit_code"
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