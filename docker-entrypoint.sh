#!/bin/bash
set -e

python manage.py migrate --noinput
python manage.py collectstatic --noinput || true

if [ "$SERVICE_TYPE" = "API" ]; then
    echo "Starting API server..."
    exec gunicorn config.wsgi:application \
        --bind 0.0.0.0:8000 \
        --workers ${GUNICORN_WORKERS:-2} \
        --timeout ${GUNICORN_TIMEOUT:-120} \
        --access-logfile - \
        --error-logfile - \
        --log-level ${DJANGO_LOG_LEVEL:-info}

elif [ "$SERVICE_TYPE" = "CSV_WORKER" ]; then
    echo "Starting CSV processing worker..."
    exec python manage.py csv_worker

elif [ "$SERVICE_TYPE" = "MANIFEST_WORKER" ]; then
    echo "Starting manifest generation worker..."
    exec python manage.py manifest_worker

elif [ "$SERVICE_TYPE" = "DELIVERY_WORKER" ]; then
    echo "Starting delivery tracking worker..."
    exec python manage.py delivery_worker

elif [ "$SERVICE_TYPE" = "EXCEPTION_WORKER" ]; then
    echo "Starting exception notification worker..."
    exec python manage.py exception_worker

else
    echo "ERROR: SERVICE_TYPE environment variable not set or invalid"
    echo "Valid values: API, CSV_WORKER, MANIFEST_WORKER, DELIVERY_WORKER, EXCEPTION_WORKER"
    exit 1
fi