web:    gunicorn app:app --timeout 120 --workers 1
worker: celery -A celery_worker.celery worker --loglevel=info

