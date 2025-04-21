# celery_worker.py
# Configuración de Celery para procesar las tareas en segundo plano

import os
from celery import Celery

# Usamos la misma variable de entorno REDIS_URL que provisionamos en Heroku
redis_url = os.environ.get('REDIS_URL')
if not redis_url:
    raise RuntimeError("La variable de entorno REDIS_URL no está configurada")

# Inicializamos la instancia de Celery
celery = Celery(
    'worker',
    broker=redis_url,
    backend=redis_url,
)

# Opcional: configuración de serialización
celery.conf.update(
    task_serializer='pickle',
    accept_content=['pickle'],
)

# Importamos la función process_excel desde tu app
# Evitamos circular imports moviendo esta línea después de la creación de "celery"
from app import process_excel

@celery.task(name='tasks.process_excel_task')
def process_excel_task(filepath):
    """
    Tarea asíncrona que ejecuta process_excel y devuelve sus resultados.
    """
    # process_excel devuelve: df_clean, preview_df, pozos_celestes
    df_clean, preview_df, pozos_celestes = process_excel(filepath)

    # Convertimos preview_df a HTML (string) para enviarlo como resultado
    preview_html = preview_df.to_html(classes="table table-striped", index=False)

    return {
        'status': 'completed',
        'preview': preview_html,
        'pozos_celestes': pozos_celestes
    }
