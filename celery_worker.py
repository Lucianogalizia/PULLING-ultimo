# celery_worker.py

import os
from celery import Celery

# 1) Obtenemos el URL de Redis, puede venir en REDIS_URL o REDISCLOUD_URL
redis_url = os.environ.get('REDIS_URL') or os.environ.get('REDISCLOUD_URL')
if not redis_url:
    raise RuntimeError("Ni REDIS_URL ni REDISCLOUD_URL están configuradas")

# 2) Inicializamos Celery
celery = Celery(
    'worker',
    broker=redis_url,
    backend=redis_url,
)
celery.conf.update(
    task_serializer='pickle',
    accept_content=['pickle'],
)

# 3) Importamos process_excel desde app.py
from app import process_excel

# 4) Definimos la tarea SIN espacios ni tabs antes del @
@celery.task(name='tasks.process_excel_task')
def process_excel_task(filepath):
    """
    Ejecuta process_excel() y devuelve:
      - status
      - preview (HTML)
      - pozos_celestes
      - data_records (lista de dicts)
    """
    # Llamada a la función pesada
    df_clean, preview_df, pozos_celestes = process_excel(filepath)

    # Construimos el resultado serializable
    preview_html = preview_df.to_html(classes="table table-striped", index=False)
    data_records = df_clean.to_dict(orient='records')

    return {
        'status': 'completed',
        'preview': preview_html,
        'pozos_celestes': pozos_celestes,
        'data_records': data_records,
    }


         'status': 'completed',
         'preview': preview_html,
         'pozos_celestes': pozos_celestes,
+        'data_records': data_records,
     }
