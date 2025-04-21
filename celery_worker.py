# celery_worker.py
import os
from celery import Celery

# 1) Obtenemos el URL de Redis, se llama REDIS_URL o REDISCLOUD_URL en tu Config Vars
redis_url = (
    os.environ.get('REDIS_URL')
    or os.environ.get('REDISCLOUD_URL')
)
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

# 3) Importamos process_excel de app.py
from app import process_excel

# 4) Decorador y definición de la tarea CON CERO espacios delante
@celery.task(name='tasks.process_excel_task')
def process_excel_task(filepath):
    """
    Tarea que llama a process_excel() y devuelve:
      - preview HTML
      - pozos_celestes
      - data_records (lista de dicts)
    """
    df_clean, preview_df, pozos_celestes = process_excel(filepath)

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
