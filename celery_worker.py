import os
from celery import Celery

# Obtener la URL de Redis desde las variables de entorno
redis_url = os.getenv('REDIS_URL') or os.getenv('REDISCLOUD_URL')
if not redis_url:
    raise RuntimeError("Ni REDIS_URL ni REDISCLOUD_URL están configuradas")

# Inicializar Celery
celery = Celery('worker', broker=redis_url, backend=redis_url)
celery.conf.update(
    task_serializer='pickle',
    accept_content=['pickle'],
)

# Importar la función pesada desde app.py
from app import process_excel

# Definir la tarea de manera síncrona (sin espacios extra antes del @)
@celery.task(name='tasks.process_excel_task')
def process_excel_task(filepath):
    # Ejecutar la función principal
    df_clean, preview_df, pozos_celestes = process_excel(filepath)

    # Convertir preview a HTML
    preview_html = preview_df.to_html(classes="table table-striped", index=False)
    # Serializar el DataFrame completo
    data_records = df_clean.to_dict(orient='records')

    # Devolver un dict serializable
    return {
        'status': 'completed',
        'preview': preview_html,
        'pozos_celestes': pozos_celestes,
        'data_records': data_records
    }



         'status': 'completed',
         'preview': preview_html,
         'pozos_celestes': pozos_celestes,
+        'data_records': data_records,
     }
