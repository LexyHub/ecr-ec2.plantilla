import os
import time
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

# Configuración Celery - FORZAR RABBITMQ
app = Celery('worker_test')
app.conf.update(
    broker_url='amqp://lexy_dev:2NtH4p7Qid@rabbitmq:5672/lexy_dev_vhost',  # ← FORZAR RabbitMQ
    result_backend='redis://:test123@redis:6379/0',  # ← Redis solo para resultados
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

@app.task
def process_sqs_message(message_data):
    """Procesa mensaje de SQS con wait de 10 segundos"""
    print(f"📨 Mensaje recibido: {message_data}")
    
    # Wait de 10 segundos---
    time.sleep(5)
    
    # Procesar datos
    result = {
        "original_data": message_data,
        "status": "procesado",
        "processed_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    print(f"✅ Procesamiento completado: {result}")
    return result

if __name__ == '__main__':
    app.start(['worker', '--loglevel=info'])