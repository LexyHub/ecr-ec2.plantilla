import os
import time
import boto3
from dotenv import load_dotenv

# Importa la instancia de Celery y las tareas
from worker_test import app as celery_app
from worker_test import process_sqs_message  # importa tu tarea aquí

load_dotenv()

sqs = boto3.client(
    'sqs',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
)

queue_url = os.getenv('URL_SQS_VENTAS_QUEUE')

print(f"🌉 SQS Gateway escuchando en: {queue_url}")

while True:
    print("⏳ Polling SQS...")
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5
    )
    messages = response.get('Messages', [])
    if messages:
        for msg in messages:
            print(f"📥 Recibido mensaje: {msg['MessageId']}")
            print(f"Contenido: {msg['Body'][:200]}...")
            # Enviar tarea a Celery
            celery_app.send_task('worker_test.process_sqs_message', args=[msg['Body']])
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg['ReceiptHandle']
            )
            print(f"🗑️ Mensaje eliminado de SQS: {msg['MessageId']}")
    else:
        print("🔎 No hay mensajes en SQS.")
    time.sleep(2)