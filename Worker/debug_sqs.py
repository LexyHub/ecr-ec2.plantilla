import boto3
import json
import os
from dotenv import load_dotenv
import time

load_dotenv()
### Función de debug para el polling de SQS
def debug_sqs_polling():
    """Debug completo del polling SQS"""
    
    # Configuración
    sqs = boto3.client(
        'sqs',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name='us-east-1'
    )
    
    queue_url = os.getenv('URL_SQS_VENTAS_QUEUE')
    
    print(f"🔧 DEBUG SQS:")
    print(f"   - Queue URL: {queue_url}")
    print(f"   - AWS Key: {os.getenv('AWS_ACCESS_KEY_ID')[:10]}...")
    
    # 1. Verificar atributos de la cola
    try:
        print(f"\n📊 ATRIBUTOS DE LA COLA:")
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['All']
        )
        
        attrs = response['Attributes']
        print(f"   - Mensajes disponibles: {attrs.get('ApproximateNumberOfMessages', 'N/A')}")
        print(f"   - Mensajes en procesamiento: {attrs.get('ApproximateNumberOfMessagesNotVisible', 'N/A')}")
        print(f"   - Tiempo de visibilidad: {attrs.get('VisibilityTimeout', 'N/A')} segundos")
        
    except Exception as e:
        print(f"❌ Error obteniendo atributos: {e}")
        return
    
    # 2. Hacer polling manual
    print(f"\n🔄 INICIANDO POLLING MANUAL...")
    
    for i in range(5):  # 5 intentos
        try:
            print(f"\n📥 Intento {i+1}/5 - Polling...")
            
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=2,  # Short polling para debug
                MessageAttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            
            if messages:
                print(f"✅ MENSAJE ENCONTRADO!")
                for msg in messages:
                    print(f"   - MessageId: {msg.get('MessageId')}")
                    print(f"   - Body: {msg.get('Body')[:200]}...")
                    print(f"   - Attributes: {msg.get('MessageAttributes', {})}")
                    
                    # Procesar mensaje (sin eliminar para debug)
                    try:
                        body = json.loads(msg['Body'])
                        if 'Message' in body:
                            # Es mensaje de SNS
                            sns_message = json.loads(body['Message'])
                            print(f"   - SNS Message: {json.dumps(sns_message, indent=4)}")
                        else:
                            # Es mensaje directo
                            print(f"   - Direct Message: {json.dumps(body, indent=4)}")
                    except Exception as parse_error:
                        print(f"   ❌ Error parseando mensaje: {parse_error}")
                
                break
            else:
                print(f"   ⏳ No hay mensajes (intento {i+1}/5)")
                time.sleep(1)
        
        except Exception as e:
            print(f"❌ Error en polling: {e}")
            break
    
    print(f"\n🏁 Debug completado.")

if __name__ == "__main__":
    debug_sqs_polling()