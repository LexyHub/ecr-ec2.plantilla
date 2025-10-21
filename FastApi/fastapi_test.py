import os
import boto3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
import traceback
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Cliente SNS con debugging
sns_client = boto3.client(
    'sns',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
)

class PublishData(BaseModel):
    id_test: str
    descripcion: str

@app.get("/")
def read_root():
    return {"status": "FastAPI funcionando con un cambio automatizado desde ecr", "version": "1.0"}

@app.get("/test_docker_deploy")
def docker_deploy():
    return {"status": "FastAPI desplegado correctamente desde Docker y simultaneo"}


@app.get("/debug/config")
def debug_config():
    """Verificar configuración AWS"""
    return {
        "aws_access_key": os.getenv('AWS_ACCESS_KEY_ID', 'NO_SET')[:10] + "...",
        "aws_region": os.getenv('AWS_DEFAULT_REGION', 'NO_SET'),
        "sns_topic_arn": os.getenv('ARN_SNS_VENTAS_LEAD_AGENDADO_V1', 'NO_SET'),
        "sqs_queue_url": os.getenv('URL_SQS_VENTAS_QUEUE', 'NO_SET')
    }

@app.post("/publicar")
def publicar_mensaje(data: PublishData):
    """Publica mensaje a SNS con debugging completo"""
    try:
        topic_arn = os.getenv('ARN_SNS_VENTAS_LEAD_AGENDADO_V1')

        # Mensaje a publicar
        message_data = {
            "id_test": data.id_test,
            "descripcion": data.descripcion,
            "timestamp": "2024-10-16T15:00:00Z"
        }
        
        print(f"📤 INTENTANDO publicar a SNS...")
        print(f"📨 Mensaje: {json.dumps(message_data, indent=2)}")
        
        # Publicar a SNS
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message_data),
            Subject=f"Test mensaje {data.id_test}"
        )
        return {
            "status": "SUCCESS",
            "message": "Mensaje publicado a SNS exitosamente",
            "sns_message_id": response.get('MessageId'),
            "topic_arn": topic_arn,
            "data": message_data,
            "debug_info": {
                "http_status": response.get('ResponseMetadata', {}).get('HTTPStatusCode'),
                "full_response": response
            }
        }
        
    except Exception as e:
        error_details = {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc()
        }
        
        
        return {
            "status": "ERROR",
            "error": error_details
        }

@app.post("/debug/test-sns")
def test_sns_direct():
    """Test directo de conexión SNS"""
    try:
        topic_arn = os.getenv('ARN_SNS_VENTAS_LEAD_AGENDADO_V1')
        
        # Test simple
        response = sns_client.list_topics()
        topics = [t['TopicArn'] for t in response.get('Topics', [])]
        
        topic_exists = topic_arn in topics
        
        return {
            "status": "SUCCESS",
            "topic_arn": topic_arn,
            "topic_exists": topic_exists,
            "total_topics": len(topics),
            "matching_topics": [t for t in topics if 'Ventas' in t or 'Salud' in t]
        }
        
    except Exception as e:
        return {
            "status": "ERROR",
            "error": str(e),
            "traceback": traceback.format_exc()
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("fastapi_test:app", host="127.0.0.1", port=8000, reload=True)