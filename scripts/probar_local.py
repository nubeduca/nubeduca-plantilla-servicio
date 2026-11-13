"""
Prueba el criterio de LIB-01 en el LocalStack de LOC-01 (nubeduca-infra/localstack):
un servicio publica un evento por outbox y otro lo consume una sola vez.

    cd nubeduca-infra/localstack && docker compose up -d --wait
    cd nubeduca-plantilla-servicio
    pip install -r requirements.txt
    python scripts/probar_local.py

Crea su propia tabla con Stream (local-t_plantilla), una regla en el bus `lms` y una cola,
y corre los handlers en tu máquina contra LocalStack. El relay hace el trabajo del Pipe.
"""

import json
import os
import sys
import time
from pathlib import Path

os.environ.setdefault("AWS_ENDPOINT_URL", "http://localhost:4566")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.update({"TABLA": "local-t_plantilla", "EVENT_BUS": "lms", "AUTH_MODE": "mock"})
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import boto3  # noqa: E402

from api import ejemplos  # noqa: E402
from consumidores import ejemplo as consumidor  # noqa: E402
from lib import inbox  # noqa: E402
from relay import stream_a_bus  # noqa: E402

TABLA, BUS = os.environ["TABLA"], os.environ["EVENT_BUS"]
COLA, REGLA = "nubeduca-local-plantilla-eventos", "nubeduca-local-plantilla"


def paso(texto):
    print(f"\n▶ {texto}")


def preparar():
    ddb, sqs, events = boto3.client("dynamodb"), boto3.client("sqs"), boto3.client("events")
    try:
        ddb.delete_table(TableName=TABLA)
        ddb.get_waiter("table_not_exists").wait(TableName=TABLA)
    except ddb.exceptions.ResourceNotFoundException:
        pass
    ddb.create_table(
        TableName=TABLA, BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "tenant_id", "AttributeType": "S"},
                              {"AttributeName": "sk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "tenant_id", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
    )
    ddb.get_waiter("table_exists").wait(TableName=TABLA)
    cola = sqs.create_queue(QueueName=COLA)["QueueUrl"]
    sqs.purge_queue(QueueUrl=cola)
    cola_arn = sqs.get_queue_attributes(QueueUrl=cola, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    try:
        events.create_event_bus(Name=BUS)
    except events.exceptions.ResourceAlreadyExistsException:
        pass
    events.put_rule(Name=REGLA, EventBusName=BUS,
                    EventPattern=json.dumps({"source": ["lms.plantilla"],
                                             "detail-type": ["lms.plantilla.example.created.v1"]}))
    events.put_targets(Rule=REGLA, EventBusName=BUS, Targets=[{"Id": "1", "Arn": cola_arn}])
    return cola


def registros_stream():
    ddb, streams = boto3.client("dynamodb"), boto3.client("dynamodbstreams")
    arn = ddb.describe_table(TableName=TABLA)["Table"]["LatestStreamArn"]
    registros = []
    for shard in streams.describe_stream(StreamArn=arn)["StreamDescription"]["Shards"]:
        it = streams.get_shard_iterator(StreamArn=arn, ShardId=shard["ShardId"],
                                        ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        registros += streams.get_records(ShardIterator=it)["Records"]
    return registros


def main():
    paso("Preparando tabla con Stream, regla en el bus lms y cola en LocalStack")
    cola = preparar()

    paso("POST /examples como docente de UTEC (guarda el ejemplo + EVENTO# en una transacción)")
    r = ejemplos.lambda_handler({"httpMethod": "POST",
                                 "headers": {"Authorization": "Bearer mock.utec.TEACHER.c62d9930-b473-4b1d-adec-72fdda7cf599"},
                                 "body": json.dumps({"title": "Semana 1: Serverless"})}, None)
    print(r["statusCode"], r["body"])

    paso("Relay: Stream → bus lms")
    time.sleep(1)
    print("eventos publicados:", stream_a_bus.publicar(registros_stream()))

    paso("Consumidor: leyendo la cola")
    sqs = boto3.client("sqs")
    mensajes = []
    for _ in range(10):
        mensajes = sqs.receive_message(QueueUrl=cola, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", [])
        if mensajes:
            break
    if not mensajes:
        sys.exit("✗ No llegó ningún mensaje a la cola")
    cuerpo = mensajes[0]["Body"]
    print("1ª entrega:", inbox.procesar(inbox.leer_mensaje(cuerpo), consumidor.REACCIONES))
    print("2ª entrega (SQS reintenta):", inbox.procesar(inbox.leer_mensaje(cuerpo), consumidor.REACCIONES))

    tabla = boto3.resource("dynamodb").Table(TABLA)
    proy = tabla.query(KeyConditionExpression="tenant_id = :t AND begins_with(sk, :p)",
                       ExpressionAttributeValues={":t": "utec", ":p": "PROY#"})["Items"]
    print("\nProyecciones guardadas:", len(proy))
    print("✓ LIB-01: publica y consume sin duplicados" if len(proy) == 1 else "✗ Algo falló")


if __name__ == "__main__":
    main()
