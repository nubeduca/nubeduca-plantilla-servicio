"""Criterio de LIB-01: un servicio de ejemplo publica y consume sin duplicados."""

import json

import boto3
import pytest
from botocore.exceptions import ClientError

from api import ejemplos, salud
from consumidores import ejemplo as consumidor
from lib import inbox, outbox
from lib import tabla as t
from relay import stream_a_bus
from tests.conftest import ALUMNO, TABLA, TENANT, token


def _post(titulo, headers):
    return ejemplos.lambda_handler({"httpMethod": "POST", "headers": headers, "body": json.dumps({"title": titulo})}, None)


def _registros_stream():
    ddb = boto3.client("dynamodb")
    streams = boto3.client("dynamodbstreams")
    arn = ddb.describe_table(TableName=TABLA)["Table"]["LatestStreamArn"]
    shard = streams.describe_stream(StreamArn=arn)["StreamDescription"]["Shards"][0]["ShardId"]
    it = streams.get_shard_iterator(StreamArn=arn, ShardId=shard, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
    return streams.get_records(ShardIterator=it)["Records"]


def _items(prefijo):
    resp = t.tabla().query(
        KeyConditionExpression="tenant_id = :t AND begins_with(sk, :p)",
        ExpressionAttributeValues={":t": TENANT, ":p": prefijo},
    )
    return resp["Items"]


def test_salud():
    r = salud.lambda_handler({}, None)
    assert r["statusCode"] == 200 and json.loads(r["body"])["status"] == "ok"


def test_publica_y_consume_sin_duplicados(aws):
    # 1. Comando: guarda el ejemplo y su evento en la misma transacción.
    r = _post("Semana 1", token())
    assert r["statusCode"] == 201
    example_id = json.loads(r["body"])["exampleId"]
    assert len(_items("EXAMPLE#")) == 1
    eventos = _items("EVENTO#")
    assert len(eventos) == 1
    detail = json.loads(eventos[0]["detail"])
    assert detail["metadata"]["tenantId"] == TENANT
    assert detail["data"] == {"exampleId": example_id, "title": "Semana 1"}

    # 2. Relay: el Stream lleva el EVENTO# al bus (el ítem EXAMPLE# se ignora).
    assert stream_a_bus.publicar(_registros_stream()) == 1

    # 3. La regla del bus copió el evento a la cola del consumidor.
    mensajes = aws["sqs"].receive_message(QueueUrl=aws["cola"], MaxNumberOfMessages=10)["Messages"]
    assert len(mensajes) == 1
    lote = {"Records": [{"messageId": "m1", "body": mensajes[0]["Body"]}]}

    # 4. El consumidor lo procesa una vez...
    assert consumidor.lambda_handler(lote, None) == {"batchItemFailures": []}
    assert len(_items("PROY#EXAMPLE#")) == 1
    assert len(_items("INBOX#")) == 1
    # ...y si SQS lo entrega de nuevo (at-least-once), se descarta.
    evento = inbox.leer_mensaje(mensajes[0]["Body"])
    assert inbox.procesar(evento, consumidor.REACCIONES) == "DUPLICADO"
    assert len(_items("PROY#EXAMPLE#")) == 1
    assert len(_items("INBOX#")) == 1


def test_descarta_eventos_viejos(aws):
    base = outbox.nuevo_evento(
        "lms.plantilla.example.created.v1",
        tenant_id=TENANT, aggregate_type="example", aggregate_id="exm_1",
        aggregate_version=2, actor_id="system", data={"exampleId": "exm_1", "title": "v2"},
    )
    viejo = outbox.nuevo_evento(
        "lms.plantilla.example.created.v1",
        tenant_id=TENANT, aggregate_type="example", aggregate_id="exm_1",
        aggregate_version=1, actor_id="system", data={"exampleId": "exm_1", "title": "v1"},
    )
    cuerpo = lambda e: json.dumps({"detail-type": e["detailType"], "source": e["source"], "detail": e["detail"]})
    assert inbox.procesar(inbox.leer_mensaje(cuerpo(base)), consumidor.REACCIONES) == "PROCESADO"
    assert inbox.procesar(inbox.leer_mensaje(cuerpo(viejo)), consumidor.REACCIONES) == "VIEJO"
    assert _items("PROY#EXAMPLE#")[0]["title"] == "v2"


EXM = "exm_01JCQRZHQT9F4WY2JYPDV546K7"


def test_transaccion_atomica(aws):
    """Si el cambio falla, el evento tampoco se guarda (nunca 'guardar y luego publicar')."""
    evento = outbox.nuevo_evento(
        "lms.plantilla.example.created.v1",
        tenant_id=TENANT, aggregate_type="example", aggregate_id=EXM,
        aggregate_version=1, actor_id="system", data={"exampleId": EXM, "title": "x"},
    )
    t.tabla().put_item(Item={"tenant_id": TENANT, "sk": f"EXAMPLE#{EXM}"})
    cambio = {"Put": {"Item": {"tenant_id": TENANT, "sk": f"EXAMPLE#{EXM}"},
                      "ConditionExpression": "attribute_not_exists(sk)"}}
    with pytest.raises(ClientError):  # falla la condición del cambio, no el contrato
        outbox.guardar([cambio], [evento])
    assert _items("EVENTO#") == []


def test_tenant_sale_del_token(aws):
    r = ejemplos.lambda_handler(
        {"httpMethod": "POST", "headers": token(tenant="upc"), "body": json.dumps({"title": "x", "tenant_id": "utec"})},
        None,
    )
    assert r["statusCode"] == 201
    assert _items("EXAMPLE#") == []  # nada en utec: el body no decide el tenant


def test_permisos_y_validacion(aws):
    assert _post("x", {})["statusCode"] == 401
    assert _post("x", token(rol="STUDENT", user=ALUMNO))["statusCode"] == 403
    assert _post("", token())["statusCode"] == 400
    lista = ejemplos.lambda_handler({"httpMethod": "GET", "headers": token(rol="STUDENT", user=ALUMNO)}, None)
    assert lista["statusCode"] == 200
