import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib import tabla as t  # noqa: E402
from relay import stream_a_bus  # noqa: E402

TABLA = "test-t_plantilla"
BUS = "lms"
TENANT = "utec"
DOCENTE = "c62d9930-b473-4b1d-adec-72fdda7cf599"
ALUMNO = "a18577e2-e005-4a49-a29e-755dbf08f5b6"


def token(rol="TEACHER", tenant=TENANT, user=DOCENTE):
    return {"Authorization": f"Bearer mock.{tenant}.{rol}.{user}"}


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("TABLA", TABLA)
    monkeypatch.setenv("EVENT_BUS", BUS)
    monkeypatch.setenv("AUTH_MODE", "mock")
    with mock_aws():
        t.reiniciar()
        stream_a_bus.reiniciar()
        ddb = boto3.client("dynamodb")
        ddb.create_table(
            TableName=TABLA,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "tenant_id", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "tenant_id", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
        )
        sqs = boto3.client("sqs")
        cola = sqs.create_queue(QueueName="plantilla-eventos")["QueueUrl"]
        cola_arn = sqs.get_queue_attributes(QueueUrl=cola, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        events = boto3.client("events")
        events.create_event_bus(Name=BUS)
        events.put_rule(
            Name="plantilla-consumidor",
            EventBusName=BUS,
            EventPattern='{"source":["lms.plantilla"],"detail-type":["lms.plantilla.example.created.v1"]}',
        )
        events.put_targets(Rule="plantilla-consumidor", EventBusName=BUS, Targets=[{"Id": "1", "Arn": cola_arn}])
        yield {"cola": cola, "sqs": sqs, "ddb": ddb}
