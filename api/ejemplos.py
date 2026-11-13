"""
Ejemplo de comando y consulta. Al copiar la plantilla, reemplázalo por las rutas del
OpenAPI de tu servicio (nubeduca-contratos/apis/<servicio>.openapi.yaml).

POST /examples  (TEACHER, ADMIN)  crea un ejemplo y publica lms.plantilla.example.created.v1
GET  /examples  (cualquier rol)   lista los ejemplos del tenant del token
"""

from boto3.dynamodb.conditions import Key

from lib import auth, http, ids, outbox
from lib import tabla as t

DETAIL_TYPE_CREADO = "lms.plantilla.example.created.v1"
ROLES_ESCRITURA = ("TEACHER", "ADMIN")


def sk_ejemplo(example_id: str) -> str:
    return f"EXAMPLE#{example_id}"


def crear(event):
    usuario = auth.usuario(event)
    if usuario is None:
        return http.no_autenticado(event)
    if usuario.rol not in ROLES_ESCRITURA:
        return http.sin_permiso(event)

    cuerpo = http.leer_json(event) or {}
    titulo = (cuerpo.get("title") or "").strip()
    if not titulo or len(titulo) > 200:
        return http.datos_invalidos(event, "title es obligatorio y tiene hasta 200 caracteres.")

    example_id = ids.nuevo("exm")
    ejemplo = {
        "exampleId": example_id,
        "title": titulo,
        "createdBy": usuario.user_id,
        "createdAt": outbox.ahora_iso(),
        "version": 1,
    }
    evento = outbox.nuevo_evento(
        DETAIL_TYPE_CREADO,
        tenant_id=usuario.tenant_id,  # del token, nunca del body
        aggregate_type="example",
        aggregate_id=example_id,
        aggregate_version=1,
        actor_id=usuario.user_id,
        data={"exampleId": example_id, "title": titulo},  # payload mínimo
    )
    outbox.guardar(
        cambios=[
            {
                "Put": {
                    "Item": {"tenant_id": usuario.tenant_id, "sk": sk_ejemplo(example_id),
                             "entidad": "EXAMPLE", **ejemplo},
                    "ConditionExpression": "attribute_not_exists(sk)",
                }
            }
        ],
        eventos=[evento],
    )
    return http.respuesta(201, ejemplo)


def listar(event):
    usuario = auth.usuario(event)
    if usuario is None:
        return http.no_autenticado(event)
    resultado = t.tabla().query(
        KeyConditionExpression=Key("tenant_id").eq(usuario.tenant_id) & Key("sk").begins_with("EXAMPLE#")
    )
    items = [
        {k: v for k, v in item.items() if k not in ("tenant_id", "sk", "entidad")}
        for item in resultado.get("Items", [])
    ]
    return http.respuesta(200, {"items": items})


def lambda_handler(event, context):
    metodo = (event or {}).get("httpMethod", "GET")
    return crear(event) if metodo == "POST" else listar(event)
