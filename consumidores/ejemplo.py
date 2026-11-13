"""
Consumidor de ejemplo: regla del bus `lms` → cola SQS (con DLQ) → este Lambda.

Cada servicio declara aquí sus reacciones {detail-type: función}. La función recibe el
Evento y devuelve las escrituras a aplicar; inbox.manejar_lote se encarga del resto
(duplicados, eventos viejos y fallos parciales).

En la plantilla, el servicio consume su propio evento para probar el ciclo completo.
En un servicio real consumes eventos de OTROS servicios, por ejemplo ms-contenidos:
    "lms.courses.course.created.v1": guardar_curso,
    "lms.courses.enrollment.created.v1": agregar_matriculado,
"""

from lib import inbox


def guardar_proyeccion(evento: inbox.Evento) -> list:
    return [
        {
            "Put": {
                "Item": {
                    "tenant_id": evento.tenant_id,
                    "sk": f"PROY#EXAMPLE#{evento.data['exampleId']}",
                    "entidad": "PROYECCION",
                    "title": evento.data.get("title"),
                    "recibidoDe": evento.source,
                }
            }
        }
    ]


REACCIONES = {
    "lms.plantilla.example.created.v1": guardar_proyeccion,
}


def lambda_handler(event, context):
    return inbox.manejar_lote(event, REACCIONES)
