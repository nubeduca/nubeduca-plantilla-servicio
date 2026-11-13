"""Configuración del servicio, leída de las variables de entorno que pone serverless.yml."""

import os

TENANTS = ("utec", "upc", "pucp")
ROLES = ("STUDENT", "TEACHER", "ADMIN")

# Cambia estos dos valores al copiar la plantilla (por ejemplo "ms-contenidos" y "lms.content").
SERVICIO = os.environ.get("SERVICIO", "ms-plantilla")
SOURCE = os.environ.get("EVENT_SOURCE", "lms.plantilla")


def tabla_nombre() -> str:
    return os.environ.get("TABLA", "dev-t_plantilla")


def bus_nombre() -> str:
    return os.environ.get("EVENT_BUS", "lms")


def auth_mode() -> str:
    """cognito: claims del authorizer de API Gateway · mock: token de demo del frontend."""
    return os.environ.get("AUTH_MODE", "cognito").lower()
