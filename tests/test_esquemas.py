"""Paso 4 del diagrama: el evento se valida contra su esquema antes de guardarse."""

import json
from pathlib import Path

import pytest

from lib import esquemas, outbox
from lib import tabla as t
from tests.conftest import TENANT

EJEMPLOS = Path(__file__).parent / "ejemplos"


@pytest.mark.parametrize("archivo", sorted(p.name for p in EJEMPLOS.glob("*.json")))
def test_ejemplos_oficiales_cumplen_su_esquema(archivo):
    evento = json.loads((EJEMPLOS / archivo).read_text(encoding="utf-8"))
    assert esquemas.errores(evento["detail-type"], evento["detail"]) == []


def _modulo():
    return json.loads((EJEMPLOS / "lms.content.module.published.v1.json").read_text(encoding="utf-8"))["detail"]


def test_rechaza_campo_faltante():
    detail = _modulo()
    del detail["data"]["title"]
    assert "detail.data.title: es obligatorio" in esquemas.errores("lms.content.module.published.v1", detail)


def test_rechaza_campo_extra_tipo_y_patron():
    detail = _modulo()
    detail["data"]["profesor"] = "Juan"  # payload mínimo: nada de nombres
    detail["data"]["position"] = "3"
    detail["metadata"]["courseId"] = "curso-1"
    errs = esquemas.errores("lms.content.module.published.v1", detail)
    assert any("profesor" in e for e in errs)
    assert any("position: debe ser integer" in e for e in errs)
    assert any("courseId: no cumple el patrón" in e for e in errs)


def test_rechaza_evento_fuera_del_catalogo():
    with pytest.raises(esquemas.EventoInvalido):
        esquemas.validar("lms.content.archivo.inventado.v1", _modulo())


def test_nuevo_evento_de_la_plantilla_cumple_su_esquema():
    evento = outbox.nuevo_evento(
        "lms.plantilla.example.created.v1", tenant_id=TENANT, aggregate_type="example",
        aggregate_id="exm_01JCQRZHQT9F4WY2JYPDV546K7", aggregate_version=1, actor_id="system",
        data={"exampleId": "exm_01JCQRZHQT9F4WY2JYPDV546K7", "title": "Semana 1"},
    )
    assert esquemas.errores(evento["detailType"], evento["detail"]) == []


def test_outbox_no_guarda_nada_si_el_evento_es_invalido(aws):
    evento = outbox.nuevo_evento(
        "lms.plantilla.example.created.v1", tenant_id=TENANT, aggregate_type="example",
        aggregate_id="exm_x", aggregate_version=1, actor_id="system",
        data={"exampleId": "no-es-un-id", "title": "x"},
    )
    cambio = {"Put": {"Item": {"tenant_id": TENANT, "sk": "EXAMPLE#exm_x"}}}
    with pytest.raises(esquemas.EventoInvalido):
        outbox.guardar([cambio], [evento])
    assert t.tabla().get_item(Key={"tenant_id": TENANT, "sk": "EXAMPLE#exm_x"}).get("Item") is None
