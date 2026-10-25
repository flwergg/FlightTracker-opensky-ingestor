"""Ingestor de OpenSky para FlightTracker.

Obtiene un token OAuth2 de OpenSky, consulta los estados de vuelo en vivo y
los publica en Pub/Sub. Por defecto publica un mensaje por lectura con el
contrato opensky.state.v2 (T-161, docs/sprint2/contrato-opensky-state-v2.md
del repositorio principal):
todos los aviones de aerolinea de la lectura en un JSON comprimido con gzip.
Con OPENSKY_CONTRATO=v1 publica un mensaje por avion, como antes (se quita en
T-232).

La normalizacion replica la del productor anterior en Cloud Run
(katals/FlightTracker: pipelines/streaming/productor_opensky/main.py) para que
la funcion project_opensky_state los consuma sin cambios.

Solo se publican vuelos de aerolinea (T-160): callsign con el ICAO de una
aerolinea de aerolineas_icao.txt seguido de un numero de vuelo (AAL1234,
DAL98A). La aviacion general, los militares y los estados sin callsign se
descartan y se cuentan en el log.

Variables de entorno:
  OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET  credenciales OAuth2 (secrets)
  GCP_PROJECT_ID                            proyecto GCP de destino
  PUBSUB_TOPIC                              topic de destino (opensky-states-v1)
  OPENSKY_BBOX                              opcional: "lamin,lomin,lamax,lomax"
  OPENSKY_CONTRATO                          opcional: v2 (por defecto) o v1
"""

import gzip
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import requests

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
STATES_URL = "https://opensky-network.org/api/states/all"
SCHEMA_VERSION = "opensky.state.v1"
SCHEMA_VERSION_V2 = "opensky.state.v2"
CONTENT_ENCODING = "gzip"
# Campos de cada avion en opensky.state.v2, ademas de aerolinea_icao y numero.
CAMPOS_AVION_V2 = (
    "icao24", "callsign", "origin_country", "latitude", "longitude",
    "baro_altitude", "geo_altitude", "on_ground", "velocity", "heading",
    "vertical_rate", "squawk", "last_contact", "position_source", "category",
)
SOURCE = "opensky"
TIMEOUT_SEC = 30
AEROLINEAS_ICAO_FILE = Path(__file__).with_name("aerolineas_icao.txt")
# ICAO de la aerolinea (3 letras), numero de 1 a 4 digitos y hasta 2 letras de
# sufijo que el control de transito agrega a algunos vuelos (DAL98A).
CALLSIGN_AEROLINEA = re.compile(r"^([A-Z]{3})(\d{1,4})([A-Z]{0,2})$")


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Falta la variable de entorno {name}")
    return value


def obtener_token(client_id: str, client_secret: str) -> str:
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=TIMEOUT_SEC,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("OpenSky no devolvio access_token")
    # Enmascarar antes de cualquier otra salida: los logs del repo son publicos.
    print(f"::add-mask::{token}", flush=True)
    return token


def _parametros_bbox(bbox: str) -> dict[str, float]:
    partes = [p.strip() for p in bbox.split(",")]
    if len(partes) != 4:
        raise RuntimeError("OPENSKY_BBOX debe tener 4 valores: lamin,lomin,lamax,lomax")
    lamin, lomin, lamax, lomax = (float(p) for p in partes)
    return {"lamin": lamin, "lomin": lomin, "lamax": lamax, "lomax": lomax}


def consultar_estados(token: str, bbox: str) -> tuple[int | None, list[list[Any]]]:
    params = _parametros_bbox(bbox) if bbox else None
    response = requests.get(
        STATES_URL,
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=TIMEOUT_SEC,
    )
    response.raise_for_status()
    payload = response.json()
    return payload.get("time"), payload.get("states") or []


def cargar_aerolineas_icao(path: Path = AEROLINEAS_ICAO_FILE) -> frozenset[str]:
    """ICAO de aerolinea de aerolineas_icao.txt (uno por linea; # comenta)."""
    lineas = path.read_text(encoding="utf-8").splitlines()
    return frozenset(linea.strip() for linea in lineas if linea.strip() and not linea.startswith("#"))


def datos_aerolinea(callsign: str | None, codigos: frozenset[str]) -> tuple[str, int] | None:
    """("AAL", 1234) si el callsign es de un vuelo de aerolinea; None si no.

    El numero va sin ceros a la izquierda, como numero en la referencia de BTS
    (agente.referencia_vuelo) y en shared/flight_reference.
    """
    match = CALLSIGN_AEROLINEA.match((callsign or "").strip().upper())
    if not match or match.group(1) not in codigos or int(match.group(2)) == 0:
        return None
    return match.group(1), int(match.group(2))


def filtrar_aerolineas(
    eventos: list[dict[str, Any]], codigos: frozenset[str]
) -> tuple[list[dict[str, Any]], int]:
    """Deja solo los vuelos de aerolinea; devuelve (aceptados, descartados)."""
    aceptados = [e for e in eventos if datos_aerolinea(e.get("callsign"), codigos)]
    return aceptados, len(eventos) - len(aceptados)


def _build_event_id(observed_at: int | None, icao24: str, last_contact: int | None) -> str:
    key = f"{observed_at}|{icao24}|{last_contact or ''}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def normalizar_estado(observed_at: int | None, state: list[Any]) -> dict[str, Any] | None:
    if not state or len(state) < 17 or not state[0]:
        return None

    icao24 = str(state[0]).strip().lower()
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": _build_event_id(observed_at, icao24, state[4]),
        "observed_at": observed_at,
        "icao24": icao24,
        "callsign": (state[1] or "").strip(),
        "origin_country": state[2],
        "longitude": state[5],
        "latitude": state[6],
        "baro_altitude": state[7],
        "on_ground": state[8],
        "velocity": state[9],
        "heading": state[10],
        "vertical_rate": state[11],
        "geo_altitude": state[13],
        "squawk": state[14],
        "spi": state[15],
        "position_source": state[16],
        "category": state[17] if len(state) > 17 else None,
        "last_contact": state[4],
        "source": SOURCE,
    }


def avion_v2(evento: dict[str, Any], aerolinea: tuple[str, int]) -> dict[str, Any]:
    """Un avion de opensky.state.v2 desde un estado normalizado y su (ICAO, numero)."""
    avion = {campo: evento.get(campo) for campo in CAMPOS_AVION_V2}
    avion["aerolinea_icao"], avion["numero"] = aerolinea
    return avion


def construir_lectura_v2(
    observed_at: int | None,
    eventos: list[dict[str, Any]],
    codigos: frozenset[str],
    area: dict[str, float] | None,
    source: str = SOURCE,
) -> dict[str, Any]:
    """Cuerpo de opensky.state.v2: los aviones de aerolinea de una lectura.

    eventos son estados normalizados (normalizar_estado); los que no son de
    aerolinea se cuentan en descartados_aviacion_general.
    """
    aviones = []
    for evento in eventos:
        aerolinea = datos_aerolinea(evento.get("callsign"), codigos)
        if aerolinea:
            aviones.append(avion_v2(evento, aerolinea))
    clave = f"{observed_at}|{source}|{json.dumps(area, sort_keys=True)}"
    return {
        "schema_version": SCHEMA_VERSION_V2,
        # Trazabilidad del mensaje (ADR-001): una lectura, un event_id.
        "event_id": hashlib.sha256(clave.encode("utf-8")).hexdigest(),
        "observed_at": observed_at,
        "source": source,
        "area": area,
        "descartados_aviacion_general": len(eventos) - len(aviones),
        "aviones": aviones,
    }


def codificar_lectura(lectura: dict[str, Any]) -> tuple[bytes, dict[str, str]]:
    """Datos (JSON comprimido con gzip) y atributos del mensaje de Pub/Sub."""
    cuerpo = json.dumps(lectura, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    atributos = {
        "schema_version": SCHEMA_VERSION_V2,
        "content_encoding": CONTENT_ENCODING,
        "observed_at": str(lectura["observed_at"]),
        "source": lectura["source"],
        "aviones": str(len(lectura["aviones"])),
    }
    # mtime=0: la misma lectura da siempre los mismos bytes.
    return gzip.compress(cuerpo, mtime=0), atributos


def decodificar_lectura(datos: bytes, atributos: dict[str, str]) -> dict[str, Any]:
    """Inversa de codificar_lectura (referencia para los consumidores del topic)."""
    if atributos.get("content_encoding") == CONTENT_ENCODING:
        datos = gzip.decompress(datos)
    return json.loads(datos)


def publicar(project_id: str, topic: str, eventos: list[dict[str, Any]]) -> tuple[int, int]:
    # Import diferido: las pruebas del filtro no necesitan el cliente de Pub/Sub.
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic)

    futures = [
        publisher.publish(
            topic_path,
            json.dumps(evento).encode("utf-8"),
            schema_version=SCHEMA_VERSION,
            source=SOURCE,
        )
        for evento in eventos
    ]

    publicados, fallidos = 0, 0
    for future in futures:
        try:
            future.result(timeout=60)
            publicados += 1
        except Exception as exc:  # noqa: BLE001 - se reporta y se cuenta
            fallidos += 1
            if fallidos <= 3:
                print(f"Error publicando un estado: {exc}", flush=True)
    return publicados, fallidos


def publicar_lectura(project_id: str, topic: str, lectura: dict[str, Any]) -> None:
    """Publica una lectura opensky.state.v2 como un solo mensaje."""
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    datos, atributos = codificar_lectura(lectura)
    publisher.publish(publisher.topic_path(project_id, topic), datos, **atributos).result(timeout=60)


def main() -> int:
    client_id = _required_env("OPENSKY_CLIENT_ID")
    client_secret = _required_env("OPENSKY_CLIENT_SECRET")
    project_id = _required_env("GCP_PROJECT_ID")
    topic = _required_env("PUBSUB_TOPIC")
    bbox = os.environ.get("OPENSKY_BBOX", "").strip()
    contrato = os.environ.get("OPENSKY_CONTRATO", "v2").strip() or "v2"
    if contrato not in ("v1", "v2"):
        raise RuntimeError("OPENSKY_CONTRATO debe ser v1 o v2")

    token = obtener_token(client_id, client_secret)
    observed_at, states = consultar_estados(token, bbox)

    eventos = [e for e in (normalizar_estado(observed_at, s) for s in states) if e]
    descartados = len(states) - len(eventos)
    codigos = cargar_aerolineas_icao()
    if contrato == "v2":
        lectura = construir_lectura_v2(observed_at, eventos, codigos, _parametros_bbox(bbox) if bbox else None)
        publicar_lectura(project_id, topic, lectura)
        print(f"contrato: {SCHEMA_VERSION_V2} (un mensaje por lectura)")
        print(f"area consultada: {bbox or 'mundo completo'}")
        print(f"observed_at: {observed_at}")
        print(f"states recibidos: {len(states)}")
        print(f"states descartados: {descartados}")
        print(f"states descartados (aviacion general): {lectura['descartados_aviacion_general']}")
        print(f"aviones publicados: {len(lectura['aviones'])}")
        if not lectura["aviones"]:
            print("::warning::No se publico ningun avion en esta corrida")
        return 0

    eventos, aviacion_general = filtrar_aerolineas(eventos, codigos)
    publicados, fallidos = publicar(project_id, topic, eventos)

    print(f"area consultada: {bbox or 'mundo completo'}")
    print(f"observed_at: {observed_at}")
    print(f"states recibidos: {len(states)}")
    print(f"states descartados: {descartados}")
    print(f"states descartados (aviacion general): {aviacion_general}")
    print(f"states publicados: {publicados}")
    print(f"states fallidos: {fallidos}")

    if fallidos:
        return 1
    if publicados == 0:
        print("::warning::No se publico ningun estado en esta corrida")
    return 0


if __name__ == "__main__":
    sys.exit(main())
