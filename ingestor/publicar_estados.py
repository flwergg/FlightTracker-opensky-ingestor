"""Ingestor de OpenSky para FlightTracker.

Obtiene un token OAuth2 de OpenSky, consulta los estados de vuelo en vivo,
los normaliza al contrato opensky.state.v1 y los publica en Pub/Sub.

La normalizacion replica la del productor anterior en Cloud Run
(katals/FlightTracker: pipelines/streaming/productor_opensky/main.py) para que
la funcion project_opensky_state los consuma sin cambios.

Variables de entorno:
  OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET  credenciales OAuth2 (secrets)
  GCP_PROJECT_ID                            proyecto GCP de destino
  PUBSUB_TOPIC                              topic de destino (opensky-states-v1)
  OPENSKY_BBOX                              opcional: "lamin,lomin,lamax,lomax"
"""

import hashlib
import json
import os
import sys
from typing import Any

import requests
from google.cloud import pubsub_v1

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
STATES_URL = "https://opensky-network.org/api/states/all"
SCHEMA_VERSION = "opensky.state.v1"
SOURCE = "opensky"
TIMEOUT_SEC = 30


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
        "source": SOURCE,
    }


def publicar(project_id: str, topic: str, eventos: list[dict[str, Any]]) -> tuple[int, int]:
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


def main() -> int:
    client_id = _required_env("OPENSKY_CLIENT_ID")
    client_secret = _required_env("OPENSKY_CLIENT_SECRET")
    project_id = _required_env("GCP_PROJECT_ID")
    topic = _required_env("PUBSUB_TOPIC")
    bbox = os.environ.get("OPENSKY_BBOX", "").strip()

    token = obtener_token(client_id, client_secret)
    observed_at, states = consultar_estados(token, bbox)

    eventos = [e for e in (normalizar_estado(observed_at, s) for s in states) if e]
    descartados = len(states) - len(eventos)
    publicados, fallidos = publicar(project_id, topic, eventos)

    print(f"area consultada: {bbox or 'mundo completo'}")
    print(f"observed_at: {observed_at}")
    print(f"states recibidos: {len(states)}")
    print(f"states descartados: {descartados}")
    print(f"states publicados: {publicados}")
    print(f"states fallidos: {fallidos}")

    if fallidos:
        return 1
    if publicados == 0:
        print("::warning::No se publico ningun estado en esta corrida")
    return 0


if __name__ == "__main__":
    sys.exit(main())
