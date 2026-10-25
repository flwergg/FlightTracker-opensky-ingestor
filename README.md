# FlightTracker — Ingestor de OpenSky

Ingestor de estados de vuelo en vivo para FlightTracker, el proyecto de la asignatura SI4002 (Proyecto de Ingeniería de Datos) de la Universidad EAFIT, semestre 2026-2. Este repositorio contiene únicamente el componente que obtiene datos reales de OpenSky Network y los publica en Google Cloud; el resto de la plataforma vive en el repositorio principal, [katals/FlightTracker](https://github.com/katals/FlightTracker), que es privado.

## Qué hace

Cloud Scheduler, desde el proyecto de GCP de FlightTracker, dispara el workflow de GitHub Actions cada 2 minutos por `workflow_dispatch` (función `disparar_ingestor_opensky`). En cada corrida, el workflow:

1. Obtiene un token OAuth2 de OpenSky Network mediante el flujo `client_credentials`.
2. Consulta `https://opensky-network.org/api/states/all` en el área de la costa este de EE. UU. (`24.5,-85.0,43.0,-69.5`), la misma geografía de los vuelos de BTS. Cada consulta cuesta 3 créditos de OpenSky.
3. Deja solo los vuelos de aerolínea: callsign con el ICAO de una aerolínea de `ingestor/aerolineas_icao.txt` seguido de un número de vuelo (`AAL1234`, `DAL98A`). La aviación general se descarta y se cuenta en el log.
4. Arma un mensaje `opensky.state.v2` con todos los aviones de la lectura, en JSON comprimido con gzip.
5. Se autentica en Google Cloud mediante Workload Identity Federation, sin llaves JSON de service account, y publica el mensaje en el topic de Pub/Sub `opensky-states-v1`.

El workflow también puede dispararse a mano con `workflow_dispatch`, con otra área en el input `bbox`.

## Por qué vive separado del repositorio principal

Hay dos restricciones que se combinan.

**Restricción de red.** OpenSky no acepta conexiones desde Google Cloud: desde Cloud Shell y Cloud Run la conexión TCP expira, mientras que desde una red residencial y desde un runner de GitHub Actions el servidor responde. El diagnóstico completo está en `docs/sprint2/evidencias/01-conectividad-opensky-oauth2-y-bloqueo-gcp.md` del repositorio principal. Por eso la ingesta se hace desde GitHub Actions y no desde un servicio en GCP.

**Restricción de minutos de GitHub Actions.** El repositorio principal es privado, y en repositorios privados los minutos de GitHub Actions tienen una cuota mensual. Un workflow cada 5 minutos son 288 ejecuciones diarias; como cada job se factura redondeando hacia arriba al minuto, el consumo mínimo ronda los 8.640 minutos al mes, muy por encima de la cuota del plan gratuito. En repositorios públicos los runners estándar no consumen esa cuota. Por eso, en decisión acordada con el docente el 16 de septiembre de 2026, el ingestor vive en este repositorio público y separado.

## Cómo se conecta con `opensky-states-v1`

```text
Cloud Scheduler (*/2) -> disparar_ingestor_opensky -> workflow_dispatch
   ▼
GitHub Actions (este repo)
   │  OAuth2 → OpenSky /api/states/all (costa este)
   │  Workload Identity Federation → Google Cloud
   ▼
Pub/Sub: opensky-states-v1          (un mensaje opensky.state.v2 por lectura)
   ▼
Cloud Function: project_opensky_state
   ├─ Firestore: live_state/actual   (estado de todos los aviones de la lectura)
   ├─ Firestore: flight_events       (despegó y aterrizó)
   └─ Cloud Storage: lectura cruda   (se borra a los 14 días)
   ▼
API Cloud Run: /live/flights, /live/flights/{icao24}, /live/count
```

Este repositorio solo es responsable del primer tramo, hasta la publicación en `opensky-states-v1`. El topic, la Cloud Function, las colecciones y la API se declaran y despliegan desde el repositorio principal.

### Contrato del mensaje (`opensky.state.v2`)

Un mensaje por lectura. Los datos son JSON en UTF-8 comprimido con gzip. Los atributos de Pub/Sub son `schema_version=opensky.state.v2`, `content_encoding=gzip`, `observed_at` (segundos Unix de la lectura), `source=opensky` y `aviones` (cantidad).

El cuerpo trae `schema_version`, `event_id` (identifica la lectura), `observed_at`, `source`, `area` (los límites consultados), `descartados_aviacion_general` y la lista `aviones`. Cada avión trae `icao24`, `callsign`, `aerolinea_icao`, `numero` (sin ceros a la izquierda, como en BTS), `origin_country`, `latitude`, `longitude`, `baro_altitude`, `geo_altitude`, `on_ground`, `velocity`, `heading`, `vertical_rate`, `squawk`, `last_contact`, `position_source` y `category`.

La especificación completa, con un ejemplo, está en `docs/sprint2/contrato-opensky-state-v2.md` del repositorio principal. Es el único contrato: desde T-232 `project_opensky_state` manda cualquier otro a `opensky-states-dlq`, así que el ingestor ya no tiene modo v1.

`ingestor/aerolineas_icao.txt` lo genera `ingestor/generar_aerolineas_icao.py`: todos los ICAO de OpenFlights, en un commit fijo y sin mirar la marca de activa (PSA Airlines figura inactiva), más las aerolíneas de EE. UU. que OpenFlights no tiene con su ICAO actual.

## Autenticación y secretos

La autenticación contra Google Cloud usará `google-github-actions/auth` con Workload Identity Federation, de modo que Google Cloud confíe directamente en este repositorio sin guardar llaves JSON de service account. El proveedor de identidad se configurará para aceptar únicamente tokens emitidos para este repositorio. Esta configuración corresponde a las tareas T-01 y T-02 del backlog y está en construcción.

Secrets que requerirá el workflow en *Settings → Secrets and variables → Actions* (solo se documentan sus nombres; los valores nunca se escriben en el repositorio):

| Secret                           | Uso                                                               |
| -------------------------------- | ----------------------------------------------------------------- |
| `OPENSKY_CLIENT_ID`              | Cliente OAuth2 de OpenSky                                         |
| `OPENSKY_CLIENT_SECRET`          | Secreto OAuth2 de OpenSky                                         |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | Nombre completo del proveedor de Workload Identity                |
| `GCP_SERVICE_ACCOUNT`            | Service account con permiso de publicación en `opensky-states-v1` |

## Seguridad: los logs de este repositorio son públicos

Como el repositorio es público, cualquier persona puede leer los logs de las ejecuciones de GitHub Actions. Por eso:

- el token de OpenSky se enmascara con `::add-mask::` **antes** de cualquier impresión, y nunca se imprime, ni siquiera parcialmente;
- los secrets solo se leen desde `secrets.*` y nunca se escriben en archivos del repositorio;
- el log de cada corrida solo reporta conteos (estados recibidos, publicados y descartados).

## Consideraciones operativas

- **Lo dispara Cloud Scheduler, no un cron de GitHub.** Los workflows programados de GitHub resultaron irregulares (US-24) y, mientras convivieron con Cloud Scheduler, duplicaban lecturas. Por eso el workflow ya no tiene `schedule`.
- **Créditos de OpenSky.** 720 lecturas al día (`*/2`) a 3 créditos cada una son 2.160 de los 4.000 créditos diarios de una cuenta registrada (medición V4, T-158).
- **Minutos de GitHub Actions.** Cada corrida dura unos 20 s y se factura como 1 minuto. Son unos 720 minutos al día, sin costo porque el repositorio es público.
- **Copia en el repositorio principal.** `pipelines/streaming/ingestor_opensky/` lleva una copia de `publicar_estados.py` para el servicio `datos-live` de Docker Compose. Un cambio aquí se lleva allá a mano, manteniendo sus dos diferencias documentadas (no imprime el token y los avisos son mensajes normales).

## Estado

En producción desde el Sprint 2 (US-01.1, US-24). Con US-43 pasó a la costa este, a una lectura cada 2 minutos y al contrato `opensky.state.v2`.

## Equipo

- Gabriela Martínez Mercado — [@flwergg](https://github.com/flwergg)
- Juan Carlos Muñoz Trejos — [@katals](https://github.com/katals)
- Juan Simón Ospina Martínez — [@juansimonEAFIT](https://github.com/juansimonEAFIT)
