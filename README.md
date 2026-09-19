# FlightTracker — Ingestor de OpenSky

Ingestor de estados de vuelo en vivo para FlightTracker, el proyecto de la asignatura SI4002 (Proyecto de Ingeniería de Datos) de la Universidad EAFIT, semestre 2026-2. Este repositorio contiene únicamente el componente que obtiene datos reales de OpenSky Network y los publica en Google Cloud; el resto de la plataforma vive en el repositorio principal, [katals/FlightTracker](https://github.com/katals/FlightTracker), que es privado.

## Qué hace

Un workflow de GitHub Actions se ejecuta cada 5 minutos y, en cada corrida:

1. Obtiene un token OAuth2 de OpenSky Network mediante el flujo `client_credentials`.
2. Consulta el endpoint `https://opensky-network.org/api/states/all`.
3. Normaliza cada vector de estado al contrato `opensky.state.v1` (ver más abajo).
4. Se autentica en Google Cloud mediante Workload Identity Federation, sin llaves JSON de service account.
5. Publica cada estado como un mensaje en el topic de Pub/Sub `opensky-states-v1` del proyecto GCP de FlightTracker.

El workflow también puede dispararse manualmente con `workflow_dispatch`.

## Por qué vive separado del repositorio principal

Hay dos restricciones que se combinan.

**Restricción de red.** OpenSky no acepta conexiones desde Google Cloud: desde Cloud Shell y Cloud Run la conexión TCP expira, mientras que desde una red residencial y desde un runner de GitHub Actions el servidor responde. El diagnóstico completo está en `docs/sprint2/evidencias/01-conectividad-opensky-oauth2-y-bloqueo-gcp.md` del repositorio principal. Por eso la ingesta se hace desde GitHub Actions y no desde un servicio en GCP.

**Restricción de minutos de GitHub Actions.** El repositorio principal es privado, y en repositorios privados los minutos de GitHub Actions tienen una cuota mensual. Un workflow cada 5 minutos son 288 ejecuciones diarias; como cada job se factura redondeando hacia arriba al minuto, el consumo mínimo ronda los 8.640 minutos al mes, muy por encima de la cuota del plan gratuito. En repositorios públicos los runners estándar no consumen esa cuota. Por eso, en decisión acordada con el docente el 16 de septiembre de 2026, el ingestor vive en este repositorio público y separado.

## Cómo se conecta con `opensky-states-v1`

```text
GitHub Actions (este repo)
   │  OAuth2 → OpenSky /api/states/all
   │  Workload Identity Federation → Google Cloud
   ▼
Pub/Sub: opensky-states-v1          (repositorio principal, Terraform)
   ▼
Cloud Function: project_opensky_state
   ▼
Firestore: live_flights             (un documento por icao24, se sobrescribe)
   ▼
API Cloud Run: /live/flights, /live/flights/{icao24}, /live/count
```

Este repositorio solo es responsable del primer tramo, hasta la publicación en `opensky-states-v1`. El topic, la Cloud Function, la colección y la API se declaran y despliegan desde el repositorio principal.

### Contrato del mensaje (`opensky.state.v1`)

Cada mensaje es un JSON en UTF-8 con estos campos. El contrato es idéntico al que usaba el productor anterior en Cloud Run (`pipelines/streaming/productor_opensky/main.py`), así que la función `project_opensky_state` lo consume sin cambios.

| Campo                                  | Origen en el vector de OpenSky | Notas                                                                                              |
| -------------------------------------- | ------------------------------ | -------------------------------------------------------------------------------------------------- |
| `schema_version`                       | constante                      | Siempre `opensky.state.v1`                                                                         |
| `event_id`                             | calculado                      | `sha256("{observed_at}\|{icao24}\|{last_contact}")`, para trazabilidad de la ingesta               |
| `observed_at`                          | `time` de la respuesta         | Epoch Unix en segundos                                                                             |
| `icao24`                               | índice 0                       | En minúsculas y sin espacios; clave del documento en `live_flights`                                |
| `callsign`                             | índice 1                       | Sin espacios al inicio ni al final                                                                 |
| `origin_country`                       | índice 2                       |                                                                                                    |
| `longitude`, `latitude`                | índices 5 y 6                  |                                                                                                    |
| `baro_altitude`                        | índice 7                       |                                                                                                    |
| `on_ground`                            | índice 8                       |                                                                                                    |
| `velocity`, `heading`, `vertical_rate` | índices 9, 10 y 11             |                                                                                                    |
| `geo_altitude`                         | índice 13                      |                                                                                                    |
| `squawk`, `spi`, `position_source`     | índices 14, 15 y 16            |                                                                                                    |
| `category`                             | índice 17                      | Puede no venir; en ese caso `null`                                                                 |
| `source`                               | constante                      | Siempre `opensky`, para distinguirlo de los eventos sintéticos (`manual-demo`, `integration-test`) |

Los vectores sin `icao24` se descartan. Cada mensaje lleva además los atributos de Pub/Sub `schema_version=opensky.state.v1` y `source=opensky`.

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

- GitHub puede retrasar las ejecuciones programadas en momentos de alta carga, por lo que el intervalo de 5 minutos es aproximado. Para una demostración conviene disparar el workflow manualmente.
- GitHub desactiva los workflows programados de un repositorio público tras 60 días sin actividad. Si se desactiva, se reactiva desde la pestaña *Actions*.

## Estado

En construcción durante el Sprint 2 (historia US-01.1 del backlog del repositorio principal). Este README se actualizará cuando exista una corrida productiva documentada.

## Equipo

- Gabriela Martínez Mercado — [@flwergg](https://github.com/flwergg)
- Juan Carlos Muñoz Trejos — [@katals](https://github.com/katals)
- Juan Simón Ospina Martínez — [@juansimonEAFIT](https://github.com/juansimonEAFIT)
