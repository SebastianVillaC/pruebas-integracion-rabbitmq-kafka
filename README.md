# Pruebas de integración en arquitecturas basadas en mensajes y eventos

Taller de Aseguramiento de la Calidad (ITM). Sistema de procesamiento de pedidos implementado dos veces:

| Arquitectura | Carpeta | Contenedores |
|---|---|---|
| Mensajes con **RabbitMQ** | [`rabbitmq/`](rabbitmq) | `rabbitmq`, `orders-api`, `notification-worker` (escalable) |
| Eventos con **Apache Kafka** | [`kafka/`](kafka) | `kafka`, `kafka-ui`, `orders-api`, `inventory-consumer`, `billing-consumer`, `notification-consumer` |

Cada componente corre en su propio contenedor. La API solo publica en el broker: **nunca** llama a los
consumidores, y estos no exponen HTTP.

```
Cliente HTTP ──POST /orders──▶ orders-api ──AMQP──▶ RabbitMQ [cola pedidos] ──▶ notification-worker (xN)

Cliente HTTP ──POST /orders──▶ orders-api ──Kafka──▶ Kafka [orders.events, 3 particiones]
                                                        ├──▶ inventory-consumer    (inventory-service-group)
                                                        ├──▶ billing-consumer      (billing-service-group)
                                                        └──▶ notification-consumer (notification-service-group)
```

## Requisitos

- Docker Desktop con Docker Compose v2
- `curl` y `python3` (los usan los scripts de prueba)
- Postman (opcional): importar `postman/Pedidos.postman_collection.json`

> Las dos arquitecturas publican la API en `localhost:3000`, así que **se levanta una a la vez**
> (`docker compose down` en una antes de `up` en la otra). Para tener ambas a la vez: `API_PORT=3001 docker compose up -d`.

## RabbitMQ

```bash
cd rabbitmq
docker compose up -d --build
docker compose ps
```

| Puerto | Uso |
|---|---|
| 3000 | API `POST /orders`, `GET /health` |
| 5672 | AMQP (uso interno: `amqp://admin:admin@rabbitmq:5672`) |
| 15672 | Consola RabbitMQ Management: http://localhost:15672 (admin / admin) |

```bash
curl -X POST http://localhost:3000/orders -H 'Content-Type: application/json' \
  -d '{"customerName":"Pedro","product":"Libro de arquitectura","quantity":1}'
# {"status":"accepted","message":"Pedido recibido y enviado a RabbitMQ","orderId":"ORD-...","correlationId":"..."}

docker compose logs -f notification-worker        # consumo
docker compose up -d --scale notification-worker=2 # múltiples workers
```

- Cola `pedidos` **durable**, mensajes **persistentes** y volumen `rabbitmq-data`: sobreviven al reinicio del broker.
- Worker con `prefetch(1)` y **ack manual**, procesamiento simulado de 2–5 s para observar `Unacked`.
- API y worker reintentan la conexión y se reconectan solos si RabbitMQ se reinicia.

## Kafka

```bash
cd kafka
docker compose up -d --build
docker compose ps
```

| Puerto | Uso |
|---|---|
| 3000 | API `POST /orders`, `GET /health` |
| 9092 | Kafka, uso interno entre contenedores (`kafka:9092`) |
| 29092 | Kafka desde la máquina anfitriona (`localhost:29092`) |
| 8080 | Kafka UI: http://localhost:8080 |

```bash
curl -X POST http://localhost:3000/orders -H 'Content-Type: application/json' \
  -d '{"customerName":"Pedro","product":"Libro de arquitectura","quantity":1,"unitPrice":85000}'
# {"status":"accepted","message":"Pedido recibido y evento OrderCreated publicado en Kafka","orderId":"ORD-...","correlationId":"..."}

docker compose logs -f inventory-consumer billing-consumer notification-consumer
docker compose up -d --scale inventory-consumer=2  # varias instancias del mismo grupo
```

- Evento de dominio `OrderCreated` (eventId, eventType, eventVersion, occurredAt, source, correlationId, data), con key = `orderId`.
- La API crea el tópico `orders.events` con **3 particiones** (necesario para repartir carga dentro de un grupo).
- Un consumer group distinto por consumidor: los tres reciben cada evento. El offset se confirma **después** de procesar.
- Los consumidores leen `fromBeginning`: un grupo nuevo lee todo el historial del log.
- Volumen `kafka-data`: el log y los offsets sobreviven al reinicio del broker.

## Casos de prueba → scripts

Cada script ejecuta el caso de punta a punta e imprime la evidencia (respuestas HTTP, contadores de la cola,
offsets y lag, logs filtrados por `orderId`/`correlationId`). Se ejecutan desde la carpeta de la arquitectura,
con el entorno levantado: `./scripts/<script>.sh`.

| Caso | Propiedad | RabbitMQ (`rabbitmq/scripts/`) | Kafka (`kafka/scripts/`) |
|---|---|---|---|
| CP-01 | Creación de pedido vía API | `cp-01-02-03-flujo-basico.sh` | `cp-01-04-05-flujo-basico.sh` |
| CP-02 | Mensaje en la cola | `cp-01-02-03-flujo-basico.sh` | — |
| CP-03 | Consumo del mensaje | `cp-01-02-03-flujo-basico.sh` | — |
| CP-04 | Publicación de OrderCreated | — | `cp-01-04-05-flujo-basico.sh` |
| CP-05 | Consumo del evento | — | `cp-01-04-05-flujo-basico.sh` |
| CP-06 | Trazabilidad | `cp-06-trazabilidad.sh` | `cp-06-trazabilidad.sh` |
| CP-07 | Responsive | `cp-07-responsive.sh` | `cp-07-responsive.sh` |
| CP-08 | Message-driven | `cp-08-message-driven.sh` | `cp-08-message-driven.sh` |
| CP-09 | Resilient | `cp-09-resiliencia.sh` | `cp-09-resiliencia.sh` |
| CP-10 | Elastic | `cp-10-elastic.sh [replicas]` | `cp-10-elastic.sh [replicas]` |
| CP-11 | Desacoplamiento temporal | `cp-11-desacople-temporal.sh` | `cp-11-desacople-temporal.sh` (incluye consumidor posterior) |
| CP-12 | Recuperación | `cp-12-recuperacion.sh` | `cp-12-recuperacion.sh` |

Utilidades: `crear-pedido.sh`, `crear-pedidos.sh N`, `estado-cola.sh` (RabbitMQ), `grupos.sh` (Kafka).

Mientras corre cada script conviene tener abierta la consola (RabbitMQ Management o Kafka UI) para tomar
las capturas del informe: cola `pedidos` con Ready/Unacked/Total; tópico `orders.events`, mensajes, particiones y consumer groups.

## Limpieza

```bash
docker compose down      # detiene y elimina los contenedores (conserva los datos del broker)
docker compose down -v   # además borra los volúmenes (cola / log de eventos vacíos)
```
