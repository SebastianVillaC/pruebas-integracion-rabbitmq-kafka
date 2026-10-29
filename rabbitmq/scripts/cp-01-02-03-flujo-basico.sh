#!/usr/bin/env bash
# CP-01 Creación de pedido vía API
# CP-02 Envío de mensaje a RabbitMQ (mensaje visible en la cola)
# CP-03 Consumo de mensaje por el worker
source "$(dirname "$0")/_comun.sh"

titulo "CP-01 / CP-02 / CP-03 - Flujo básico RabbitMQ ($(ahora))"
docker compose ps

paso "Se detiene el worker para poder observar el mensaje dentro de la cola"
docker compose stop notification-worker

paso "CP-01: POST /orders"
crear_pedido "Pedro" "Libro de arquitectura" 1

paso "CP-01: datos inválidos -> debe responder 400"
curl -s -w '\n   -> HTTP %{http_code}\n' -X POST "$API_URL/orders" -H 'Content-Type: application/json' \
  -d '{"customerName":"","product":"Libro","quantity":0}'

paso "CP-02: cola pedidos en el broker (Ready debe ser 1)"
estado_cola
ver_mensajes_en_cola 1

paso "CP-03: se inicia el worker y consume el mensaje"
docker compose start notification-worker
sleep 8
logs_worker | grep -E "$ORDER_ID|Conectado" || true
estado_cola 2
echo
echo "Capturas sugeridas: consola http://localhost:15672/#/queues/%2F/pedidos y los logs anteriores."
