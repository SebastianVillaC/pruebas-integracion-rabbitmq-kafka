#!/usr/bin/env bash
# CP-06 Trazabilidad: seguir el mismo orderId/correlationId en API, broker y worker
source "$(dirname "$0")/_comun.sh"
CORR="corr-cp06-$(date +%s)"

titulo "CP-06 - Trazabilidad RabbitMQ ($(ahora))  correlationId=$CORR"

paso "Worker detenido momentáneamente para capturar el mensaje en el broker"
docker compose stop notification-worker

paso "1) API: se envía el pedido con header X-Correlation-Id"
crear_pedido "Laura" "Teclado mecánico" 2 "$CORR"

paso "2) Broker: el mensaje en la cola pedidos lleva el mismo orderId/correlationId"
ver_mensajes_en_cola 1

paso "3) Worker: se inicia y procesa el mensaje"
docker compose start notification-worker
sleep 8

paso "Logs filtrados por correlationId en todos los servicios"
docker compose logs --no-color --tail 3000 orders-api notification-worker | grep "$CORR" | grep -v "| {"
