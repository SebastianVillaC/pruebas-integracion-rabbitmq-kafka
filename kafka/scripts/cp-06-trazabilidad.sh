#!/usr/bin/env bash
# CP-06 Trazabilidad: seguir el mismo orderId/correlationId en API, broker y consumidores
source "$(dirname "$0")/_comun.sh"
CORR="corr-cp06-$(date +%s)"
titulo "CP-06 - Trazabilidad Kafka ($(ahora))  correlationId=$CORR"

paso "1) API: pedido con header X-Correlation-Id"
crear_pedido "Laura" "Teclado mecánico" 2 120000 "$CORR"

paso "2) Broker: evento en orders.events con el mismo correlationId (headers y cuerpo)"
sleep 1
ver_evento "$ORDER_ID"

paso "3) Consumidores: cada uno registra el mismo correlationId"
sleep 8
paso "Logs de todos los servicios filtrados por correlationId"
logs orders-api "${CONSUMIDORES[@]}" | grep "$CORR" | grep -v '| {'
