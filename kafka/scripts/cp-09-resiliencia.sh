#!/usr/bin/env bash
# CP-09 Resilient: el sistema soporta la caída de un consumidor
source "$(dirname "$0")/_comun.sh"
titulo "CP-09 - Resiliencia ante caída de un consumidor Kafka ($(ahora))"

paso "Se apaga billing-consumer (simula caída)"
docker compose stop billing-consumer
docker compose ps

paso "La API sigue aceptando pedidos (HTTP 202)"
IDS=()
for i in 1 2 3 4 5; do crear_pedido "Cliente-caida-$i" "Producto-$i" 1 30000; IDS+=("$ORDER_ID"); done

paso "Inventario y notificación siguen procesando; billing acumula lag"
sleep 25
lags
grupo billing-service-group

paso "Se recupera billing-consumer y procesa los eventos pendientes desde su último offset"
docker compose start billing-consumer
sleep 35
for id in "${IDS[@]}"; do logs billing-consumer | grep "$id" | grep "offset confirmado"; done
lags
