#!/usr/bin/env bash
# CP-09 Resilient: el sistema soporta la caída del consumidor
source "$(dirname "$0")/_comun.sh"
titulo "CP-09 - Resiliencia ante caída del consumidor RabbitMQ ($(ahora))"

paso "Se apaga el worker (simula caída)"
docker compose stop notification-worker
docker compose ps

paso "La API sigue aceptando pedidos (HTTP 202) aunque no haya consumidor"
IDS=()
for i in 1 2 3 4 5; do crear_pedido "Cliente-caida-$i" "Producto-$i" 1; IDS+=("$ORDER_ID"); done

paso "Los mensajes se acumulan en la cola (Ready=5, Consumidores=0)"
estado_cola

paso "Logs de la API: no hubo errores"
for id in "${IDS[@]}"; do logs_api | grep "$id"; done

paso "Se recupera el worker y vacía la cola"
docker compose start notification-worker
sleep 25
for id in "${IDS[@]}"; do logs_worker | grep "$id" | grep "confirmado"; done
estado_cola 2
