#!/usr/bin/env bash
# CP-11 Desacoplamiento temporal: mensajes publicados con el worker apagado se procesan al iniciarlo
source "$(dirname "$0")/_comun.sh"
titulo "CP-11 - Desacoplamiento temporal RabbitMQ ($(ahora))"

paso "Worker apagado"
docker compose stop notification-worker

paso "Se publican 3 pedidos mientras el worker no existe"
IDS=()
for i in 1 2 3; do crear_pedido "Cliente-offline-$i" "Producto-$i" 1; IDS+=("$ORDER_ID"); done
estado_cola

paso "Se espera 15 s con el worker apagado: los mensajes siguen en la cola"
sleep 15
estado_cola 0
ver_mensajes_en_cola 3

paso "Se inicia el worker: procesa los mensajes publicados mientras estaba apagado"
docker compose start notification-worker
sleep 18
for id in "${IDS[@]}"; do logs_worker | grep "$id" | grep -E "recibido|confirmado"; done
estado_cola 2
