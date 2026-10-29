#!/usr/bin/env bash
# CP-12 Recuperación:
#  A) el worker muere con un mensaje sin confirmar (Unacked) -> RabbitMQ lo reentrega (redelivered=true)
#  B) se reinicia el broker -> la cola durable y los mensajes persistentes sobreviven
source "$(dirname "$0")/_comun.sh"
titulo "CP-12 - Recuperación RabbitMQ ($(ahora))"

paso "A) Se envía un pedido y se mata el worker (SIGKILL) mientras lo procesa"
crear_pedido "Cliente-kill" "Monitor" 1
ID_KILL="$ORDER_ID"
sleep 1
estado_cola 0
logs_worker | grep "$ID_KILL"
docker compose kill notification-worker
echo "   Worker eliminado sin enviar ack"

paso "El mensaje no se perdió: volvió a estado Ready"
estado_cola

paso "Se reinicia el worker: recibe el mensaje con redelivered=true"
docker compose start notification-worker
sleep 8
logs_worker | grep "$ID_KILL"

paso "B) Worker apagado, 3 pedidos en cola y reinicio del broker"
docker compose stop notification-worker
IDS=()
for i in 1 2 3; do crear_pedido "Cliente-restart-$i" "Producto-$i" 1; IDS+=("$ORDER_ID"); done
estado_cola
docker compose restart rabbitmq
esperar_rabbitmq
paso "Tras el reinicio del broker los mensajes siguen en la cola"
estado_cola 8

paso "La API se reconectó sola y sigue aceptando pedidos"
sleep 3
logs_api | grep -E "cerrada|Conectado" | tail -n 3
crear_pedido "Cliente-post-restart" "Producto" 1
IDS+=("$ORDER_ID")

paso "Se inicia el worker y procesa todo sin pérdida"
docker compose start notification-worker
sleep 25
for id in "${IDS[@]}"; do logs_worker | grep "$id" | grep "confirmado"; done
estado_cola 2
