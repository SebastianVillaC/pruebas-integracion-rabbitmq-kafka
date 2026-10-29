#!/usr/bin/env bash
# CP-07 Responsive: la API responde sin esperar al consumidor
source "$(dirname "$0")/_comun.sh"
titulo "CP-07 - Responsive RabbitMQ ($(ahora))"

paso "Se envían 5 pedidos seguidos (el worker tarda 2-5 s por mensaje)"
INICIO=$(date +%s)
IDS=()
for i in 1 2 3 4 5; do
  crear_pedido "Cliente-$i" "Producto-$i" 1
  IDS+=("$ORDER_ID")
done
echo "   Los 5 pedidos fueron aceptados en $(( $(date +%s) - INICIO )) s en total"

paso "Mientras tanto la cola tiene trabajo pendiente (Ready/Unacked > 0)"
estado_cola 3

paso "Tiempo de respuesta de la API (responseTimeMs) frente al momento del ack del worker"
sleep 22
for id in "${IDS[@]}"; do
  logs_api    | grep "$id" | grep -o '.*responseTimeMs=[0-9]*' | sed 's/^/   /'
  logs_worker | grep "$id" | grep 'confirmado' | sed 's/^/   /'
done
