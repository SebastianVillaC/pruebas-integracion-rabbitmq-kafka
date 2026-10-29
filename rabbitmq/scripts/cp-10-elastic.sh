#!/usr/bin/env bash
# CP-10 Elastic: distribución de carga entre múltiples workers
source "$(dirname "$0")/_comun.sh"
REPLICAS="${1:-3}"
titulo "CP-10 - Elasticidad RabbitMQ con $REPLICAS workers ($(ahora))"

paso "Se escala el worker: docker compose up -d --scale notification-worker=$REPLICAS"
docker compose up -d --scale notification-worker="$REPLICAS" --no-recreate
sleep 4
docker compose ps
estado_cola 3

paso "Se envían 9 pedidos"
IDS=()
for i in $(seq 1 9); do crear_pedido "Cliente-escala-$i" "Producto-$i" 1 >/dev/null; echo "   pedido $i -> $ORDER_ID"; IDS+=("$ORDER_ID"); done
FILTRO=$(IFS='|'; echo "${IDS[*]}")

paso "Cada worker procesa una parte (prefetch=1 -> reparto round-robin)"
sleep 20
logs_worker | grep "Mensaje recibido" | grep -E "$FILTRO"
echo
echo "   Mensajes por contenedor:"
logs_worker | grep "Mensaje recibido" | grep -E "$FILTRO" \
  | awk '{print $1}' | sort | uniq -c | sed 's/^/   /'

paso "Se vuelve a 1 worker"
docker compose up -d --scale notification-worker=1 --no-recreate
