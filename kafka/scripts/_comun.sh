#!/usr/bin/env bash
# Funciones compartidas por los scripts de prueba de Kafka.
# Se ejecutan siempre desde la carpeta kafka/ para que docker compose encuentre el compose.
set -u
cd "$(dirname "${BASH_SOURCE[0]}")/.."

API_URL="${API_URL:-http://localhost:3000}"
TOPIC="orders.events"
GRUPOS=(inventory-service-group billing-service-group notification-service-group)
CONSUMIDORES=(inventory-consumer billing-consumer notification-consumer)

titulo() { printf '\n\033[1;36m==== %s ====\033[0m\n' "$*"; }
paso()   { printf '\n\033[1;33m>> %s\033[0m\n' "$*"; }
ahora()  { date '+%Y-%m-%d %H:%M:%S'; }

# Ejecuta una herramienta de Kafka dentro del contenedor del broker
kt() { docker compose exec -T kafka "$@" --bootstrap-server localhost:9092 2>/dev/null; }

# crear_pedido <cliente> <producto> <cantidad> <precio> [correlationId]
# Imprime la respuesta y deja el orderId en la variable ORDER_ID.
crear_pedido() {
  local cliente="${1:-Pedro}" producto="${2:-Libro de arquitectura}" cantidad="${3:-1}" precio="${4:-85000}" corr="${5:-}"
  local headers=(-H 'Content-Type: application/json')
  [[ -n "$corr" ]] && headers+=(-H "X-Correlation-Id: $corr")
  local salida cuerpo meta
  salida=$(curl -s -w '\n%{http_code} %{time_total}' -X POST "$API_URL/orders" "${headers[@]}" \
    -d "{\"customerName\":\"$cliente\",\"product\":\"$producto\",\"quantity\":$cantidad,\"unitPrice\":$precio}")
  cuerpo=$(echo "$salida" | sed '$d')
  meta=$(echo "$salida" | tail -n1)
  ORDER_ID=$(echo "$cuerpo" | sed -n 's/.*"orderId":"\([^"]*\)".*/\1/p')
  echo "$cuerpo"
  echo "   -> HTTP $(echo "$meta" | cut -d' ' -f1) en $(echo "$meta" | cut -d' ' -f2)s"
}

describir_topico() { kt kafka-topics.sh --describe --topic "$TOPIC"; }

# Offsets finales (log-end) de cada partición del tópico
offsets_topico() { kt kafka-get-offsets.sh --topic "$TOPIC" | sed 's/^/   /'; }

# Estado de un consumer group (offset actual, log-end, lag y miembro asignado por partición)
grupo() { kt kafka-consumer-groups.sh --describe --group "$1"; }
grupos() { for g in "${GRUPOS[@]}"; do grupo "$g"; done; }
lag_grupo() { grupo "$1" | awk -v g="$1" '$1==g && $6 ~ /^[0-9]+$/ {s+=$6} END {printf "   %s -> lag total=%d\n", g, s}'; }
lags() { for g in "${GRUPOS[@]}"; do lag_grupo "$g"; done; }

# Lee del broker el evento de un pedido, usando la partición/offset que registró la API
ver_evento() {
  local id="$1" linea particion offset
  linea=$(docker compose logs --no-color orders-api | grep "publicado" | grep "orderId=$id " | tail -n1)
  particion=$(echo "$linea" | sed -n 's/.*partition=\([0-9]*\).*/\1/p')
  offset=$(echo "$linea" | sed -n 's/.*offset=\([0-9]*\).*/\1/p')
  echo "   [broker] topic=$TOPIC partition=$particion offset=$offset"
  docker compose exec -T kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$TOPIC" \
    --partition "$particion" --offset "$offset" --max-messages 1 --timeout-ms 10000 \
    --property print.key=true --property print.headers=true 2>/dev/null | sed 's/^/   /'
}

esperar_kafka() {
  printf '   Esperando a que kafka esté healthy'
  until [[ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" == "healthy" ]]; do
    printf '.'; sleep 2
  done
  echo ' OK'
}

# Últimas líneas de log de los servicios indicados. Se filtra por orderId/correlationId (únicos por pedido)
# en vez de por tiempo, porque `docker compose logs --since` no es fiable tras reiniciar Docker.
logs() { docker compose logs --no-color --tail 3000 "$@"; }
