#!/usr/bin/env bash
# Funciones compartidas por los scripts de prueba de RabbitMQ.
# Se ejecutan siempre desde la carpeta rabbitmq/ para que docker compose encuentre el compose.
set -u
cd "$(dirname "${BASH_SOURCE[0]}")/.."

API_URL="${API_URL:-http://localhost:3000}"
MGMT_URL="${MGMT_URL:-http://localhost:15672}"
MGMT_AUTH="admin:admin"

titulo() { printf '\n\033[1;36m==== %s ====\033[0m\n' "$*"; }
paso()   { printf '\n\033[1;33m>> %s\033[0m\n' "$*"; }
ahora()  { date '+%Y-%m-%d %H:%M:%S'; }

json() { if command -v jq >/dev/null 2>&1; then jq .; else python3 -m json.tool; fi; }

# crear_pedido <cliente> <producto> <cantidad> [correlationId]
# Imprime la respuesta y deja el orderId en la variable ORDER_ID.
crear_pedido() {
  local cliente="${1:-Pedro}" producto="${2:-Libro de arquitectura}" cantidad="${3:-1}" corr="${4:-}"
  local headers=(-H 'Content-Type: application/json')
  [[ -n "$corr" ]] && headers+=(-H "X-Correlation-Id: $corr")
  local salida
  salida=$(curl -s -w '\n%{http_code} %{time_total}' -X POST "$API_URL/orders" "${headers[@]}" \
    -d "{\"customerName\":\"$cliente\",\"product\":\"$producto\",\"quantity\":$cantidad}")
  local cuerpo meta
  cuerpo=$(echo "$salida" | sed '$d')
  meta=$(echo "$salida" | tail -n1)
  ORDER_ID=$(echo "$cuerpo" | sed -n 's/.*"orderId":"\([^"]*\)".*/\1/p')
  echo "$cuerpo"
  echo "   -> HTTP $(echo "$meta" | cut -d' ' -f1) en $(echo "$meta" | cut -d' ' -f2)s"
}

# Contadores de la cola pedidos, leídos en tiempo real del broker con rabbitmqctl
# (los mismos Ready / Unacked / Total que muestra la consola web).
estado_cola() {
  sleep "${1:-1}"
  docker compose exec -T rabbitmq rabbitmqctl list_queues -q name durable messages_ready messages_unacknowledged messages consumers 2>/dev/null \
    | awk '$1=="pedidos" {printf "   Cola %s -> Ready=%s  Unacked=%s  Total=%s  Consumidores=%s  Durable=%s\n", $1, $3, $4, $5, $6, $2}'
}

# Lee los mensajes en cola sin consumirlos: el broker los vuelve a encolar
# (por eso, cuando el worker los reciba después, aparecerán con redelivered=true).
ver_mensajes_en_cola() {
  curl -s -u "$MGMT_AUTH" -H 'Content-Type: application/json' -X POST \
    "$MGMT_URL/api/queues/%2F/pedidos/get" \
    -d "{\"count\":${1:-5},\"ackmode\":\"reject_requeue_true\",\"encoding\":\"auto\"}" | python3 -c '
import json, sys
for m in json.load(sys.stdin):
    p = m["properties"]
    print("   [broker] messageId=%s correlationId=%s delivery_mode=%s (2=persistente) redelivered=%s" % (
        p.get("message_id"), p.get("correlation_id"), p.get("delivery_mode"), m["redelivered"]))
    print("            payload=" + m["payload"])'
}

esperar_rabbitmq() {
  printf '   Esperando a que rabbitmq esté healthy'
  until [[ "$(docker inspect -f '{{.State.Health.Status}}' rabbitmq 2>/dev/null)" == "healthy" ]]; do
    printf '.'; sleep 2
  done
  echo ' OK'
}

# Últimas líneas de log de cada servicio. Se filtra por orderId/correlationId (únicos por pedido)
# en vez de por tiempo, porque `docker compose logs --since` no es fiable tras reiniciar Docker.
logs_worker() { docker compose logs --no-color --tail 3000 notification-worker; }
logs_api()    { docker compose logs --no-color --tail 3000 orders-api; }
