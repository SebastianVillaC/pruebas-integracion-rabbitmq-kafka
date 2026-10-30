#!/usr/bin/env bash
# CP-01 Creación de pedido vía API
# CP-04 Publicación del evento OrderCreated en Kafka (tópico orders.events)
# CP-05 Consumo del evento por los consumidores
source "$(dirname "$0")/_comun.sh"
titulo "CP-01 / CP-04 / CP-05 - Flujo básico Kafka ($(ahora))"
docker compose ps

paso "Tópico orders.events (3 particiones)"
describir_topico

paso "CP-01: POST /orders"
crear_pedido "Pedro" "Libro de arquitectura" 1 85000

paso "CP-01: datos inválidos -> debe responder 400"
curl -s -w '\n   -> HTTP %{http_code}\n' -X POST "$API_URL/orders" -H 'Content-Type: application/json' \
  -d '{"customerName":"Pedro","product":"","quantity":1}'

paso "CP-04: la API registró la publicación del evento"
sleep 1
logs orders-api | grep "$ORDER_ID"

paso "CP-04: el evento OrderCreated está almacenado en el tópico (leído del broker)"
ver_evento "$ORDER_ID"

paso "CP-05: los 3 consumidores (grupos distintos) reciben el mismo evento"
sleep 8
for c in "${CONSUMIDORES[@]}"; do
  echo "   --- $c ---"
  logs "$c" | grep "$ORDER_ID" | grep -v '^.*| {'
done

paso "CP-05: avance de offsets de cada consumer group (lag 0 = todo consumido)"
grupos
echo
echo "Capturas sugeridas: Kafka UI http://localhost:8080 (Topics > orders.events > Messages, Consumers)."
