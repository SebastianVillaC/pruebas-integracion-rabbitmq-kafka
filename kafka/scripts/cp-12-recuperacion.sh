#!/usr/bin/env bash
# CP-12 Recuperación:
#  A) un consumidor muere antes de confirmar el offset -> al reiniciar reprocesa el evento pendiente
#  B) se reinicia el broker -> los eventos del log y los offsets de los grupos se conservan
source "$(dirname "$0")/_comun.sh"
titulo "CP-12 - Recuperación Kafka ($(ahora))"

paso "A) Se envía un pedido y se mata inventory-consumer (SIGKILL) mientras lo procesa"
crear_pedido "Cliente-kill" "Monitor" 1 700000
ID_KILL="$ORDER_ID"
sleep 1.5
logs inventory-consumer | grep "$ID_KILL" | grep -v '| {'
docker compose kill inventory-consumer
echo "   inventory-consumer eliminado sin confirmar el offset"

paso "El offset no avanzó: el evento sigue pendiente para inventory-service-group (lag 1)"
grupo inventory-service-group

paso "Se reinicia inventory-consumer: retoma desde el último offset confirmado y procesa el evento"
docker compose start inventory-consumer
sleep 30
logs inventory-consumer | grep "$ID_KILL" | grep -v '| {'
lag_grupo inventory-service-group

paso "B) Se apagan los consumidores, se publican 3 pedidos y se reinicia el broker"
docker compose stop "${CONSUMIDORES[@]}"
IDS=()
for i in 1 2 3; do crear_pedido "Cliente-restart-$i" "Producto-$i" 1 15000; IDS+=("$ORDER_ID"); done
sleep 2
echo "   Offsets del tópico ANTES del reinicio:"; offsets_topico
lags
docker compose restart kafka
esperar_kafka
echo "   Offsets del tópico DESPUÉS del reinicio:"; offsets_topico
lags

paso "La API se reconecta sola y sigue publicando"
crear_pedido "Cliente-post-restart" "Producto" 1 15000
IDS+=("$ORDER_ID")

paso "Se inician los consumidores: procesan los 4 eventos pendientes sin pérdida"
docker compose start "${CONSUMIDORES[@]}"
sleep 40
for id in "${IDS[@]}"; do logs "${CONSUMIDORES[@]}" | grep "$id" | grep "offset confirmado"; done
lags
