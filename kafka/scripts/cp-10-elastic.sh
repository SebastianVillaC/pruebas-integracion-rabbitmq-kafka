#!/usr/bin/env bash
# CP-10 Elastic: varias instancias del MISMO consumer group se reparten las particiones
source "$(dirname "$0")/_comun.sh"
REPLICAS="${1:-3}"
titulo "CP-10 - Elasticidad Kafka con $REPLICAS instancias de inventory-consumer ($(ahora))"

paso "docker compose up -d --scale inventory-consumer=$REPLICAS"
docker compose up -d --scale inventory-consumer="$REPLICAS" --no-recreate
echo "   Esperando el rebalanceo del grupo..."
sleep 20
docker compose ps inventory-consumer

paso "Asignación de particiones: cada instancia recibe una partición distinta"
kt kafka-consumer-groups.sh --describe --members --verbose --group inventory-service-group

paso "Se envían 9 pedidos"
IDS=()
for i in $(seq 1 9); do crear_pedido "Cliente-escala-$i" "Producto-$i" 1 10000 >/dev/null; echo "   pedido $i -> $ORDER_ID"; IDS+=("$ORDER_ID"); done
FILTRO=$(IFS='|'; echo "${IDS[*]}")

paso "Cada instancia procesa los eventos de sus particiones"
sleep 30
logs inventory-consumer | grep "Evento recibido" | grep -E "$FILTRO" | sed 's/eventId=[^ ]* //; s/correlationId=[^ ]* //'
echo
echo "   Eventos por instancia:"
logs inventory-consumer | grep "Evento recibido" | grep -E "$FILTRO" | awk '{print $1}' | sort | uniq -c | sed 's/^/   /'
grupo inventory-service-group

paso "Se vuelve a 1 instancia"
docker compose up -d --scale inventory-consumer=1 --no-recreate
