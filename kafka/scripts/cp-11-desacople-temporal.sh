#!/usr/bin/env bash
# CP-11 Desacoplamiento temporal:
#  A) eventos publicados con los consumidores apagados se procesan cuando estos inician
#  B) consumidor posterior: un consumer group nuevo, iniciado después de publicar, lee todo desde el log
source "$(dirname "$0")/_comun.sh"
titulo "CP-11 - Desacoplamiento temporal Kafka ($(ahora))"

paso "A) Se apagan los 3 consumidores"
docker compose stop "${CONSUMIDORES[@]}"

paso "Se publican 3 pedidos mientras no hay ningún consumidor"
IDS=()
for i in 1 2 3; do crear_pedido "Cliente-offline-$i" "Producto-$i" 1 20000; IDS+=("$ORDER_ID"); done

paso "Los eventos quedan en el log de Kafka (lag = 3 en cada grupo)"
sleep 2
offsets_topico
lags
sleep 10

paso "Se inician los consumidores: procesan los eventos publicados mientras estaban apagados"
docker compose start "${CONSUMIDORES[@]}"
sleep 35
for id in "${IDS[@]}"; do logs "${CONSUMIDORES[@]}" | grep "$id" | grep "offset confirmado"; done
lags

paso "B) Consumidor posterior: nuevo grupo audit-service-group iniciado DESPUÉS de publicar todos los eventos"
TOTAL=$(kt kafka-get-offsets.sh --topic "$TOPIC" | awk -F: '{s+=$3} END {print s}')
echo "   Eventos almacenados en el tópico: $TOTAL"
docker compose run -d --rm --no-deps --name audit-consumer \
  -e KAFKA_GROUP_ID=audit-service-group -e PROCESSING_MS_MIN=50 -e PROCESSING_MS_MAX=100 \
  notification-consumer >/dev/null
sleep 25
docker logs audit-consumer 2>&1 | grep -E "Unido|Evento recibido" | head -n 6
echo "   ..."
echo "   Eventos leídos por audit-service-group desde el inicio del log: $(docker logs audit-consumer 2>&1 | grep -c 'Evento recibido') de $TOTAL"
grupo audit-service-group
docker stop audit-consumer >/dev/null
