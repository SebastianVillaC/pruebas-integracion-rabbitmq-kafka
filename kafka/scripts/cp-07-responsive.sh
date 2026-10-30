#!/usr/bin/env bash
# CP-07 Responsive: la API responde sin esperar a los consumidores
source "$(dirname "$0")/_comun.sh"
titulo "CP-07 - Responsive Kafka ($(ahora))"

paso "Se envían 5 pedidos seguidos (cada consumidor tarda 2-5 s por evento)"
IDS=()
for i in 1 2 3 4 5; do crear_pedido "Cliente-$i" "Producto-$i" 1 50000; IDS+=("$ORDER_ID"); done

paso "Inmediatamente después, los consumidores todavía tienen eventos pendientes (lag > 0)"
lags

paso "Tiempo de respuesta de la API (responseTimeMs) frente al fin del procesamiento en cada consumidor"
sleep 25
for id in "${IDS[@]}"; do
  logs orders-api | grep "$id" | grep -o '.*responseTimeMs=[0-9]*' | sed 's/^/   /'
  logs "${CONSUMIDORES[@]}" | grep "$id" | grep "offset confirmado" | sed 's/^/   /'
done
lags
