#!/usr/bin/env bash
# CP-08 Message-driven: no existe comunicación directa entre la API y los consumidores
source "$(dirname "$0")/_comun.sh"
titulo "CP-08 - Message/Event-driven Kafka ($(ahora))"

paso "1) Puertos: los consumidores no exponen ningún puerto"
docker compose ps --format 'table {{.Name}}\t{{.Service}}\t{{.Ports}}'

paso "2) Configuración: la API y los consumidores solo conocen el broker y el tópico"
for s in orders-api "${CONSUMIDORES[@]}"; do
  echo "   $s:"; docker compose exec -T "$s" env | grep KAFKA | sed 's/^/     /'
done

paso "3) Desde orders-api no se puede llamar a ningún consumidor por HTTP"
for c in "${CONSUMIDORES[@]}"; do
  docker compose exec -T orders-api sh -c "wget -q -T 3 -O- http://$c:3000/ 2>&1 || echo '   -> $c: sin respuesta (no expone HTTP)'"
done

paso "4) Kafka: cada consumidor es miembro de su propio consumer group"
for g in "${GRUPOS[@]}"; do kt kafka-consumer-groups.sh --describe --members --group "$g"; done

paso "5) Código: los consumidores no tienen servidor ni llamadas HTTP"
for c in "${CONSUMIDORES[@]}"; do
  grep -rnE "require\('(http|https|express|axios|node-fetch)'\)|[^e]fetch\(|http://" "$c/src" \
    || echo "   -> $c/src no contiene servidor ni llamadas HTTP"
done
