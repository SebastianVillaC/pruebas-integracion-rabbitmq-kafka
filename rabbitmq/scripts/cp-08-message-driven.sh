#!/usr/bin/env bash
# CP-08 Message-driven: no existe comunicación directa entre API y worker
source "$(dirname "$0")/_comun.sh"
titulo "CP-08 - Message-driven RabbitMQ ($(ahora))"

paso "1) Puertos: solo orders-api (3000) y rabbitmq (5672/15672) exponen puertos; el worker no"
docker compose ps --format 'table {{.Name}}\t{{.Service}}\t{{.Ports}}'

paso "2) Configuración: la API y el worker solo conocen la URL del broker"
echo "   orders-api:";          docker compose exec -T orders-api env          | grep RABBITMQ | sed 's/^/     /'
echo "   notification-worker:"; docker compose exec -T notification-worker env | grep RABBITMQ | sed 's/^/     /'

paso "3) Desde orders-api no hay forma de llamar al worker por HTTP (no escucha ningún puerto)"
docker compose exec -T orders-api sh -c 'wget -q -T 3 -O- http://notification-worker:3000/ 2>&1 || echo "   -> Sin respuesta: el worker no expone HTTP"'

paso "4) Conexiones abiertas en el broker: ambos servicios son clientes AMQP de RabbitMQ"
docker compose exec -T rabbitmq rabbitmqctl list_connections name user state 2>/dev/null

paso "5) Código: el worker no tiene dependencias HTTP"
grep -n '"dependencies"' -A3 notification-worker/package.json | sed 's/^/   /'
grep -rnE "require\('(http|https|express|axios|node-fetch)'\)|[^e]fetch\(|http://" notification-worker/src \
  || echo "   -> notification-worker/src no contiene servidor ni llamadas HTTP"
