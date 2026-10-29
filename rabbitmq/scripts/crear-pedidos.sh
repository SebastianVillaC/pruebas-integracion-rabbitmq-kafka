#!/usr/bin/env bash
# Uso: ./scripts/crear-pedidos.sh [cantidad_de_pedidos]
source "$(dirname "$0")/_comun.sh"
N="${1:-5}"
for i in $(seq 1 "$N"); do
  crear_pedido "Cliente-$i" "Producto-$i" "$i"
done
