#!/usr/bin/env bash
# Uso: ./scripts/crear-pedido.sh [cliente] [producto] [cantidad] [precio_unitario]
source "$(dirname "$0")/_comun.sh"
crear_pedido "${1:-Pedro}" "${2:-Libro de arquitectura}" "${3:-1}" "${4:-85000}"
