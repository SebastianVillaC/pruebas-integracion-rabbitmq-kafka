#!/usr/bin/env bash
# Offsets y lag de los consumer groups + offsets finales del tópico
source "$(dirname "$0")/_comun.sh"
paso "Offsets finales del tópico $TOPIC"
offsets_topico
paso "Consumer groups"
grupos
