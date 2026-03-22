#!/usr/bin/env bash

# shellcheck source=/dev/null
source ../../.venv/bin/activate

test -f "output.log" && rm output.log
test -f "error.log" && rm error.log

mutation-indexer \
   viz \
   ./conf/config.toml \
   ./conf/config-viz.toml \
   "$@"

mutation-indexer \
   gene_expression \
   ./conf/config.toml \
   ./conf/config-gene_expression.toml \
   "$@"
