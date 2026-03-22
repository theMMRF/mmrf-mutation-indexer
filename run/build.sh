#!/usr/bin/env bash

# shellcheck source=/dev/null
source ../../.venv/bin/activate

export PIP_CONSTRAINT=$(pwd)/requirements.txt

cd ../ || exit

export SCIE_BASE=/tmp/nce
export PEX_ROOT=/tmp/pex

pip install --no-build-isolation .[client] && tox -e pex
