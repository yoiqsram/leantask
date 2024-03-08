#!/usr/bin/bash

SCRIPT=${BASH_SOURCE[0]}
SCRIPT_DIR=$(realpath $(dirname $SCRIPT))
VENV_DIR="$SCRIPT_DIR/venv"
VENV_ACTIVATE_EXECUTE="$VENV_DIR/bin/activate"

source "$VENV_ACTIVATE_EXECUTE"

python -m pytest

deactivate
